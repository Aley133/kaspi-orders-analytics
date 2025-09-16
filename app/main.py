from __future__ import annotations

# ---------- imports ----------
import os
import re
import uuid
import asyncio
from datetime import datetime, timedelta, time, date as _date
from pathlib import Path
from typing import Optional, Dict, List, Iterable, Tuple, Callable

import httpx
from httpx import HTTPStatusError, RequestError
import pytz
from dotenv import load_dotenv
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse, PlainTextResponse, JSONResponse
from cachetools import TTLCache
from pydantic import BaseModel

# multitenant middleware (кладёт tenant токен в request.state)
from app.deps.auth import attach_kaspi_token_middleware, get_current_kaspi_token

# доменные роутеры
from app.api.bridge_v2 import router as bridge_router
from app.api.profit_fifo import get_profit_fifo_router
from app.api.authz import router as auth_router
from app.api.products import get_products_router
from app.api import settings as settings_api

# Kaspi client c поддержкой tenant токена (для /orders)
from app.deps.kaspi_client_tenant import KaspiClient as TenantKaspiClient

# ---------- ENV ----------
load_dotenv()

DEFAULT_TZ        = os.getenv("TZ", "Asia/Almaty")
KASPI_BASE_URL    = os.getenv("KASPI_BASE_URL", "https://kaspi.kz/shop/api/v2").rstrip("/")
CURRENCY          = os.getenv("CURRENCY", "KZT")
SHOP_NAME         = os.getenv("SHOP_NAME", "LeoXpress")
PARTNER_ID        = os.getenv("PARTNER_ID", "")

AMOUNT_FIELDS     = [s.strip() for s in os.getenv("AMOUNT_FIELDS", "totalPrice").split(",") if s.strip()]
AMOUNT_DIVISOR    = float(os.getenv("AMOUNT_DIVISOR", "1") or 1)

DATE_FIELD_DEFAULT = os.getenv("DATE_FIELD_DEFAULT", "creationDate")
DATE_FIELD_OPTIONS = [s.strip() for s in os.getenv(
    "DATE_FIELD_OPTIONS", "creationDate,plannedShipmentDate,plannedDeliveryDate,shipmentDate,deliveryDate"
).split(",") if s.strip()]
CITY_KEYS          = [s.strip() for s in os.getenv("CITY_KEYS", "city,deliveryAddress.city").split(",") if s.strip()]

CHUNK_DAYS  = int(os.getenv("CHUNK_DAYS", "7") or 7)
CACHE_TTL   = int(os.getenv("CACHE_TTL", "300") or 300)

BUSINESS_DAY_START = os.getenv("BUSINESS_DAY_START", "20:00")   # HH:MM
USE_BUSINESS_DAY   = os.getenv("USE_BUSINESS_DAY", "true").lower() in ("1","true","yes","on")
STORE_ACCEPT_UNTIL = os.getenv("STORE_ACCEPT_UNTIL", "17:00")   # HH:MM

ENRICH_CONCURRENCY = int(os.getenv("ENRICH_CONCURRENCY", "6") or 6)

# запас вокруг интервала сбора (днями), чтобы не терять «переехавшие» заказы
SCAN_FIELD        = os.getenv("SCAN_FIELD", "creationDate")
SCAN_MARGIN_DAYS  = int(os.getenv("SCAN_MARGIN_DAYS", "2") or 2)

# ---------- FastAPI ----------
app = FastAPI(title="Kaspi Orders Analytics")

origins = [o.strip() for o in os.getenv("ALLOWED_ORIGINS", "").split(",") if o.strip()]
if origins:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        allow_headers=["*"],
        allow_credentials=False,
    )

# кладём токен в request.state
app.middleware("http")(attach_kaspi_token_middleware)

# tenant-aware клиент для /orders
client = TenantKaspiClient(base_url=KASPI_BASE_URL)

# кэш для entries (резерв)
orders_cache = TTLCache(maxsize=512, ttl=CACHE_TTL)

# /ui статика (best-effort)
_ui_candidates = ("app/static", "app/ui", "static", "ui")
_ui_dir = next((p for p in _ui_candidates if Path(p).is_dir()), None)
if _ui_dir:
    app.mount("/ui", StaticFiles(directory=_ui_dir, html=True), name="ui")

# роутеры
app.include_router(get_products_router(client), prefix="/products")
app.include_router(get_profit_fifo_router(), prefix="/profit")
app.include_router(bridge_router, prefix="/profit")
app.include_router(auth_router)
app.include_router(settings_api.router, prefix="/settings", tags=["settings"])

# ---------- helpers ----------
RU_STATUS_MAP = {
    "НОВЫЙ": "NEW",
    "ОПЛАТА ПОДТВЕРЖДЕНА": "APPROVED_BY_BANK",
    "ПРИНЯТ МАГАЗИНОМ": "ACCEPTED_BY_MERCHANT",
    "ГОТОВ К ОТГРУЗКЕ": "READY_FOR_SHIPMENT",
    "KASPI ДОСТАВКА (ПЕРЕДАН)": "KASPI_DELIVERY",
    "KASPI ДОСТАВКА": "KASPI_DELIVERY",
    "ДОСТАВЛЕН": "DELIVERED",
    "ЗАВЕРШЁН (АРХИВ)": "ARCHIVE",
    "ЗАВЕРШЕН (АРХИВ)": "ARCHIVE",
    "АРХИВ (ИСТОРИЯ)": "ARCHIVED",
    "ВОЗВРАТ": "RETURNED",
    "ОТМЕНЁН": "CANCELED",
    "ОТМЕНЕН": "CANCELED",
}

def _bd_delta(hhmm: str) -> timedelta:
    try:
        h, m = hhmm.split(":")
        return timedelta(hours=int(h), minutes=int(m))
    except Exception:
        return timedelta(0)

def norm_state(s: str) -> str:
    return (s or "").strip().upper()

def parse_states_csv(s: Optional[str]) -> Optional[set[str]]:
    if not s:
        return None
    out: set[str] = set()
    for raw in re.split(r"[\s,;]+", s):
        raw = raw.strip()
        if not raw:
            continue
        up = raw.upper()
        code = RU_STATUS_MAP.get(up) or up
        out.add(code)
    return out

def tzinfo_of(name: str) -> pytz.BaseTzInfo:
    try:
        return pytz.timezone(name)
    except Exception:
        raise HTTPException(status_code=400, detail=f"Bad timezone: {name}")

def parse_date_local(d: str, tz: str) -> datetime:
    tzinfo = tzinfo_of(tz)
    try:
        y, m, dd = map(int, d.split("-"))
        return tzinfo.localize(datetime(y, m, dd, 0, 0, 0, 0))
    except Exception:
        raise HTTPException(status_code=400, detail=f"Bad date: {d}")

def apply_hhmm(dt_local: datetime, hhmm: str) -> datetime:
    hh, mm = map(int, hhmm.split(":"))
    return dt_local.replace(hour=hh, minute=mm, second=0, microsecond=0)

def iter_chunks(start_dt: datetime, end_dt: datetime, step_days: int) -> Iterable[Tuple[datetime, datetime]]:
    cur = start_dt
    while cur <= end_dt:
        nxt = min(cur + timedelta(days=step_days) - timedelta(milliseconds=1), end_dt)
        yield cur, nxt
        cur = nxt + timedelta(milliseconds=1)

def dict_get_path(d: dict, path: str):
    cur = d
    for key in path.split("."):
        if not isinstance(cur, dict):
            return None
        if key not in cur:
            return None
        cur = cur[key]
    return cur

_CITY_KEY_HINTS = {"city", "cityname", "town", "locality", "settlement"}

def _normalize_city(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = s.strip()
    s = re.sub(r"^\s*(г\.?|город)\s+", "", s, flags=re.IGNORECASE)
    s = s.split(",")[0].strip()
    return s

def _deep_find_city(obj) -> str:
    if isinstance(obj, dict):
        for k, v in obj.items():
            kl = str(k).lower()
            if any(h in kl for h in _CITY_KEY_HINTS) and isinstance(v, str) and v.strip():
                return _normalize_city(v)
            found = _deep_find_city(v)
            if found:
                return found
    elif isinstance(obj, list):
        for it in obj:
            found = _deep_find_city(it)
            if found:
                return found
    return ""

def extract_city(attrs: dict) -> str:
    for k in CITY_KEYS:
        v = dict_get_path(attrs, k) if "." in k else attrs.get(k)
        if isinstance(v, str) and v.strip():
            return _normalize_city(v)
        if isinstance(v, (dict, list)):
            res = _deep_find_city(v)
            if res:
                return res
    res = _deep_find_city(attrs)
    return res or ""

def extract_amount(attrs: dict) -> float:
    total = 0.0
    for k in AMOUNT_FIELDS:
        v = dict_get_path(attrs, k) if "." in k else attrs.get(k)
        if v is None:
            continue
        try:
            total += float(v)
        except Exception:
            continue
    return total / (AMOUNT_DIVISOR or 1.0)

def extract_ms(attrs: dict, field: str) -> Optional[int]:
    """Поддержка ISO/миллисекунд и алиаса creationDate<->date."""
    v = attrs.get(field)
    if v is None and field == "creationDate":
        v = attrs.get("date")
    if v is None:
        return None
    try:
        return int(v)
    except Exception:
        try:
            return int(datetime.fromisoformat(str(v).replace("Z", "+00:00")).timestamp() * 1000)
        except Exception:
            return None

def bucket_date(dt_local: datetime, use_bd: bool, bd_start: str) -> str:
    if use_bd:
        shift = timedelta(hours=24) - _bd_delta(bd_start)
        return (dt_local + shift).date().isoformat()
    return dt_local.date().isoformat()

def _guess_number(attrs: dict, fallback_id: str) -> str:
    for k in ("number", "code", "orderNumber"):
        v = attrs.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
        try:
            if isinstance(v, (int, float)):
                return str(v)
        except Exception:
            pass
    return str(fallback_id)

# ---------- HTTPX (для /entries) ----------
BASE_TIMEOUT = httpx.Timeout(connect=10.0, read=80.0, write=20.0, pool=60.0)
HTTPX_LIMITS  = httpx.Limits(max_connections=20, max_keepalive_connections=10)

def _async_client(scale: float = 1.0):
    scale = max(1.0, float(scale))
    return httpx.AsyncClient(
        base_url=KASPI_BASE_URL,
        timeout=httpx.Timeout(
            connect=BASE_TIMEOUT.connect,
            read=min(420.0, BASE_TIMEOUT.read * scale),
            write=min(150.0, BASE_TIMEOUT.write * scale),
            pool=BASE_TIMEOUT.pool,
        ),
        limits=HTTPX_LIMITS,
    )

# ---------- «умный» операционный день ----------
_DELIVERED_STATES = {"KASPI_DELIVERY", "DELIVERED", "ARCHIVE", "ARCHIVED"}

def _smart_operational_day(attrs: dict, state: str, tzinfo: pytz.BaseTzInfo,
                           store_accept_until: str, business_day_start: str) -> Tuple[str, str]:
    ms_creation = extract_ms(attrs, "creationDate")
    ms_planned  = extract_ms(attrs, "plannedShipmentDate")
    ms_ship     = extract_ms(attrs, "shipmentDate")

    dt_creation = datetime.fromtimestamp(ms_creation/1000, tz=pytz.UTC).astimezone(tzinfo) if ms_creation else None
    dt_planned  = datetime.fromtimestamp(ms_planned/1000,  tz=pytz.UTC).astimezone(tzinfo) if ms_planned  else None
    dt_ship     = datetime.fromtimestamp(ms_ship/1000,     tz=pytz.UTC).astimezone(tzinfo) if ms_ship     else None

    if state in _DELIVERED_STATES:
        base = dt_ship or dt_planned or dt_creation or datetime.now(tzinfo)
        shift = timedelta(hours=24) - _bd_delta(business_day_start)
        return (base + shift).date().isoformat(), "delivered_business_day"

    if dt_planned:
        return dt_planned.date().isoformat(), "planned"

    cutoff_h, cutoff_m = map(int, store_accept_until.split(":"))
    cutoff = time(cutoff_h, cutoff_m, 0)
    if dt_creation:
        if dt_creation.time() <= cutoff:
            return dt_creation.date().isoformat(), "created_before_cutoff"
        else:
            return (dt_creation + timedelta(days=1)).date().isoformat(), "created_after_cutoff_next_day"

    return datetime.now(tzinfo).date().isoformat(), "fallback_now"

# ---------- обогащение позиций ----------
async def _first_item_details(order_id: str, timeout_scale: float = 1.0) -> Optional[Dict[str, object]]:
    token = get_current_kaspi_token()
    if not token:
        return None

    headers = {
        "X-Auth-Token": token,
        "Accept": "application/vnd.api+json",
        "Content-Type": "application/vnd.api+json",
        "User-Agent": "leo-analytics/1.0",
    }

    async with _async_client(scale=timeout_scale) as cli:
        # 1) быстрый путь — /orderentries
        try:
            r = await cli.get("/orderentries",
                              params={"filter[order.id]": order_id, "page[size]": "200"},
                              headers=headers)
            r.raise_for_status()
            j = r.json()
            data = (j.get("data") or [])
            if data:
                attrs_e = data[0].get("attributes", {}) or {}
                title = ""
                for key in ("offerName","title","name","productName","shortName"):
                    v = attrs_e.get(key)
                    if isinstance(v, str) and v.strip():
                        title = v.strip(); break
                sku = ""
                for key in ("sku","code","productCode"):
                    v = attrs_e.get(key)
                    if isinstance(v, str) and v.strip():
                        sku = v.strip(); break
                off = attrs_e.get("offer") or {}
                if isinstance(off, dict) and off.get("code"):
                    sku = off["code"]
                return {"sku": sku, "title": title}
        except Exception:
            pass

        # 2) запасной путь — /orders/{id}/entries
        try:
            r = await cli.get(f"/orders/{order_id}/entries",
                              params={"page[size]": "200"},
                              headers=headers)
            r.raise_for_status()
            j = r.json()
            data = j.get("data") or []
            if data:
                attrs_e = data[0].get("attributes", {}) or {}
                title = ""
                for key in ("offerName","title","name","productName","shortName"):
                    v = attrs_e.get(key)
                    if isinstance(v, str) and v.strip():
                        title = v.strip(); break
                sku = ""
                off = attrs_e.get("offer") or {}
                if isinstance(off, dict) and off.get("code"):
                    sku = off["code"]
                for key in ("sku","code","productCode"):
                    v = attrs_e.get(key)
                    if isinstance(v, str) and v.strip():
                        sku = v.strip(); break
                return {"sku": sku, "title": title}
        except Exception:
            pass

    return None

# ---------- модели ----------
class DayPoint(BaseModel):
    x: str
    count: int
    amount: float = 0.0

class CityCount(BaseModel):
    city: str
    count: int

class AnalyticsResponse(BaseModel):
    range: Dict[str, str]
    timezone: str
    currency: str
    date_field: str
    total_orders: int
    total_amount: float
    days: List[DayPoint]
    prev_days: List[DayPoint] = []
    cities: List[CityCount] = []
    state_breakdown: Dict[str, int] = {}

# ---------- META ----------
@app.get("/auth/meta", tags=["auth"])
def auth_meta():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_ANON_KEY")
    if not url or not key:
        raise HTTPException(status_code=500, detail="Missing SUPABASE_URL or SUPABASE_ANON_KEY")
    return {"SUPABASE_URL": url, "SUPABASE_ANON_KEY": key}

@app.get("/meta")
async def meta():
    return {
        "shop": SHOP_NAME,
        "partner_id": PARTNER_ID,
        "timezone": DEFAULT_TZ,
        "currency": CURRENCY,
        "amount_fields": AMOUNT_FIELDS,
        "divisor": AMOUNT_DIVISOR,
        "chunk_days": CHUNK_DAYS,
        "date_field_default": DATE_FIELD_DEFAULT,
        "date_field_options": DATE_FIELD_OPTIONS,
        "city_keys": CITY_KEYS,
        "use_business_day": USE_BUSINESS_DAY,
        "business_day_start": BUSINESS_DAY_START,
        "store_accept_until": STORE_ACCEPT_UNTIL,
        "scan_field": SCAN_FIELD,
        "scan_margin_days": SCAN_MARGIN_DAYS,
    }

# ---------- утилиты состояний ----------
def _normalize_states_inc(states_inc: set[str] | None, expand_archive: bool = False) -> set[str]:
    if not states_inc:
        return set()
    out = set(states_inc)
    if expand_archive and (("KASPI_DELIVERY" in out) or ("DELIVERED" in out)):
        out |= {"ARCHIVE", "ARCHIVED"}
    return out

# ---------- ядро сбора ----------
def _collect_range(
    start_dt: datetime, end_dt: datetime, tz: str, date_field: str,
    states_inc: Optional[set], states_ex: set,
    assign_mode: str, store_accept_until: str, business_day_start: str,
) -> tuple[list[DayPoint], Dict[str, int], int, float, Dict[str, int], List[Dict[str, object]]]:

    tzinfo = tzinfo_of(tz)

    # расширяем окно сканирования по дням, чтобы не потерять «переехавшие» заказы
    scan_start = start_dt - timedelta(days=SCAN_MARGIN_DAYS)
    scan_end   = end_dt   + timedelta(days=SCAN_MARGIN_DAYS)

    seen_ids: set[str] = set()
    day_counts: Dict[str, int]    = {}
    day_amounts: Dict[str, float] = {}
    city_counts: Dict[str, int]   = {}
    state_counts: Dict[str, int]  = {}

    total_orders = 0
    total_amount = 0.0
    flat_out: List[Dict[str, object]] = []

    states_inc = _normalize_states_inc(states_inc, expand_archive=True)

    want_start_day = start_dt.astimezone(tzinfo).date().isoformat()
    want_end_day   = end_dt.astimezone(tzinfo).date().isoformat()

    if client is None:
        raise HTTPException(status_code=500, detail="Kaspi client not configured")

    # ВСЕГДА сканируем по SCAN_FIELD (обычно creationDate)
    for s, e in iter_chunks(scan_start, scan_end, CHUNK_DAYS):
        try:
            # если магазин не умеет фильтрацию по SCAN_FIELD — считаем это фатальной ошибкой
            for order in client.iter_orders(start=s, end=e, filter_field=SCAN_FIELD):
                oid = str(order.get("id"))
                if oid in seen_ids:
                    continue

                attrs = order.get("attributes", {}) or {}

                st = norm_state(str(attrs.get("state", "")))
                if states_inc and st not in states_inc:
                    continue
                if st in states_ex:
                    continue

                # Время приёма (accept): всегда creationDate
                ms_accept = extract_ms(attrs, "creationDate")
                if ms_accept is None:
                    continue
                dt_accept  = datetime.fromtimestamp(ms_accept / 1000, tz=pytz.UTC).astimezone(tzinfo)
                day_accept = dt_accept.date().isoformat()

                # Pivot (выбранное поле) — для business/диагностики
                ms_pivot = extract_ms(attrs, date_field) or ms_accept
                dt_pivot = datetime.fromtimestamp(ms_pivot / 1000, tz=pytz.UTC).astimezone(tzinfo)

                # Определяем день принадлежности по режиму
                if assign_mode == "smart":
                    op_day, reason = _smart_operational_day(attrs, st, tzinfo, store_accept_until, business_day_start)
                    # для smart фильтруем по дню
                    if not (want_start_day <= op_day <= want_end_day):
                        continue
                elif assign_mode == "business":
                    op_day, reason = bucket_date(dt_pivot, use_bd=True, bd_start=business_day_start), "business"
                    if not (want_start_day <= op_day <= want_end_day):
                        continue
                else:
                    # RAW: фильтруем по точному времени приёма
                    if not (start_dt <= dt_accept <= end_dt):
                        continue
                    op_day, reason = day_accept, "raw"

                amt  = extract_amount(attrs)
                city = extract_city(attrs)

                day_counts[op_day]  = day_counts.get(op_day, 0) + 1
                day_amounts[op_day] = day_amounts.get(op_day, 0.0) + amt
                if city:
                    city_counts[city] = city_counts.get(city, 0) + 1
                state_counts[st] = state_counts.get(st, 0) + 1

                total_orders += 1
                total_amount += amt

                flat_out.append({
                    "id": oid,
                    "number": _guess_number(attrs, oid),
                    "state": st,
                    "date": dt_accept.isoformat(),       # время приёма (accept)
                    "date_ms": ms_accept,                # сырой штамп мс приёма
                    "date_pivot": dt_pivot.isoformat(),  # поворотный штамп (для business/диагностики)
                    "op_day": op_day,
                    "op_reason": reason,
                    "amount": round(amt, 2),
                    "city": city,
                })

                seen_ids.add(oid)

        except HTTPStatusError as ee:
            # фатально: магазин не умеет фильтрацию по SCAN_FIELD
            raise HTTPException(status_code=502, detail=f"Scan failed for field '{SCAN_FIELD}': {ee}")
        except RequestError as e:
            raise HTTPException(status_code=502, detail=f"Network: {e}")

    # ось дней
    out_days: List[DayPoint] = []
    cur = start_dt.astimezone(tzinfo).date()
    end_d = end_dt.astimezone(tzinfo).date()
    while cur <= end_d:
        key = cur.isoformat()
        out_days.append(DayPoint(x=key, count=day_counts.get(key, 0), amount=round(day_amounts.get(key, 0.0), 2)))
        cur = cur + timedelta(days=1)

    return out_days, city_counts, total_orders, round(total_amount, 2), state_counts, flat_out

# ---------- публичные эндпойнты: аналитика ----------
@app.get("/orders/analytics", response_model=AnalyticsResponse)
async def analytics(
    start: str = Query(...), end: str = Query(...), tz: str = Query(DEFAULT_TZ),
    date_field: str = Query(DATE_FIELD_DEFAULT),
    states: Optional[str] = Query(None), exclude_states: Optional[str] = Query(None),
    with_prev: bool = Query(True),
    # уважает UI: по умолчанию НЕ исключаем CANCELED
    exclude_canceled: Optional[bool] = Query(None),
    start_time: Optional[str] = Query(None), end_time: Optional[str] = Query(None),
    use_bd: Optional[bool] = Query(None), business_day_start: Optional[str] = Query(None),
    assign_mode: str = Query("smart", pattern="^(smart|business|raw)$"),
    store_accept_until: Optional[str] = Query(None),
):
    tzinfo = tzinfo_of(tz)

    eff_use_bd = USE_BUSINESS_DAY if use_bd is None else bool(use_bd)  # оставлено для метаданных
    eff_bds    = business_day_start or BUSINESS_DAY_START

    start_dt = parse_date_local(start, tz)
    end_dt   = parse_date_local(end, tz) + timedelta(days=1) - timedelta(milliseconds=1)

    # ВРЕМЯРЕЗ применяем ТОЛЬКО в raw-режиме (по времени приёма)
    if assign_mode == "raw":
        if start_time:
            start_dt = apply_hhmm(start_dt, start_time)
        if end_time:
            e0 = parse_date_local(end, tz)
            end_dt = apply_hhmm(e0, end_time)

    if end_dt < start_dt:
        raise HTTPException(status_code=400, detail="end < start")

    inc = parse_states_csv(states)
    exc = parse_states_csv(exclude_states) or set()
    if (exclude_canceled is True) and not exclude_states:
        exc |= {"CANCELED"}

    days, cities_dict, tot, tot_amt, st_counts, _ = _collect_range(
        start_dt, end_dt, tz, date_field, inc, exc,
        assign_mode=assign_mode,
        store_accept_until=(store_accept_until or STORE_ACCEPT_UNTIL),
        business_day_start=eff_bds,
    )

    cities_list = [{"city": c, "count": n} for c, n in sorted(cities_dict.items(), key=lambda x: -x[1])]

    prev_days: List[DayPoint] = []
    if with_prev:
        span_days = (end_dt.date() - start_dt.date()).days + 1
        prev_end   = start_dt - timedelta(milliseconds=1)
        prev_start = prev_end - timedelta(days=span_days) + timedelta(milliseconds=1)
        prev_days, _, _, _, _, _ = _collect_range(
            prev_start, prev_end, tz, date_field, inc, exc,
            assign_mode=assign_mode,
            store_accept_until=(store_accept_until or STORE_ACCEPT_UNTIL),
            business_day_start=eff_bds,
        )

    return {
        "range": {"start": start_dt.astimezone(tzinfo).date().isoformat(),
                  "end":   end_dt.astimezone(tzinfo).date().isoformat()},
        "timezone": tz,
        "currency": CURRENCY,
        "date_field": date_field,
        "total_orders": tot,
        "total_amount": tot_amt,
        "days": days,
        "prev_days": prev_days,
        "cities": cities_list,
        "state_breakdown": st_counts,
    }

# ---------- /orders/ids CORE ----------
def _select_targets(out: List[Dict[str, object]], enrich_day: str, enrich_scope: str, limit: int) -> List[Dict[str, object]]:
    if enrich_scope == "none":
        return []
    if enrich_scope == "all":
        return out if not limit or limit <= 0 else out[:limit]
    if enrich_scope == "last_day":
        sel = [it for it in out if str(it["op_day"]) == enrich_day]
        return sel if not limit or limit <= 0 else sel[:limit]
    if enrich_scope in ("last_week", "last_month"):
        try:
            end_d = _date.fromisoformat(enrich_day)
        except Exception:
            return out if not limit or limit <= 0 else out[:limit]
        span = 7 if enrich_scope == "last_week" else 30
        start_d = end_d - timedelta(days=span-1)
        s, e = start_d.isoformat(), end_d.isoformat()
        sel = [it for it in out if s <= str(it["op_day"]) <= e]
        return sel if not limit or limit <= 0 else sel[:limit]
    return out

async def _list_ids_core(
    start: str, end: str, tz: str, date_field: str,
    states: Optional[str], exclude_states: Optional[str],
    use_bd: Optional[bool], business_day_start: Optional[str],
    limit: int, order: str, grouped: int,
    with_items: int, enrich_scope: str,
    assign_mode: str, store_accept_until: Optional[str],
    start_time: Optional[str] = None, end_time: Optional[str] = None,
    progress_cb: Optional[Callable[[str, int, int, str], None]] = None,
) -> Dict[str, object]:

    tzinfo = tzinfo_of(tz)
    eff_use_bd = USE_BUSINESS_DAY if use_bd is None else bool(use_bd)  # метаданные
    eff_bds    = business_day_start or BUSINESS_DAY_START

    start_dt = parse_date_local(start, tz)
    end_dt   = parse_date_local(end, tz) + timedelta(days=1) - timedelta(milliseconds=1)

    # времярез — только для raw
    if assign_mode == "raw":
        if start_time:
            start_dt = apply_hhmm(start_dt, start_time)
        if end_time:
            e0 = parse_date_local(end, tz)
            end_dt = apply_hhmm(e0, end_time)

    inc = parse_states_csv(states)
    exc = parse_states_csv(exclude_states) or set()

    _, _, _, _, _, out = _collect_range(
        start_dt, end_dt, tz, date_field, inc, exc,
        assign_mode=assign_mode,
        store_accept_until=(store_accept_until or STORE_ACCEPT_UNTIL),
        business_day_start=eff_bds,
    )

    # сортировка и обрезка
    out.sort(key=lambda it: (str(it["op_day"]), str(it["date"])), reverse=(order == "desc"))
    if limit and limit > 0:
        out = out[:limit]

    # группировка для UI
    groups: List[Dict[str, object]] = []
    if grouped:
        cur_day: Optional[str] = None
        bucket: List[Dict[str, object]] = []
        for it in out:
            d = str(it["op_day"])
            if cur_day is None:
                cur_day = d
            if d != cur_day:
                groups.append({
                    "day": cur_day,
                    "items": bucket,
                    "total_amount": round(sum(float(x.get("amount", 0) or 0) for x in bucket), 2),
                })
                cur_day, bucket = d, []
            bucket.append(it)
        if cur_day is not None:
            groups.append({
                "day": cur_day,
                "items": bucket,
                "total_amount": round(sum(float(x.get("amount", 0) or 0) for x in bucket), 2),
            })

    # обогащение
    if with_items and out and enrich_scope != "none":
        enrich_day  = bucket_date(end_dt.astimezone(tzinfo), use_bd=eff_use_bd, bd_start=eff_bds)
        targets     = _select_targets(out, enrich_day, enrich_scope, limit)
        total_t     = len(targets)

        sem = asyncio.Semaphore(max(1, ENRICH_CONCURRENCY))
        done = 0
        if progress_cb: progress_cb("enrich", done, total_t, "enrich start")

        async def enrich(it):
            nonlocal done
            async with sem:
                extra = await _first_item_details(str(it["id"]), timeout_scale=1.0 + (0.5 if total_t >= 400 else 0.0))
                if extra:
                    it["sku"]   = extra.get("sku")
                    it["title"] = extra.get("title")
                done += 1
                if progress_cb:
                    progress_cb("enrich", done, total_t, f"enrich {done}/{total_t}")
                await asyncio.sleep(0.02)

        await asyncio.gather(*(enrich(it) for it in targets))

    period_total_amount = round(sum(float(it.get("amount", 0) or 0) for it in out), 2)
    period_total_count  = len(out)

    return {
        "items": out,
        "groups": groups,
        "period_total_count": period_total_count,
        "period_total_amount": period_total_amount,
        "currency": CURRENCY,
    }

# ---------- /orders/ids ----------
@app.get("/orders/ids")
async def list_ids(
    start: str = Query(...),
    end: str = Query(...),
    tz: str = Query(DEFAULT_TZ),
    date_field: str = Query(DATE_FIELD_DEFAULT),
    states: Optional[str] = Query(None),
    exclude_states: Optional[str] = Query(None),
    use_bd: Optional[bool] = Query(None),
    business_day_start: Optional[str] = Query(None),
    limit: int = Query(0, description="0 = без ограничения"),
    order: str = Query("asc", pattern="^(asc|desc)$"),
    grouped: int = Query(1),
    with_items: int = Query(1, description="1=тянуть первую позицию"),
    enrich_scope: str = Query("last_day", pattern="^(none|last_day|last_week|last_month|all)$"),
    assign_mode: str = Query("smart", pattern="^(smart|business|raw)$"),
    store_accept_until: Optional[str] = Query(None),
    start_time: Optional[str] = Query(None),
    end_time: Optional[str] = Query(None),
):
    return await _list_ids_core(
        start, end, tz, date_field, states, exclude_states,
        use_bd, business_day_start, limit, order, grouped,
        with_items, enrich_scope, assign_mode, store_accept_until,
        start_time, end_time, None
    )

# ---------- CSV ----------
@app.get("/orders/ids.csv", response_class=PlainTextResponse)
async def list_ids_csv(
    start: str = Query(...),
    end: str = Query(...),
    tz: str = Query(DEFAULT_TZ),
    date_field: str = Query(DATE_FIELD_DEFAULT),
    states: Optional[str] = Query(None),
    exclude_states: Optional[str] = Query(None),
    use_bd: Optional[bool] = Query(None),
    business_day_start: Optional[str] = Query(None),
    order: str = Query("asc", pattern="^(asc|desc)$"),
    assign_mode: str = Query("smart", pattern="^(smart|business|raw)$"),
    store_accept_until: Optional[str] = Query(None),
    start_time: Optional[str] = Query(None),
    end_time: Optional[str] = Query(None),
):
    data = await _list_ids_core(
        start, end, tz, date_field, states, exclude_states,
        use_bd, business_day_start,
        limit=100000, order=order, grouped=0,
        with_items=0, enrich_scope="none",
        assign_mode=assign_mode, store_accept_until=store_accept_until,
        start_time=start_time, end_time=end_time, progress_cb=None
    )
    return "\n".join([str(it["number"]) for it in data["items"]])

# ---------- Async + jobs ----------
Jobs: Dict[str, Dict[str, object]] = {}

def _new_job() -> str:
    job_id = uuid.uuid4().hex
    Jobs[job_id] = {
        "status": "queued", "phase": "scan", "progress": 0.0, "message": "",
        "created": datetime.utcnow().isoformat()+"Z", "updated": datetime.utcnow().isoformat()+"Z",
        "total": 0, "done": 0, "result": None, "cancel": False,
    }
    return job_id

def _job_update(job_id: str, **patch):
    st = Jobs.get(job_id)
    if not st: return
    st.update(patch); st["updated"] = datetime.utcnow().isoformat()+"Z"

def _job_progress_cb(job_id: Optional[str]):
    if not job_id: return None
    def cb(phase: str, done: int, total: int, extra_msg: str = ""):
        if job_id not in Jobs: return
        if Jobs[job_id].get("cancel"): return
        prog = 0.0
        if total > 0:
            if phase == "scan":
                prog = min(0.6, 0.6 * (done / total))
            else:
                prog = 0.6 + min(0.4, 0.4 * (done / total))
        _job_update(job_id, phase=phase, progress=prog, done=done, total=total, message=extra_msg or Jobs[job_id].get("message",""))
    return cb

@app.post("/orders/ids.async")
async def list_ids_async(
    start: str = Query(...),
    end: str = Query(...),
    tz: str = Query(DEFAULT_TZ),
    date_field: str = Query(DATE_FIELD_DEFAULT),
    states: Optional[str] = Query(None),
    exclude_states: Optional[str] = Query(None),
    use_bd: Optional[bool] = Query(None),
    business_day_start: Optional[str] = Query(None),
    limit: int = Query(0, description="0 = без ограничения"),
    order: str = Query("asc", pattern="^(asc|desc)$"),
    grouped: int = Query(1),
    with_items: int = Query(1),
    enrich_scope: str = Query("last_day", pattern="^(none|last_day|last_week|last_month|all)$"),
    assign_mode: str = Query("smart", pattern="^(smart|business|raw)$"),
    store_accept_until: Optional[str] = Query(None),
    start_time: Optional[str] = Query(None),
    end_time: Optional[str] = Query(None),
):
    job_id = _new_job()
    async def worker():
        try:
            _job_update(job_id, status="running", message="started")
            res = await _list_ids_core(
                start, end, tz, date_field, states, exclude_states,
                use_bd, business_day_start, limit, order, grouped,
                with_items, enrich_scope, assign_mode, store_accept_until,
                start_time, end_time,
                progress_cb=_job_progress_cb(job_id)
            )
            if Jobs.get(job_id, {}).get("cancel"):
                _job_update(job_id, status="canceled", message="canceled by user", result=None)
            else:
                _job_update(job_id, status="done", progress=1.0, message="done", result=res)
        except Exception as e:
            _job_update(job_id, status="error", message=str(e))
    asyncio.create_task(worker())
    return {"job_id": job_id}

@app.get("/jobs/{job_id}")
async def job_status(job_id: str):
    st = Jobs.get(job_id)
    if not st: raise HTTPException(status_code=404, detail="job not found")
    payload = {k: v for k, v in st.items() if k != "result"}
    if st.get("status") == "done": payload["result_ready"] = True
    return JSONResponse(payload)

@app.get("/jobs/{job_id}/result")
async def job_result(job_id: str):
    st = Jobs.get(job_id)
    if not st: raise HTTPException(status_code=404, detail="job not found")
    if st.get("status") != "done": raise HTTPException(status_code=409, detail="job not finished")
    return JSONResponse(st.get("result") or {})

@app.delete("/jobs/{job_id}")
async def job_cancel(job_id: str):
    st = Jobs.get(job_id)
    if not st: raise HTTPException(status_code=404, detail="job not found")
    st["cancel"] = True
    return {"ok": True}

# ---------- ROOT ----------
@app.get("/", include_in_schema=False)
async def root():
    return RedirectResponse(url="/ui/")
