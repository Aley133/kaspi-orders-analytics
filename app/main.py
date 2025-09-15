# app/main.py
from __future__ import annotations

import os
import re
import uuid
import random
import asyncio
from datetime import datetime, timedelta, time, date
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

# tenant token middleware (Supabase → request.state)
from app.deps.auth import attach_kaspi_token_middleware, get_current_kaspi_token

# routers
from app.api.bridge_v2 import router as bridge_router
from app.api.profit_fifo import get_profit_fifo_router
from app.api.authz import router as auth_router
from app.api.products import get_products_router
from app.api import settings as settings_api

# tenant-aware Kaspi client
from app.deps.kaspi_client_tenant import KaspiClient as TenantKaspiClient

# ---------- ENV ----------
load_dotenv()

DEFAULT_TZ       = os.getenv("TZ", "Asia/Almaty")
KASPI_BASE_URL   = os.getenv("KASPI_BASE_URL", "https://kaspi.kz/shop/api/v2").rstrip("/")
CURRENCY         = os.getenv("CURRENCY", "KZT")
SHOP_NAME        = os.getenv("SHOP_NAME", "LeoXpress")
PARTNER_ID       = os.getenv("PARTNER_ID", "")

AMOUNT_FIELDS    = [s.strip() for s in os.getenv("AMOUNT_FIELDS", "totalPrice").split(",") if s.strip()]
AMOUNT_DIVISOR   = float(os.getenv("AMOUNT_DIVISOR", "1") or 1)

DATE_FIELD_DEFAULT  = os.getenv("DATE_FIELD_DEFAULT", "creationDate")
DATE_FIELD_OPTIONS  = [s.strip() for s in os.getenv("DATE_FIELD_OPTIONS", "creationDate,plannedShipmentDate,shipmentDate,deliveryDate").split(",") if s.strip()]
CITY_KEYS           = [s.strip() for s in os.getenv("CITY_KEYS", "city,deliveryAddress.city").split(",") if s.strip()]

CHUNK_DAYS   = int(os.getenv("CHUNK_DAYS", "7") or 7)
CACHE_TTL    = int(os.getenv("CACHE_TTL", "300") or 300)

# business-day / smart-mode
BUSINESS_DAY_START  = os.getenv("BUSINESS_DAY_START", "20:00")  # HH:MM
USE_BUSINESS_DAY    = os.getenv("USE_BUSINESS_DAY", "true").lower() in ("1","true","yes","on")
STORE_ACCEPT_UNTIL  = os.getenv("STORE_ACCEPT_UNTIL", "17:00")

# enrichment
ENRICH_CONCURRENCY  = int(os.getenv("ENRICH_CONCURRENCY", "6") or 6)

# effective flags used by bucket_date
_EFF_USE_BD: bool = False
_EFF_BDS: str     = BUSINESS_DAY_START

# ---------- FastAPI ----------
app = FastAPI(title="Kaspi Orders Analytics")

origins = [o.strip() for o in os.getenv("ALLOWED_ORIGINS", "").split(",") if o.strip()]
if origins:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_methods=["GET","POST","PUT","DELETE","OPTIONS"],
        allow_headers=["*"],
        allow_credentials=False,
    )

# attach middleware that resolves per-tenant Kaspi token
app.middleware("http")(attach_kaspi_token_middleware)

# tenant-aware client
client = TenantKaspiClient(base_url=KASPI_BASE_URL)

# cache
orders_cache = TTLCache(maxsize=512, ttl=CACHE_TTL)

# static UI (best-effort search)
_ui_candidates = ("app/static", "app/ui", "static", "ui")
_ui_dir = next((p for p in _ui_candidates if Path(p).is_dir()), None)
if _ui_dir:
    app.mount("/ui", StaticFiles(directory=_ui_dir, html=True), name="ui")

# routers
app.include_router(get_products_router(client), prefix="/products")
app.include_router(get_profit_fifo_router(), prefix="/profit")
app.include_router(bridge_router, prefix="/profit")
app.include_router(auth_router)
app.include_router(settings_api.router, prefix="/settings", tags=["settings"])

# ---------- helpers ----------
def _bd_delta(hhmm: str) -> timedelta:
    try:
        h, m = map(int, hhmm.split(":"))
        return timedelta(hours=h, minutes=m)
    except Exception:
        return timedelta(0)

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
    for k in path.split("."):
        if not isinstance(cur, dict):
            return None
        if k not in cur:
            return None
        cur = cur[k]
    return cur

def norm_state(s: str) -> str:
    return (s or "").strip().upper()

def parse_states_csv(s: Optional[str]) -> Optional[set[str]]:
    if not s:
        return None
    return {norm_state(x) for x in re.split(r"[\s,;]+", s) if x.strip()}

_CITY_KEY_HINTS = {"city","cityname","town","locality","settlement"}

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
            if isinstance(k, str) and any(h in k.lower() for h in _CITY_KEY_HINTS) and isinstance(v, str) and v.strip():
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
            pass
    return total / (AMOUNT_DIVISOR or 1.0)

def extract_ms(attrs: dict, field: str) -> Optional[int]:
    """
    Извлечение таймштампа из поля заказа.
    Поддержка алиасов: creationDate ↔ date; допускаем ISO-строки.
    """
    v = attrs.get(field)
    # alias for creationDate
    if v is None and field == "creationDate":
        v = attrs.get("date")
    if v is None:
        return None
    # int millis
    try:
        return int(v)
    except Exception:
        pass
    # ISO → millis
    try:
        iso = str(v).replace("Z", "+00:00")
        return int(datetime.fromisoformat(iso).timestamp() * 1000)
    except Exception:
        return None

def bucket_date(dt_local: datetime) -> str:
    if _EFF_USE_BD:
        shift = timedelta(hours=24) - _bd_delta(_EFF_BDS)
        return (dt_local + shift).date().isoformat()
    return dt_local.date().isoformat()

def _guess_number(attrs: dict, fallback_id: str) -> str:
    for k in ("number","code","orderNumber"):
        v = attrs.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return str(fallback_id)

# ---------- HTTPX base (for /entries enrichment) ----------
BASE_TIMEOUT = httpx.Timeout(connect=10.0, read=80.0, write=20.0, pool=60.0)
HTTPX_LIMITS  = httpx.Limits(max_connections=20, max_keepalive_connections=10)

def _kaspi_headers() -> Dict[str, str]:
    tok = get_current_kaspi_token()
    if not tok:
        raise HTTPException(status_code=401, detail="Kaspi token is not set for this tenant")
    return {
        "X-Auth-Token": tok,
        "Accept": "application/vnd.api+json",
        "Content-Type": "application/vnd.api+json",
        "User-Agent": "leo-analytics/1.0",
    }

def _async_client(scale: float = 1.0):
    scale = max(1.0, float(scale))
    return httpx.AsyncClient(base_url=KASPI_BASE_URL,
                             timeout=httpx.Timeout(connect=BASE_TIMEOUT.connect,
                                                   read=min(420.0, BASE_TIMEOUT.read * scale),
                                                   write=min(150.0, BASE_TIMEOUT.write * scale),
                                                   pool=BASE_TIMEOUT.pool),
                             limits=HTTPX_LIMITS)

# ---------- Smart-day helpers ----------
_DELIVERED_STATES = {"KASPI_DELIVERY", "DELIVERED", "ARCHIVE", "ARCHIVED"}

def _smart_operational_day(attrs: dict, state: str, tzinfo: pytz.BaseTzInfo,
                           store_accept_until: str, business_day_start: str) -> Tuple[str, str]:
    ms_creation = extract_ms(attrs, "creationDate")  # covers 'date'
    ms_planned  = extract_ms(attrs, "plannedShipmentDate")
    ms_ship     = extract_ms(attrs, "shipmentDate")

    dt_creation = datetime.fromtimestamp(ms_creation/1000, tz=pytz.UTC).astimezone(tzinfo) if ms_creation else None
    dt_planned  = datetime.fromtimestamp(ms_planned/1000,  tz=pytz.UTC).astimezone(tzinfo) if ms_planned  else None
    dt_ship     = datetime.fromtimestamp(ms_ship/1000,     tz=pytz.UTC).astimezone(tzinfo) if ms_ship     else None

    # доставленные: считаем по бизнес-дню от shipment/planned/creation
    if state in _DELIVERED_STATES:
        base = dt_ship or dt_planned or dt_creation or datetime.now(tzinfo)
        shift = timedelta(hours=24) - _bd_delta(business_day_start)
        op = (base + shift).date().isoformat()
        return op, "delivered_business_day"

    if dt_planned:
        return dt_planned.date().isoformat(), "planned"

    cutoff = time(*map(int, store_accept_until.split(":")))
    if dt_creation:
        if dt_creation.time() <= cutoff:
            return dt_creation.date().isoformat(), "created_before_cutoff"
        else:
            return (dt_creation + timedelta(days=1)).date().isoformat(), "created_after_cutoff_next_day"

    return datetime.now(tzinfo).date().isoformat(), "fallback_now"

# ---------- Enrichment (first item only; fast) ----------
async def _first_item_details(order_id: str, timeout_scale: float = 1.0) -> Optional[Dict[str, object]]:
    async with _async_client(scale=timeout_scale) as cli:
        try:
            r = await cli.get("/orderentries",
                              params={"filter[order.id]": order_id, "page[size]": "200"},
                              headers=_kaspi_headers())
            j = r.json()
            data = (j.get("data") or [])
            if not data:
                return None
            attrs_e = data[0].get("attributes", {}) or {}
            qty = int(attrs_e.get("quantity") or attrs_e.get("qty") or 1)
            price = float(attrs_e.get("basePrice") or attrs_e.get("unitPrice") or attrs_e.get("price") or 0)
            title = ""
            for key in ("offerName","title","name","productName","shortName"):
                v = attrs_e.get(key)
                if isinstance(v, str) and v.strip():
                    title = v.strip()
                    break
            sku = ""
            for key in ("sku","code","productCode"):
                v = attrs_e.get(key)
                if isinstance(v, str) and v.strip():
                    sku = v.strip()
                    break
            offer = attrs_e.get("offer") or {}
            if isinstance(offer, dict) and offer.get("code"):
                sku = offer["code"]
            return {"sku": sku, "title": title, "qty": qty, "unit_price": price}
        except Exception:
            return None

# ---------- models ----------
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

# ---------- meta ----------
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
    }

# ---------- core collection ----------
def _collect_range(
    start_dt: datetime, end_dt: datetime, tz: str, date_field: str,
    states_inc: Optional[set], states_ex: set,
    assign_mode: str, store_accept_until: str,
) -> tuple[list[DayPoint], Dict[str, int], int, float, Dict[str, int], List[Dict[str, object]]]:

    tzinfo = tzinfo_of(tz)
    seen_ids: set[str] = set()
    day_counts: Dict[str, int]   = {}
    day_amounts: Dict[str, float] = {}
    city_counts: Dict[str, int]  = {}
    state_counts: Dict[str, int] = {}
    flat_out: List[Dict[str, object]] = []

    total_orders = 0
    total_amount = 0.0

    # для отфильтровывания по диапазону используем строки YYYY-MM-DD
    range_start_day = start_dt.astimezone(tzinfo).date().isoformat()
    range_end_day   = end_dt.astimezone(tzinfo).date().isoformat()

    for s, e in iter_chunks(start_dt, end_dt, CHUNK_DAYS):
        try:
            try_field = date_field
            while True:
                try:
                    for order in client.iter_orders(start=s, end=e, filter_field=try_field):
                        oid   = str(order.get("id"))
                        if oid in seen_ids:
                            continue
                        attrs = order.get("attributes", {}) or {}

                        st = norm_state(str(attrs.get("state", "")))
                        if states_inc and st not in states_inc:
                            continue
                        if st in states_ex:
                            continue

                        # «сырой» день по выбранному полю
                        ms = extract_ms(attrs, date_field)
                        if ms is None:
                            # пробуем fallback к creationDate (на случай, если выбрали planned*, а его нет)
                            ms = extract_ms(attrs, "creationDate")
                        if ms is None:
                            continue

                        dtt = datetime.fromtimestamp(ms/1000, tz=pytz.UTC).astimezone(tzinfo)
                        raw_day = dtt.date().isoformat()

                        # какой день считаем «операционным» для отчёта
                        if assign_mode == "smart":
                            op_day, reason = _smart_operational_day(attrs, st, tzinfo, store_accept_until, _EFF_BDS)
                        elif assign_mode == "business":
                            op_day, reason = bucket_date(dtt), "business"
                        else:
                            op_day, reason = raw_day, "raw"

                        # защитный бридж: если «умный» день вышел за пределы выбранного диапазона,
                        # но «сырой» день попадает — учитываем RAW (чтобы не пустел однодневный фильтр)
                        if not (range_start_day <= op_day <= range_end_day) and (range_start_day <= raw_day <= range_end_day):
                            op_day, reason = raw_day, "raw_bridge"

                        if not (range_start_day <= op_day <= range_end_day):
                            continue

                        amt  = extract_amount(attrs)
                        city = extract_city(attrs)

                        day_counts[op_day]    = day_counts.get(op_day, 0) + 1
                        day_amounts[op_day]   = day_amounts.get(op_day, 0.0) + amt
                        if city:
                            city_counts[city]  = city_counts.get(city, 0) + 1
                        state_counts[st]      = state_counts.get(st, 0) + 1

                        total_orders += 1
                        total_amount += amt

                        flat_out.append({
                            "id": oid,
                            "number": _guess_number(attrs, oid),
                            "state": st,
                            "date": dtt.isoformat(),
                            "op_day": op_day,
                            "amount": round(amt, 2),
                            "city": city,
                            "op_reason": reason,
                        })

                        seen_ids.add(oid)
                    break
                except HTTPStatusError as ee:
                    # если магазин не умеет фильтровать по выбранному полю — откатываемся к creationDate
                    if ee.response.status_code in (400, 422) and try_field != "creationDate":
                        try_field = "creationDate"
                        continue
                    raise
        except RequestError as e:
            raise HTTPException(status_code=502, detail=f"Network: {e}")

    # подготовка оси дней
    out_days: List[DayPoint] = []
    cur = start_dt.astimezone(tzinfo).date()
    end_d = end_dt.astimezone(tzinfo).date()
    while cur <= end_d:
        key = cur.isoformat()
        out_days.append(DayPoint(x=key, count=day_counts.get(key, 0), amount=round(day_amounts.get(key, 0.0), 2)))
        cur = cur + timedelta(days=1)

    return out_days, city_counts, total_orders, round(total_amount, 2), state_counts, flat_out

# ---------- analytics ----------
@app.get("/orders/analytics", response_model=AnalyticsResponse)
async def analytics(
    start: str = Query(...), end: str = Query(...), tz: str = Query(DEFAULT_TZ),
    date_field: str = Query(DATE_FIELD_DEFAULT),
    states: Optional[str] = Query(None), exclude_states: Optional[str] = Query(None),
    with_prev: bool = Query(True), exclude_canceled: bool = Query(True),
    start_time: Optional[str] = Query(None), end_time: Optional[str] = Query(None),
    use_bd: Optional[bool] = Query(None), business_day_start: Optional[str] = Query(None),
    assign_mode: str = Query("smart", pattern="^(smart|business|raw)$"),
    store_accept_until: Optional[str] = Query(None),
):
    tzinfo = tzinfo_of(tz)

    # effective flags for business-day calculations
    global _EFF_USE_BD, _EFF_BDS
    eff_use_bd = USE_BUSINESS_DAY if use_bd is None else bool(use_bd)
    eff_bds    = BUSINESS_DAY_START if not business_day_start else business_day_start
    _EFF_USE_BD, _EFF_BDS = eff_use_bd, eff_bds

    start_dt = parse_date_local(start, tz)
    end_dt   = parse_date_local(end, tz) + timedelta(days=1) - timedelta(milliseconds=1)

    # “raw” режим — можно ограничить временем (HH:MM)
    if not eff_use_bd:
        if start_time:
            start_dt = apply_hhmm(start_dt, start_time)
        if end_time:
            e0 = parse_date_local(end, tz)
            end_dt = apply_hhmm(e0, end_time).replace(tzinfo=tzinfo)

    if end_dt < start_dt:
        raise HTTPException(status_code=400, detail="end < start")

    inc = parse_states_csv(states)
    exc = parse_states_csv(exclude_states) or set()
    if exclude_canceled:
        exc |= {"CANCELED"}

    days, cities_dict, tot, tot_amt, st_counts, _ = _collect_range(
        start_dt, end_dt, tz, date_field, inc, exc,
        assign_mode=assign_mode, store_accept_until=(store_accept_until or STORE_ACCEPT_UNTIL)
    )

    cities_list = [{"city": c, "count": n} for c, n in sorted(cities_dict.items(), key=lambda x: -x[1])]

    prev_days: List[DayPoint] = []
    if with_prev:
        span_days = (end_dt.date() - start_dt.date()).days + 1
        prev_end   = start_dt - timedelta(milliseconds=1)
        prev_start = prev_end - timedelta(days=span_days) + timedelta(milliseconds=1)
        prev_days, _, _, _, _, _ = _collect_range(
            prev_start, prev_end, tz, date_field, inc, exc,
            assign_mode=assign_mode, store_accept_until=(store_accept_until or STORE_ACCEPT_UNTIL)
        )

    return {
        "range": {
            "start": start_dt.astimezone(tzinfo).date().isoformat(),
            "end":   end_dt.astimezone(tzinfo).date().isoformat(),
        },
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

# ---------- list ids (для сверки/экспорта) ----------
def _select_targets(out: List[Dict[str, object]], enrich_day: str, enrich_scope: str, limit: int) -> List[Dict[str, object]]:
    if enrich_scope == "none":
        return []
    if enrich_scope == "all":
        return out if not limit or limit <= 0 else out[:limit]
    if enrich_scope == "last_day":
        sel = [it for it in out if str(it["op_day"]) == enrich_day]
        return sel if not limit or limit <= 0 else sel[:limit]
    return []

@app.get("/orders/ids")
async def list_ids(
    start: str = Query(...), end: str = Query(...), tz: str = Query(DEFAULT_TZ),
    date_field: str = Query(DATE_FIELD_DEFAULT),
    states: Optional[str] = Query(None), exclude_states: Optional[str] = Query(None),
    use_bd: Optional[bool] = Query(None), business_day_start: Optional[str] = Query(None),
    limit: int = Query(0, description="0 = без ограничения"),
    order: str = Query("asc", pattern="^(asc|desc)$"),
    grouped: int = Query(0),
    with_items: int = Query(1, description="1=обогащение первой позицией"),
    enrich_scope: str = Query("last_day", pattern="^(none|last_day|all)$"),
    assign_mode: str = Query("smart", pattern="^(smart|business|raw)$"),
    store_accept_until: Optional[str] = Query(None),
):
    tzinfo = tzinfo_of(tz)
    global _EFF_USE_BD, _EFF_BDS
    eff_use_bd = USE_BUSINESS_DAY if use_bd is None else bool(use_bd)
    eff_bds    = BUSINESS_DAY_START if not business_day_start else business_day_start
    _EFF_USE_BD, _EFF_BDS = eff_use_bd, eff_bds

    start_dt = parse_date_local(start, tz)
    end_dt   = parse_date_local(end, tz) + timedelta(days=1) - timedelta(milliseconds=1)

    inc = parse_states_csv(states)
    exc = parse_states_csv(exclude_states) or set()

    days, _, _, _, _, out = _collect_range(
        start_dt, end_dt, tz, date_field, inc, exc,
        assign_mode=assign_mode, store_accept_until=(store_accept_until or STORE_ACCEPT_UNTIL)
    )

    # сортировка
    out.sort(key=lambda it: (str(it["op_day"]), str(it["date"])), reverse=(order == "desc"))
    if limit and limit > 0:
        out = out[:limit]

    # группировка по дню (для UI)
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

    # обогащение SKU/Title первой позиции (быстро)
    if with_items and out:
        enrich_day  = out[-1]["op_day"] if order == "asc" else out[0]["op_day"]
        targets     = _select_targets(out, enrich_day, enrich_scope, limit)
        days_span   = (end_dt.date() - start_dt.date()).days + 1
        total_t     = len(targets)
        # лёгкая растяжка таймаутов
        t_scale     = 1.0 + (0.6 if days_span >= 10 else 0.0) + (0.6 if total_t >= 600 else 0.0)

        sem = asyncio.Semaphore(max(1, ENRICH_CONCURRENCY))
        async def enrich(it):
            async with sem:
                extra = await _first_item_details(str(it["id"]), timeout_scale=t_scale)
                if extra:
                    it["sku"]   = extra.get("sku")
                    it["title"] = extra.get("title")
                await asyncio.sleep(0.02)

        await asyncio.gather(*(enrich(it) for it in targets))

    return {
        "items": out,
        "groups": groups,
        "period_total_count": len(out),
        "period_total_amount": round(sum(float(it.get("amount", 0) or 0) for it in out), 2),
        "currency": CURRENCY,
    }

@app.get("/orders/ids.csv", response_class=PlainTextResponse)
async def list_ids_csv(
    start: str = Query(...), end: str = Query(...),
    tz: str = Query(DEFAULT_TZ),
    date_field: str = Query(DATE_FIELD_DEFAULT),
    states: Optional[str] = Query(None), exclude_states: Optional[str] = Query(None),
    use_bd: Optional[bool] = Query(None), business_day_start: Optional[str] = Query(None),
    order: str = Query("asc", pattern="^(asc|desc)$"),
    assign_mode: str = Query("smart", pattern="^(smart|business|raw)$"),
    store_accept_until: Optional[str] = Query(None),
):
    data = await list_ids(start=start, end=end, tz=tz, date_field=date_field,
                          states=states, exclude_states=exclude_states,
                          use_bd=use_bd, business_day_start=business_day_start,
                          limit=100000, order=order, grouped=0, with_items=0,
                          enrich_scope="none", assign_mode=assign_mode,
                          store_accept_until=store_accept_until)
    return "\n".join([str(it["number"]) for it in data["items"]])

# ---------- root ----------
@app.get("/", include_in_schema=False)
async def root():
    return RedirectResponse(url="/ui/")
