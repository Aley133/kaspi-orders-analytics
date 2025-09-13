# app/main.py
from __future__ import annotations

# ---------- imports ----------
import os
import re
import asyncio
from datetime import datetime, timedelta, time, date
from typing import Optional, Dict, List, Iterable, Tuple

import httpx
import pytz
from dotenv import load_dotenv
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse, PlainTextResponse
from pydantic import BaseModel
from cachetools import TTLCache
from httpx import HTTPStatusError, RequestError

# FIFO/Bridge
from app.api.bridge_v2 import router as bridge_v2_router
from app.api.profit_fifo import get_profit_fifo_router

# Роутер и ХЕЛПЕРЫ ИМПОРТИРУЕМ ИЗ debug_sku.PY
from app.debug_sku import (
    get_debug_router,
    _index_included,
    _extract_entry,
    title_candidates,
    _rel_id,
    sku_candidates,
)

# KaspiClient
try:
    from app.kaspi_client import KaspiClient  # type: ignore
except Exception:
    try:
        from .kaspi_client import KaspiClient  # type: ignore
    except Exception:
        from kaspi_client import KaspiClient  # type: ignore

# Products router
try:
    from app.api.products import get_products_router
except Exception as _e1:
    try:
        from .api.products import get_products_router
    except Exception as _e2:
        import traceback
        print("Failed to import app.api.products:", repr(_e1), "| secondary:", repr(_e2))
        traceback.print_exc()
        raise

# ---------- ENV ----------
load_dotenv()

KASPI_TOKEN = os.getenv("KASPI_TOKEN", "").strip()
DEFAULT_TZ = os.getenv("TZ", "Asia/Almaty")
KASPI_BASE_URL = os.getenv("KASPI_BASE_URL", "https://kaspi.kz/shop/api/v2").rstrip("/")
CURRENCY = os.getenv("CURRENCY", "KZT")

AMOUNT_FIELDS = [s.strip() for s in os.getenv("AMOUNT_FIELDS", "totalPrice").split(",") if s.strip()]
AMOUNT_DIVISOR = float(os.getenv("AMOUNT_DIVISOR", "1") or 1)

DATE_FIELD_DEFAULT = os.getenv("DATE_FIELD_DEFAULT", "creationDate")
DATE_FIELD_OPTIONS = [s.strip() for s in os.getenv(
    "DATE_FIELD_OPTIONS", "creationDate,plannedShipmentDate,shipmentDate,deliveryDate"
).split(",") if s.strip()]
CITY_KEYS = [s.strip() for s in os.getenv("CITY_KEYS", "city,deliveryAddress.city").split(",") if s.strip()]

CHUNK_DAYS = int(os.getenv("CHUNK_DAYS", "7") or 7)
CACHE_TTL = int(os.getenv("CACHE_TTL", "300") or 300)

SHOP_NAME = os.getenv("SHOP_NAME", "LeoXpress")
PARTNER_ID = os.getenv("PARTNER_ID", "")

# Cutoff магазина (для smart)
STORE_ACCEPT_UNTIL = os.getenv("STORE_ACCEPT_UNTIL", "17:00")

# Бизнес-день
BUSINESS_DAY_START = os.getenv("BUSINESS_DAY_START", "20:00")   # HH:MM
USE_BUSINESS_DAY = os.getenv("USE_BUSINESS_DAY", "true").lower() in ("1", "true", "yes", "on")

# runtime flags (эффективные параметры бакетизации)
_EFF_USE_BD: bool = USE_BUSINESS_DAY
_EFF_BDS: str = BUSINESS_DAY_START

# ---------- HTTPX ----------
HTTPX_TIMEOUT = httpx.Timeout(connect=10.0, read=70.0, write=15.0, pool=60.0)
HTTPX_LIMITS = httpx.Limits(max_connections=20, max_keepalive_connections=10)

def _kaspi_headers() -> Dict[str, str]:
    if not KASPI_TOKEN:
        raise HTTPException(status_code=500, detail="KASPI_TOKEN is not set")
    return {
        "X-Auth-Token": KASPI_TOKEN,
        "Accept": "application/vnd.api+json",
        "Content-Type": "application/vnd.api+json",
        "User-Agent": "Mozilla/5.0",
    }

def _async_client():
    return httpx.AsyncClient(base_url=KASPI_BASE_URL, timeout=HTTPX_TIMEOUT, limits=HTTPX_LIMITS)

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

client = KaspiClient(token=KASPI_TOKEN, base_url=KASPI_BASE_URL) if KASPI_TOKEN else None
orders_cache = TTLCache(maxsize=128, ttl=CACHE_TTL)

# роутеры
app.include_router(get_products_router(client), prefix="/products")
app.include_router(get_profit_fifo_router(),   prefix="/profit")
app.include_router(bridge_v2_router,           prefix="/profit")
app.include_router(get_debug_router())

# ---------- utils ----------
def _bd_delta(hhmm: str) -> timedelta:
    try:
        hh, mm = hhmm.split(":")
        return timedelta(hours=int(hh), minutes=int(mm))
    except Exception:
        raise HTTPException(status_code=400, detail=f"Bad HH:MM time: {hhmm}")

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

def norm_state(s: str) -> str:
    return (s or "").strip().upper()

def parse_states_csv(s: Optional[str]) -> Optional[set[str]]:
    if not s:
        return None
    return {norm_state(x) for x in re.split(r"[\s,;]+", s) if x.strip()}

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
    v = attrs.get(field)
    if v is None:
        return None
    try:
        return int(v)
    except Exception:
        try:
            return int(datetime.fromisoformat(str(v).replace("Z", "+00:00")).timestamp() * 1000)
        except Exception:
            return None

def bucket_date(dt_local: datetime) -> str:
    if _EFF_USE_BD:
        shift = timedelta(hours=24) - _bd_delta(_EFF_BDS)
        return (dt_local + shift).date().isoformat()
    return dt_local.date().isoformat()

def _guess_number(attrs: dict, fallback_id: str) -> str:
    for k in ("number", "code", "orderNumber"):
        v = attrs.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return str(fallback_id)

# ---------- SMART operational day ----------
_DELIVERED_STATES = {"KASPI_DELIVERY", "DELIVERED", "ARCHIVE", "ARCHIVED"}

def _parse_hhmm_to_time(hhmm: str) -> time:
    hh, mm = map(int, hhmm.split(":"))
    return time(hh, mm, 0)

def _smart_operational_day(attrs: dict, state: str, tzinfo: pytz.BaseTzInfo,
                           store_accept_until: str, business_day_start: str) -> Tuple[str, str]:
    ms_creation = extract_ms(attrs, "creationDate")
    ms_planned  = extract_ms(attrs, "plannedShipmentDate")
    ms_ship     = extract_ms(attrs, "shipmentDate")

    dt_creation = datetime.fromtimestamp(ms_creation/1000, tz=pytz.UTC).astimezone(tzinfo) if ms_creation else None
    dt_planned  = datetime.fromtimestamp(ms_planned/1000,  tz=pytz.UTC).astimezone(tzinfo) if ms_planned else None
    dt_ship     = datetime.fromtimestamp(ms_ship/1000,     tz=pytz.UTC).astimezone(tzinfo) if ms_ship else None

    # Доставочные статусы — бакет по бизнес-дню
    if state in _DELIVERED_STATES:
        base = dt_ship or dt_planned or dt_creation or datetime.now(tzinfo)
        shift = timedelta(hours=24) - _bd_delta(business_day_start)
        op = (base + shift).date().isoformat()
        return op, "delivered_business_day"

    # Есть план — берём плановую дату (календарь)
    if dt_planned:
        return dt_planned.date().isoformat(), "planned"

    # Cutoff магазина — до/после в день создания
    cutoff = _parse_hhmm_to_time(store_accept_until)
    if dt_creation:
        if dt_creation.time() <= cutoff:
            return dt_creation.date().isoformat(), "created_before_cutoff"
        else:
            return (dt_creation + timedelta(days=1)).date().isoformat(), "created_after_cutoff_next_day"

    return datetime.now(tzinfo).date().isoformat(), "fallback_now"

def _expand_states_with_archive(states_inc: Optional[set[str]]) -> set[str]:
    if not states_inc:
        return set()
    out = set(states_inc)
    if ("KASPI_DELIVERY" in out) or ("DELIVERED" in out):
        out |= {"ARCHIVE", "ARCHIVED"}
    return out

# ---------- Models ----------
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

# ---------- Core: scan range ----------
def _collect_range(
    start_dt: datetime, end_dt: datetime, tz: str, date_field: str,
    states_inc: Optional[set], states_ex: set,
    *, assign_mode: str = "smart", store_accept_until: str = STORE_ACCEPT_UNTIL,
    states_scope: str = "all",  # all | last_day
) -> tuple[list[DayPoint], Dict[str, int], int, float, Dict[str, int]]:
    tzinfo = tzinfo_of(tz)
    if client is None:
        raise HTTPException(status_code=500, detail="KASPI_TOKEN is not set")

    # эффективные флаги бакетизации
    range_start_day = start_dt.astimezone(tzinfo).date().isoformat()
    range_end_day   = end_dt.astimezone(tzinfo).date().isoformat()
    last_day = range_end_day

    # включающие + архивные алиасы (для доставки)
    states_inc = _expand_states_with_archive(states_inc) if states_inc else set()

    seen_ids: set[str] = set()
    day_counts: Dict[str, int] = {}
    day_amounts: Dict[str, float] = {}
    city_counts: Dict[str, int] = {}
    state_counts: Dict[str, int] = {}
    total_orders = 0
    total_amount = 0.0

    for s, e in iter_chunks(start_dt, end_dt, CHUNK_DAYS):
        try:
            try_field = date_field
            while True:
                try:
                    for order in client.iter_orders(start=s, end=e, filter_field=try_field):
                        oid = order.get("id")
                        if oid in seen_ids:
                            continue
                        attrs = order.get("attributes", {}) or {}
                        st = norm_state(attrs.get("state", ""))

                        # исключающие
                        if st in states_ex:
                            continue

                        # timestamp с fallback
                        field_for_ms = date_field if (attrs.get(date_field) not in (None, "", 0)) else "creationDate"
                        ms = extract_ms(attrs, field_for_ms)
                        if ms is None:
                            continue
                        dtt = datetime.fromtimestamp(ms / 1000.0, tz=pytz.UTC).astimezone(tzinfo)

                        # операционный день
                        if assign_mode == "smart":
                            op_day, _ = _smart_operational_day(attrs, st, tzinfo, store_accept_until, _EFF_BDS)
                        elif assign_mode == "business":
                            op_day, _ = bucket_date(dtt), "business"
                        else:
                            op_day, _ = dtt.date().isoformat(), "raw"

                        # отсечение по диапазону
                        if not (range_start_day <= op_day <= range_end_day):
                            continue

                        # включающие (all | last_day)
                        if states_inc:
                            if states_scope == "all":
                                if st not in states_inc:
                                    continue
                            else:  # last_day
                                if (op_day == last_day) and (st not in states_inc):
                                    continue
                                # на остальных днях допускаем любые стейты

                        amt = extract_amount(attrs)
                        city = extract_city(attrs)

                        day_counts[op_day] = day_counts.get(op_day, 0) + 1
                        day_amounts[op_day] = day_amounts.get(op_day, 0.0) + amt
                        if city:
                            city_counts[city] = city_counts.get(city, 0) + 1
                        state_counts[st] = state_counts.get(st, 0) + 1

                        total_orders += 1
                        total_amount += amt
                        seen_ids.add(oid)
                    break
                except HTTPStatusError as ee:
                    if ee.response.status_code in (400, 422) and try_field != "creationDate":
                        try_field = "creationDate"
                        continue
                    raise
        except RequestError as e:
            raise HTTPException(status_code=502, detail=f"Network: {e}")

    # уплотняем дни
    out_days: List[DayPoint] = []
    cur = start_dt.astimezone(tzinfo).date()
    end_d = end_dt.astimezone(tzinfo).date()
    while cur <= end_d:
        key = cur.isoformat()
        out_days.append(DayPoint(x=key, count=day_counts.get(key, 0), amount=round(day_amounts.get(key, 0.0), 2)))
        cur = cur + timedelta(days=1)

    return out_days, city_counts, total_orders, round(total_amount, 2), state_counts

# ---------- /orders/analytics ----------
@app.get("/orders/analytics", response_model=AnalyticsResponse)
async def analytics(
    start: str = Query(...),
    end: str = Query(...),
    tz: str = Query(DEFAULT_TZ),
    date_field: str = Query(DATE_FIELD_DEFAULT),
    states: Optional[str] = Query(None),
    exclude_states: Optional[str] = Query(None),
    with_prev: bool = Query(True),
    exclude_canceled: bool = Query(True),
    start_time: Optional[str] = Query(None),
    end_time: Optional[str] = Query(None),
    use_bd: Optional[bool] = Query(None),
    business_day_start: Optional[str] = Query(None),
    assign_mode: str = Query("smart", pattern="^(smart|business|raw)$"),
    store_accept_until: Optional[str] = Query(None),
    states_scope: str = Query("all", pattern="^(all|last_day)$"),
):
    tzinfo = tzinfo_of(tz)

    global _EFF_USE_BD, _EFF_BDS
    eff_use_bd = USE_BUSINESS_DAY if use_bd is None else bool(use_bd)
    eff_bds = BUSINESS_DAY_START if not business_day_start else business_day_start
    _EFF_USE_BD, _EFF_BDS = eff_use_bd, eff_bds

    # интервал
    start_dt = parse_date_local(start, tz)
    end_dt = parse_date_local(end, tz) + timedelta(days=1) - timedelta(milliseconds=1)

    # режимы времени
    if eff_use_bd:
        delta = _bd_delta(eff_bds)
        start_dt = tzinfo.localize(datetime.combine((start_dt.date() - timedelta(days=1)), time(0, 0, 0))) + delta
        end_dt   = tzinfo.localize(datetime.combine(end_dt.date(), time(0, 0, 0))) + delta - timedelta(milliseconds=1)
    else:
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

    days, cities_dict, tot, tot_amt, st_counts = _collect_range(
        start_dt, end_dt, tz, date_field, inc, exc,
        assign_mode=assign_mode,
        store_accept_until=(store_accept_until or STORE_ACCEPT_UNTIL),
        states_scope=states_scope,
    )

    cities_list = [{"city": c, "count": n} for c, n in sorted(cities_dict.items(), key=lambda x: -x[1])]

    prev_days: List[DayPoint] = []
    if with_prev:
        span_days = (end_dt.date() - start_dt.date()).days + 1
        prev_end = start_dt - timedelta(milliseconds=1)
        prev_start = prev_end - timedelta(days=span_days) + timedelta(milliseconds=1)
        prev_days, _, _, _, _ = _collect_range(
            prev_start, prev_end, tz, date_field, inc, exc,
            assign_mode=assign_mode,
            store_accept_until=(store_accept_until or STORE_ACCEPT_UNTIL),
            states_scope=states_scope,
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

# ---------- Enrichment (первая позиция) ----------
async def _first_item_details(order_id: str, return_candidates: bool = False) -> Optional[Dict[str, object]]:
    headers = _kaspi_headers()
    async with _async_client() as cli:
        # S2 — /orderentries (дёшево и стабильно)
        try:
            r = await cli.get("/orderentries", params={"filter[order.id]": order_id, "page[size]": "200"}, headers=headers)
            j = r.json()
            data = (j.get("data", []) if isinstance(j, dict) else []) or []
            if data:
                entry = data[0]
                ex = _extract_entry(entry, {})
                if ex:
                    attrs_e = entry.get("attributes", {}) or {}
                    titles = title_candidates(attrs_e)
                    # лучший title
                    best = None
                    for key in ("offer.name", "title", "name", "productName", "shortName"):
                        v = titles.get(key)
                        if isinstance(v, str) and v.strip():
                            best = v.strip(); break
                    # sku candidates
                    cand = sku_candidates(attrs_e)
                    off = attrs_e.get("offer") or {}
                    if isinstance(off, dict) and off.get("code"):
                        cand["offer.code"] = str(off["code"])
                    sku_val = None
                    for k in ("offer.code", "merchantProduct.code", "product.code", "code", "sku"):
                        vv = cand.get(k)
                        if isinstance(vv, str) and vv.strip():
                            sku_val = vv.strip(); break
                    if not sku_val:
                        sku_val = str(ex.get("sku", ""))
                    out = {"sku": sku_val, "title": (best or "")}
                    if return_candidates:
                        cand["extracted"] = str(ex.get("sku", ""))
                        out["sku_candidates"] = cand
                        out["title_candidates"] = titles
                    return out
        except Exception:
            pass

        # S1 — /orders/{id}/entries?include=...
        try:
            r = await cli.get(
                f"/orders/{order_id}/entries",
                params={"page[size]": "200", "include": "product,merchantProduct,masterProduct"},
                headers=headers,
            )
            j = r.json()
            included = _index_included(j.get("included", []) if isinstance(j, dict) else [])
            data = (j.get("data", []) if isinstance(j, dict) else []) or []
            if data:
                entry = data[0]
                ex = _extract_entry(entry, included)
                if ex:
                    attrs_e = entry.get("attributes", {}) or {}
                    titles = title_candidates(attrs_e)
                    for rel_key in ("product", "merchantProduct", "masterProduct"):
                        t, rel_id = _rel_id(entry, rel_key)
                        if t and rel_id:
                            ref = (included.get((str(t), str(rel_id))) or {})
                            ref_attrs = ref.get("attributes", {}) or {}
                            for k in ("title", "name", "productName", "shortName"):
                                v = ref_attrs.get(k)
                                if isinstance(v, str) and v.strip():
                                    titles[f"{rel_key}.{k}"] = v.strip()
                    best = None
                    for key in ("offer.name", "product.title", "product.productName", "title", "name"):
                        v = titles.get(key)
                        if isinstance(v, str) and v.strip():
                            best = v.strip(); break
                    cand = sku_candidates(attrs_e)
                    off = attrs_e.get("offer") or {}
                    if isinstance(off, dict) and off.get("code"):
                        cand["offer.code"] = str(off["code"])
                    for rel_key in ("product", "merchantProduct", "masterProduct"):
                        t, rel_id = _rel_id(entry, rel_key)
                        if t and rel_id:
                            ref = (included.get((str(t), str(rel_id))) or {})
                            ref_attrs = ref.get("attributes", {}) or {}
                            if "code" in ref_attrs and ref_attrs["code"]:
                                cand[f"{rel_key}.code"] = str(ref_attrs["code"])
                    sku_val = None
                    for k in ("offer.code", "merchantProduct.code", "product.code", "code", "sku"):
                        vv = cand.get(k)
                        if isinstance(vv, str) and vv.strip():
                            sku_val = vv.strip(); break
                    if not sku_val:
                        sku_val = str(ex.get("sku", ""))
                    out = {"sku": sku_val, "title": (best or "")}
                    if return_candidates:
                        cand["extracted"] = str(ex.get("sku", ""))
                        out["sku_candidates"] = cand
                        out["title_candidates"] = titles
                    return out
        except Exception:
            pass
    return None

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
    limit: int = Query(1000),
    order: str = Query("asc", pattern="^(asc|desc)$"),
    grouped: int = Query(0),
    with_items: int = Query(1, description="1=подтянуть sku+title первой позиции"),
    enrich_scope: str = Query("last_day", pattern="^(none|last_day|all)$"),
    return_candidates: int = Query(0, description="1=вернуть title_candidates/sku_candidates для 1-й позиции"),
    assign_mode: str = Query("smart", pattern="^(smart|business|raw)$"),
    store_accept_until: Optional[str] = Query(None),
    states_scope: str = Query("all", pattern="^(all|last_day)$"),
):
    tzinfo = tzinfo_of(tz)

    global _EFF_USE_BD, _EFF_BDS
    eff_use_bd = USE_BUSINESS_DAY if use_bd is None else bool(use_bd)
    eff_bds = BUSINESS_DAY_START if not business_day_start else business_day_start
    _EFF_USE_BD, _EFF_BDS = eff_use_bd, eff_bds

    start_dt = parse_date_local(start, tz)
    end_dt = parse_date_local(end, tz) + timedelta(days=1) - timedelta(milliseconds=1)
    if eff_use_bd:
        delta = _bd_delta(eff_bds)
        start_dt = tzinfo.localize(datetime.combine((start_dt.date() - timedelta(days=1)), time(0, 0, 0))) + delta
        end_dt   = tzinfo.localize(datetime.combine(end_dt.date(), time(0, 0, 0))) + delta - timedelta(milliseconds=1)

    range_start_day = start_dt.astimezone(tzinfo).date().isoformat()
    range_end_day   = end_dt.astimezone(tzinfo).date().isoformat()
    enrich_day = range_end_day  # last op day for enrichment filter

    inc = parse_states_csv(states)
    exc = parse_states_csv(exclude_states) or set()
    inc = _expand_states_with_archive(inc) if inc else set()

    out: List[Dict[str, object]] = []

    if client is None:
        raise HTTPException(status_code=500, detail="KASPI_TOKEN is not set")

    for s, e in iter_chunks(start_dt, end_dt, CHUNK_DAYS):
        try:
            try_field = date_field
            while True:
                try:
                    for order in client.iter_orders(start=s, end=e, filter_field=try_field):
                        attrs = order.get("attributes", {}) or {}
                        st = norm_state(attrs.get("state", ""))

                        if st in exc:
                            continue

                        field_for_ms = date_field if (attrs.get(date_field) not in (None, "", 0)) else "creationDate"
                        ms = extract_ms(attrs, field_for_ms)
                        if ms is None:
                            continue
                        dtt = datetime.fromtimestamp(ms / 1000.0, tz=pytz.UTC).astimezone(tzinfo)

                        # op_day теми же правилами
                        if assign_mode == "smart":
                            op_day, _ = _smart_operational_day(attrs, st, tzinfo, (store_accept_until or STORE_ACCEPT_UNTIL), _EFF_BDS)
                        elif assign_mode == "business":
                            op_day, _ = bucket_date(dtt), "business"
                        else:
                            op_day, _ = dtt.date().isoformat(), "raw"

                        if not (range_start_day <= op_day <= range_end_day):
                            continue

                        # включающие
                        if inc:
                            if states_scope == "all":
                                if st not in inc:
                                    continue
                            else:  # last_day
                                if (op_day == enrich_day) and (st not in inc):
                                    continue

                        out.append({
                            "id": order.get("id"),
                            "number": _guess_number(attrs, order.get("id")),
                            "state": st,
                            "date": dtt.isoformat(),
                            "op_day": op_day,
                            "amount": round(extract_amount(attrs), 2),
                            "city": extract_city(attrs),
                        })
                    break
                except HTTPStatusError as ee:
                    if ee.response.status_code in (400, 422) and try_field != "creationDate":
                        try_field = "creationDate"
                        continue
                    raise
        except RequestError as e:
            raise HTTPException(status_code=502, detail=f"Network: {e}")

    # сортировка по (op_day, date)
    out.sort(key=lambda it: (str(it["op_day"]), str(it["date"])), reverse=(order == "desc"))

    # группировки
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

    # обогащение SKU/TITLE только target-подмножества (бережём API)
    if with_items and out and enrich_scope != "none":
        if enrich_scope == "all":
            targets = out[: (limit or len(out))]
        else:  # last_day
            targets = [it for it in out if str(it["op_day"]) == enrich_day]

        sem = asyncio.Semaphore(6)
        async def enrich(it):
            async with sem:
                extra = await _first_item_details(str(it["id"]), return_candidates=bool(return_candidates))
                if extra:
                    it["sku"] = extra.get("sku")
                    it["title"] = extra.get("title")
                    if return_candidates:
                        it["first_item"] = {
                            "title_candidates": extra.get("title_candidates") or {},
                            "sku_candidates": extra.get("sku_candidates") or {},
                        }
        await asyncio.gather(*(enrich(it) for it in targets))

    period_total_amount = round(sum(float(it.get("amount", 0) or 0) for it in out), 2)
    period_total_count = len(out)

    if limit and limit > 0:
        out = out[:limit]

    return {
        "items": out,
        "groups": groups,
        "period_total_count": period_total_count,
        "period_total_amount": period_total_amount,
        "currency": CURRENCY,
    }

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
    states_scope: str = Query("all", pattern="^(all|last_day)$"),
):
    data = await list_ids(
        start=start, end=end, tz=tz, date_field=date_field,
        states=states, exclude_states=exclude_states,
        use_bd=use_bd, business_day_start=business_day_start,
        limit=100000, order=order, grouped=0,
        with_items=0, enrich_scope="none", return_candidates=0,
        assign_mode=assign_mode, store_accept_until=store_accept_until,
        states_scope=states_scope,
    )
    csv = "\n".join([str(it["number"]) for it in data["items"]])
    return csv

# ---------- ROOT/UI ----------
@app.get("/", include_in_schema=False)
async def root():
    return RedirectResponse(url="/ui/")

app.mount("/ui", StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static"), html=True), name="static")
