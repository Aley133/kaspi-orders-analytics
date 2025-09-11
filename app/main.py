# app/main.py
from __future__ import annotations

import os
import re
import uuid
import random
from datetime import datetime, timedelta, time, date
from typing import Optional, Dict, List, Iterable, Tuple, Callable

import asyncio
import httpx
import pytz
from dotenv import load_dotenv
from fastapi import FastAPI, Query, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse, PlainTextResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pathlib import Path
from pydantic import BaseModel
from cachetools import TTLCache
from httpx import HTTPStatusError, RequestError

# ---------- Корректные импорты из app.deps ----------
# (в старом деплое падало из-за 'from deps...')
try:
    from app.deps.auth import get_current_kaspi_token, attach_kaspi_token_middleware
except Exception as _e:
    # Если структура иная, можно добавить фолбэк, но рекомендуем держать именно app.deps.*
    raise

# ---------- Роутеры доменных модулей ----------
from app.api.bridge_v2 import router as bridge_router
from app.api.profit_fifo import get_profit_fifo_router
from app.api.authz import router as auth_router

# (опционально, если есть отдельный settings.py с /settings/*)
try:
    from app.api.settings import router as settings_router
except Exception:
    settings_router = None

# ---------- Хелперы из debug_sku ----------
from app.debug_sku import (
    get_debug_router,
    _index_included,
    _extract_entry,
    title_candidates as _title_candidates_from_attrs,
    _rel_id,
    sku_candidates as _sku_candidates_from_attrs,
)

# ---------- KaspiClient: сначала пробуем многопользовательский из app/deps ----------
TenantKaspiClient = None
try:
    # Вариант 1: ты переименовал его для ясности
    from app.deps.kaspi_client_tenant import KaspiClient as TenantKaspiClient  # наш tenant-aware клиент
except Exception:
    try:
        # Вариант 2: остался как app/deps/kaspi_client.py
        from app.deps.kaspi_client import KaspiClient as TenantKaspiClient      # наш tenant-aware клиент
    except Exception:
        TenantKaspiClient = None

# Фолбэк на стоковый клиент (НЕ трогаем файл)
StockKaspiClient = None
if TenantKaspiClient is None:
    try:
        from app.kaspi_client import KaspiClient as StockKaspiClient
    except Exception:
        try:
            from .kaspi_client import KaspiClient as StockKaspiClient
        except Exception:
            try:
                from kaspi_client import KaspiClient as StockKaspiClient
            except Exception:
                StockKaspiClient = None

load_dotenv()

# -------------------- ENV --------------------
KASPI_TOKEN = os.getenv("KASPI_TOKEN", "").strip()
DEFAULT_TZ = os.getenv("TZ", "Asia/Almaty")
KASPI_BASE_URL = os.getenv("KASPI_BASE_URL", "https://kaspi.kz/shop/api/v2").rstrip("/")
CURRENCY = os.getenv("CURRENCY", "KZT")

AMOUNT_FIELDS = [s.strip() for s in os.getenv("AMOUNT_FIELDS", "totalPrice").split(",") if s.strip()]
AMOUNT_DIVISOR = float(os.getenv("AMOUNT_DIVISOR", "1") or 1)

DATE_FIELD_DEFAULT = os.getenv("DATE_FIELD_DEFAULT", "creationDate")
DATE_FIELD_OPTIONS = [s.strip() for s in os.getenv(
    "DATE_FIELD_OPTIONS",
    "creationDate,plannedShipmentDate,shipmentDate,deliveryDate"
).split(",") if s.strip()]
CITY_KEYS = [s.strip() for s in os.getenv("CITY_KEYS", "city,deliveryAddress.city").split(",") if s.strip()]

CHUNK_DAYS = int(os.getenv("CHUNK_DAYS", "7") or 7)
CACHE_TTL = int(os.getenv("CACHE_TTL", "300") or 300)

SHOP_NAME = os.getenv("SHOP_NAME", "LeoXpress")
PARTNER_ID = os.getenv("PARTNER_ID", "")

# --- Business day defaults ---
BUSINESS_DAY_START = os.getenv("BUSINESS_DAY_START", "20:00")  # HH:MM
USE_BUSINESS_DAY = os.getenv("USE_BUSINESS_DAY", "true").lower() in ("1", "true", "yes", "on")

# Cutoff приёма заказов магазином (после него незакомплектованные уедут на завтра)
STORE_ACCEPT_UNTIL = os.getenv("STORE_ACCEPT_UNTIL", "17:00")  # HH:MM

# Параллельность обогащения SKU (базовая)
ENRICH_CONCURRENCY = int(os.getenv("ENRICH_CONCURRENCY", "8") or 8)

# По умолчанию включаем доставленные + архивные
DEFAULT_INCLUDE_STATES: set[str] = {"KASPI_DELIVERY", "ARCHIVE", "ARCHIVED"}

# -------------------- утилиты --------------------
def _bd_delta(hhmm: str) -> timedelta:
    try:
        hh, mm = hhmm.split(":")
        return timedelta(hours=int(hh), minutes=int(mm))
    except Exception:
        raise HTTPException(status_code=400, detail=f"Неверный BUSINESS_DAY_START: {hhmm}")

def _parse_hhmm_to_time(hhmm: str) -> time:
    try:
        hh, mm = map(int, hhmm.split(":"))
        return time(hh, mm, 0)
    except Exception:
        raise HTTPException(status_code=400, detail=f"Bad HH:MM time: {hhmm}")

def _days_between(a: datetime, b: datetime) -> int:
    return max(1, (b.date() - a.date()).days + 1)

# runtime flags
_EFF_USE_BD: bool = USE_BUSINESS_DAY
_EFF_BDS: str = BUSINESS_DAY_START

# -------------------- FastAPI --------------------
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

# middleware, который кладёт токен/тенант в контекст (для per-tenant работы)
app.middleware("http")(attach_kaspi_token_middleware)

# Инициализация kaspi-клиента:
# - если есть наш tenant-aware клиент, используем его (игнорирует глобальный токен)
# - иначе — стоковый, но только если задан глобальный KASPI_TOKEN
if TenantKaspiClient is not None:
    client = TenantKaspiClient(base_url=KASPI_BASE_URL)
else:
    client = (StockKaspiClient(token=KASPI_TOKEN, base_url=KASPI_BASE_URL)  # type: ignore
              if (StockKaspiClient is not None and KASPI_TOKEN) else None)

# Кэш ответов (включая entries по заказам)
orders_cache = TTLCache(maxsize=512, ttl=CACHE_TTL)

# Статический UI (устойчивый поиск директории)
_ui_candidates = ("app/static", "app/ui", "static", "ui")
_ui_dir = next((p for p in _ui_candidates if Path(p).is_dir()), None)
if _ui_dir:
    app.mount("/ui", StaticFiles(directory=_ui_dir, html=True), name="ui")
else:
    print("⚠️  UI directory not found, skipping /ui mount")

# Подключаем доменные роутеры
app.include_router(get_products_router(client), prefix="/products")
app.include_router(get_profit_fifo_router(), prefix="/profit")
app.include_router(get_debug_router())
app.include_router(bridge_router, prefix="/profit")
app.include_router(auth_router)
if settings_router:
    app.include_router(settings_router, prefix="/settings", tags=["settings"])

# -------------------- helpers --------------------
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

# -------------------- HTTPX и ретраи --------------------
BASE_TIMEOUT = httpx.Timeout(connect=10.0, read=80.0, write=20.0, pool=60.0)
HTTPX_LIMITS = httpx.Limits(max_connections=20, max_keepalive_connections=10)

def _kaspi_headers() -> Dict[str, str]:
    tok = get_current_kaspi_token()
    if not tok:
        # отсутствие персонального токена для текущего аккаунта
        raise HTTPException(status_code=401, detail="Kaspi token is not set for this tenant")
    return {
        "X-Auth-Token": tok,
        "Accept": "application/vnd.api+json",
        "Content-Type": "application/vnd.api+json",
        "User-Agent": "Mozilla/5.0",
    }

def _scaled_timeout(scale: float) -> httpx.Timeout:
    scale = max(1.0, float(scale))
    return httpx.Timeout(
        connect=BASE_TIMEOUT.connect,
        read=min(420.0, BASE_TIMEOUT.read * scale),
        write=min(150.0, BASE_TIMEOUT.write * scale),
        pool=BASE_TIMEOUT.pool
    )

def _async_client(scale: float = 1.0):
    return httpx.AsyncClient(base_url=KASPI_BASE_URL, timeout=_scaled_timeout(scale), limits=HTTPX_LIMITS)

async def _get_json_with_retries(cli: httpx.AsyncClient, url: str, *, params: Dict[str, str], headers: Dict[str, str], attempts: int = 5):
    for i in range(attempts):
        try:
            r = await cli.get(url, params=params, headers=headers)
            if r.status_code in (429, 500, 502, 503, 504):
                ra = r.headers.get("Retry-After")
                if ra:
                    try:
                        await asyncio.sleep(float(ra))
                    except Exception:
                        pass
                raise HTTPStatusError(f"Retryable status: {r.status_code}", request=r.request, response=r)
            r.raise_for_status()
            return r.json()
        except (HTTPStatusError, RequestError):
            if i == attempts - 1:
                raise
            backoff = min(0.9 * (2 ** i), 16.0) + random.uniform(0.0, 0.4)
            await asyncio.sleep(backoff)

# -------------------- «Умное» назначение операционного дня --------------------
_DELIVERED_STATES = {"KASPI_DELIVERY", "ARCHIVE", "ARCHIVED"}

def _smart_operational_day(attrs: dict, state: str, tzinfo: pytz.BaseTzInfo,
                           store_accept_until: str, business_day_start: str) -> Tuple[str, str]:
    ms_creation = extract_ms(attrs, "creationDate")
    ms_planned  = extract_ms(attrs, "plannedShipmentDate")
    ms_ship     = extract_ms(attrs, "shipmentDate")

    dt_creation = datetime.fromtimestamp(ms_creation/1000, tz=pytz.UTC).astimezone(tzinfo) if ms_creation else None
    dt_planned  = datetime.fromtimestamp(ms_planned/1000,  tz=pytz.UTC).astimezone(tzinfo) if ms_planned else None
    dt_ship     = datetime.fromtimestamp(ms_ship/1000,     tz=pytz.UTC).astimezone(tzinfo) if ms_ship else None

    if state in _DELIVERED_STATES:
        base = dt_ship or dt_planned or dt_creation or datetime.now(tzinfo)
        shift = timedelta(hours=24) - _bd_delta(business_day_start)
        op = (base + shift).date().isoformat()
        return op, "delivered_business_day"

    if dt_planned:
        return dt_planned.date().isoformat(), "planned"

    cutoff = _parse_hhmm_to_time(store_accept_until)
    if dt_creation:
        if dt_creation.time() <= cutoff:
            return dt_creation.date().isoformat(), "created_before_cutoff"
        else:
            return (dt_creation + timedelta(days=1)).date().isoformat(), "created_after_cutoff_next_day"

    return datetime.now(tzinfo).date().isoformat(), "fallback_now"

# -------------------- Вытягивание позиций (ВСЕ SKU) --------------------
async def _all_items_details(order_id: str, return_candidates: bool = True, timeout_scale: float = 1.0) -> List[Dict[str, object]]:
    cache_key = f"entries:{order_id}"
    cached = orders_cache.get(cache_key)
    if cached is not None:
        return [dict(x) for x in cached]

    items: List[Dict[str, object]] = []
    async with _async_client(scale=timeout_scale) as cli:
        j = await _get_json_with_retries(
            cli, f"/orders/{order_id}/entries",
            params={"page[size]": "200", "include": "product,merchantProduct,masterProduct"},
            headers=_kaspi_headers(),
            attempts=5 + int(max(0, timeout_scale - 1.0) * 2),
        )
        data = j.get("data", []) or []
        included = j.get("included", []) or []
        idx = _index_included(included)

        for entry in data:
            attrs = entry.get("attributes", {}) or {}
            ex = _extract_entry(entry, idx)
            if not ex:
                continue

            titles = _title_candidates_from_attrs(attrs)
            for rel_key in ("product", "merchantProduct", "masterProduct"):
                t, rel_id = _rel_id(entry, rel_key)
                if t and rel_id:
                    ref = (idx.get((str(t), str(rel_id))) or {})
                    ref_attrs = ref.get("attributes", {}) or {}
                    for k in ("title", "name", "productName", "shortName"):
                        v = ref_attrs.get(k)
                        if isinstance(v, str) and v.strip():
                            titles[f"{rel_key}.{k}"] = v.strip()

            sku_cands = _sku_candidates_from_attrs(attrs)
            offer = attrs.get("offer") or {}
            if isinstance(offer, dict) and offer.get("code"):
                sku_cands["offer.code"] = str(offer["code"])
            for rel_key in ("product", "merchantProduct", "masterProduct"):
                t, rel_id = _rel_id(entry, rel_key)
                if t and rel_id:
                    ref = (idx.get((str(t), str(rel_id))) or {})
                    ref_attrs = ref.get("attributes", {}) or {}
                    if "code" in ref_attrs and ref_attrs["code"]:
                        sku_cands[f"{rel_key}.code"] = str(ref_attrs["code"])

            best_title = None
            for key in ("offer.name", "product.title", "merchantProduct.title", "title", "name", "productName"):
                v = titles.get(key)
                if isinstance(v, str) and v.strip():
                    best_title = v.strip()
                    break

            best_sku = None
            for k in ("offer.code", "merchantProduct.code", "product.code", "code", "sku"):
                vv = sku_cands.get(k)
                if isinstance(vv, str) and vv.strip():
                    best_sku = vv.strip()
                    break
            if not best_sku:
                best_sku = str(ex.get("sku", ""))

            row = {
                "sku": best_sku,
                "qty": int(ex.get("qty") or 1),
                "unit_price": float(ex.get("unit_price") or 0),
                "title": best_title,
                "raw_entry_id": entry.get("id"),
            }
            if return_candidates:
                sku_cands["extracted"] = str(ex.get("sku", ""))
                row["sku_candidates"] = sku_cands
                row["title_candidates"] = titles
            items.append(row)

    orders_cache[cache_key] = [dict(x) for x in items]
    return items

# -------------------- (совместимость) первая позиция --------------------
async def _first_item_details(order_id: str, return_candidates: bool = False, timeout_scale: float = 1.0) -> Optional[Dict[str, object]]:
    items = await _all_items_details(order_id, return_candidates=return_candidates, timeout_scale=timeout_scale)
    if not items:
        return None
    first = items[0]
    out = {"sku": first.get("sku"), "title": first.get("title")}
    if return_candidates:
        out["sku_candidates"] = first.get("sku_candidates", {})
        out["title_candidates"] = first.get("title_candidates", {})
    return out

# -------------------- Models --------------------
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

# -------------------- Вспомогательное расширение состояний --------------------
_ARCHIVE_ALIASES = {"ARCHIVE", "ARCHIVED"}

def _expand_with_archive(states_inc: Optional[set[str]]) -> set[str]:
    if not states_inc:
        return set(DEFAULT_INCLUDE_STATES)
    if "KASPI_DELIVERY" in states_inc:
        return set(states_inc) | _ARCHIVE_ALIASES
    return set(states_inc)

# -------------------- Прогресс-джобы --------------------
Jobs: Dict[str, Dict[str, object]] = {}  # job_id -> state

def _new_job() -> str:
    job_id = uuid.uuid4().hex
    Jobs[job_id] = {
        "status": "queued",         # queued | running | done | error | canceled
        "phase": "scan",            # scan | enrich
        "progress": 0.0,            # 0..1
        "message": "",
        "created": datetime.utcnow().isoformat() + "Z",
        "updated": datetime.utcnow().isoformat() + "Z",
        "total": 0,
        "done": 0,
        "result": None,
        "cancel": False,
    }
    return job_id

def _job_update(job_id: str, **patch):
    st = Jobs.get(job_id)
    if not st: return
    st.update(patch)
    st["updated"] = datetime.utcnow().isoformat() + "Z"

def _job_progress_cb(job_id: Optional[str]):
    if not job_id:
        return None
    def cb(phase: str, done: int, total: int, extra_msg: str = ""):
        if job_id not in Jobs:
            return
        if Jobs[job_id].get("cancel"):
            return
        prog = 0.0
        if total > 0:
            if phase == "scan":
                prog = min(0.6, 0.6 * (done / total))
            else:
                prog = 0.6 + min(0.4, 0.4 * (done / total))
        _job_update(job_id, phase=phase, progress=prog, done=done, total=total, message=extra_msg or Jobs[job_id].get("message",""))
    return cb

@app.get("/auth/meta", tags=["auth"])
def auth_meta():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_ANON_KEY")
    if not url or not key:
        raise HTTPException(status_code=500, detail="Missing SUPABASE_URL or SUPABASE_ANON_KEY")
    return {"SUPABASE_URL": url, "SUPABASE_ANON_KEY": key}

# -------------------- Endpoints: META --------------------
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

# -------------------- Внутреннее ядро сбора --------------------
def _calc_timeout_scale(days_span: int, targets: int) -> float:
    scale = 1.0
    if days_span >= 10:
        scale += 0.8
    if days_span >= 30:
        scale += 0.7
    if targets >= 300:
        scale += 0.6
    if targets >= 1000:
        scale += 0.8
    if targets >= 2000:
        scale += 0.6
    return min(5.0, scale)

def _calc_enrich_params(total_targets: int) -> Tuple[int, float]:
    if total_targets >= 2000:
        return max(2, ENRICH_CONCURRENCY // 3), 0.12
    if total_targets >= 1000:
        return max(2, ENRICH_CONCURRENCY // 2), 0.08
    if total_targets >= 400:
        return max(3, ENRICH_CONCURRENCY // 2 + 1), 0.055
    if total_targets >= 150:
        return max(4, ENRICH_CONCURRENCY), 0.035
    return ENRICH_CONCURRENCY, 0.02

async def _collect_range(
    start_dt: datetime, end_dt: datetime, tz: str, date_field: str,
    states_inc: Optional[set], states_ex: set,
    assign_mode: str, store_accept_until: str,
    progress: Optional[Callable[[str, int, int, str], None]] = None
) -> tuple[list[DayPoint], Dict[str, int], int, float, Dict[str, int], List[Dict[str, object]]]:
    tzinfo = tzinfo_of(tz)

    seen_ids: set[str] = set()
    day_counts: Dict[str, int] = {}
    day_amounts: Dict[str, float] = {}
    city_counts: Dict[str, int] = {}
    state_counts: Dict[str, int] = {}

    total_orders = 0
    total_amount = 0.0

    if client is None:
        raise HTTPException(status_code=500, detail="Kaspi client is not configured")

    states_inc = _expand_with_archive(states_inc)

    range_start_day = start_dt.astimezone(tzinfo).date().isoformat()
    range_end_day   = end_dt.astimezone(tzinfo).date().isoformat()

    chs = list(iter_chunks(start_dt, end_dt, CHUNK_DAYS))
    total_chunks = max(1, len(chs))
    cur_chunk = 0

    flat_out: List[Dict[str, object]] = []

    for s, e in chs:
        cur_chunk += 1
        try:
            try_field = date_field
            while True:
                try:
                    for order in client.iter_orders(start=s, end=e, filter_field=try_field):
                        oid = str(order.get("id"))
                        if oid in seen_ids:
                            continue
                        attrs = order.get("attributes", {}) or {}

                        st = norm_state(str(attrs.get("state", "")))
                        if states_inc and st not in states_inc:
                            continue
                        if st in states_ex:
                            continue

                        ms = extract_ms(attrs, date_field if date_field in attrs else try_field)
                        if ms is None:
                            continue
                        dtt = datetime.fromtimestamp(ms / 1000.0, tz=pytz.UTC).astimezone(tzinfo)

                        if assign_mode == "smart":
                            op_day, reason = _smart_operational_day(attrs, st, tzinfo, store_accept_until, _EFF_BDS)
                        elif assign_mode == "business":
                            op_day, reason = bucket_date(dtt), "business"
                        else:
                            op_day, reason = dtt.date().isoformat(), "raw"

                        if not (range_start_day <= op_day <= range_end_day):
                            continue

                        amt = extract_amount(attrs)
                        city = extract_city(attrs)

                        day_counts[op_day] = day_counts.get(op_day, 0) + 1
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
                            "date": dtt.isoformat(),
                            "op_day": op_day,
                            "op_reason": reason,
                            "amount": round(amt, 2),
                            "city": city,
                        })

                        seen_ids.add(oid)
                    break
                except HTTPStatusError as ee:
                    if ee.response.status_code in (400, 422) and try_field != "creationDate":
                        try_field = "creationDate"
                        continue
                    raise
        except RequestError as e:
            raise HTTPException(status_code=502, detail=f"Network: {e}")
        finally:
            if progress:
                progress("scan", cur_chunk, total_chunks, f"scan {cur_chunk}/{total_chunks}")

    out_days: List[DayPoint] = []
    cur = start_dt.astimezone(tzinfo).date()
    end_d = end_dt.astimezone(tzinfo).date()
    while cur <= end_d:
        key = cur.isoformat()
        out_days.append(DayPoint(x=key, count=day_counts.get(key, 0), amount=round(day_amounts.get(key, 0.0), 2)))
        cur = cur + timedelta(days=1)

    return out_days, city_counts, total_orders, round(total_amount, 2), state_counts, flat_out

# -------------------- Публичные эндпоинты аналитики --------------------
@app.get("/orders/analytics", response_model=AnalyticsResponse)
async def analytics(start: str = Query(...), end: str = Query(...), tz: str = Query(DEFAULT_TZ),
                    date_field: str = Query(DATE_FIELD_DEFAULT),
                    states: Optional[str] = Query(None), exclude_states: Optional[str] = Query(None),
                    with_prev: bool = Query(True), exclude_canceled: bool = Query(True),
                    start_time: Optional[str] = Query(None), end_time: Optional[str] = Query(None),
                    use_bd: Optional[bool] = Query(None), business_day_start: Optional[str] = Query(None),
                    assign_mode: str = Query("smart", pattern="^(smart|business|raw)$"),
                    store_accept_until: Optional[str] = Query(None)):

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
        end_dt = tzinfo.localize(datetime.combine(end_dt.date(), time(0, 0, 0))) + delta - timedelta(milliseconds=1)
    else:
        if start_time:
            start_dt = apply_hhmm(start_dt, start_time)
        if end_time:
            e0 = parse_date_local(end, tz)
            end_dt = apply_hhmm(e0, end_time)
            end_dt = end_dt.replace(tzinfo=tzinfo)

    if end_dt < start_dt:
        raise HTTPException(status_code=400, detail="end < start")

    inc = parse_states_csv(states)
    exc = parse_states_csv(exclude_states) or set()
    if exclude_canceled:
        exc |= {"CANCELED"}

    days, cities_dict, tot, tot_amt, st_counts, _ = await _collect_range(
        start_dt, end_dt, tz, date_field, inc, exc,
        assign_mode=assign_mode, store_accept_until=(store_accept_until or STORE_ACCEPT_UNTIL)
    )

    cities_list = [{"city": c, "count": n} for c, n in sorted(cities_dict.items(), key=lambda x: -x[1])]

    prev_days: List[DayPoint] = []
    if with_prev:
        span_days = (end_dt.date() - start_dt.date()).days + 1
        prev_end = start_dt - timedelta(milliseconds=1)
        prev_start = prev_end - timedelta(days=span_days) + timedelta(milliseconds=1)
        prev_days, _, _, _, _, _ = await _collect_range(
            prev_start, prev_end, tz, date_field, inc, exc,
            assign_mode=assign_mode, store_accept_until=(store_accept_until or STORE_ACCEPT_UNTIL)
        )

    return {
        "range": {
            "start": start_dt.astimezone(tzinfo).date().isoformat(),
            "end": end_dt.astimezone(tzinfo).date().isoformat(),
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

# -------------------- Вспомогательная «ядровая» функция list_ids --------------------
def _select_targets(out: List[Dict[str, object]], enrich_day: str, enrich_scope: str, limit: int) -> List[Dict[str, object]]:
    if enrich_scope == "all":
        return out
    if enrich_scope == "last_day":
        return [it for it in out if str(it["op_day"]) == enrich_day]
    if enrich_scope == "last_week":
        y, m, d = map(int, enrich_day.split("-"))
        last_dt = date(y, m, d)
        scope = {(last_dt - timedelta(days=i)).isoformat() for i in range(7)}
        return [it for it in out if str(it["op_day"]) in scope]
    if enrich_scope == "last_month":
        y, m, d = map(int, enrich_day.split("-"))
        last_dt = date(y, m, d)
        scope = {(last_dt - timedelta(days=i)).isoformat() for i in range(30)}
        return [it for it in out if str(it["op_day"]) in scope]
    return []

async def _list_ids_core(
    start: str, end: str, tz: str, date_field: str,
    states: Optional[str], exclude_states: Optional[str],
    use_bd: Optional[bool], business_day_start: Optional[str],
    limit: int, order: str, grouped: int,
    with_items: int, enrich_scope: str, items_mode: str, return_candidates: int,
    assign_mode: str, store_accept_until: Optional[str],
    progress_cb: Optional[Callable[[str, int, int, str], None]] = None,
) -> Dict[str, object]:

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
        end_dt = tzinfo.localize(datetime.combine(end_dt.date(), time(0, 0, 0))) + delta - timedelta(milliseconds=1)

    inc = parse_states_csv(states)
    exc = parse_states_csv(exclude_states) or set()
    inc = _expand_with_archive(inc)

    days, cities_dict, tot, tot_amt, st_counts, out = await _collect_range(
        start_dt, end_dt, tz, date_field, inc, exc,
        assign_mode=assign_mode, store_accept_until=(store_accept_until or STORE_ACCEPT_UNTИЛ),
        progress=progress_cb
    )

    out.sort(key=lambda it: (str(it["op_day"]), str(it["date"])), reverse=(order == "desc"))

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

    if with_items and out and enrich_scope != "none":
        enrich_day = bucket_date(end_dt.astimezone(tzinfo))
        targets = _select_targets(out, enrich_day, enrich_scope, limit)

        total_targets = len(targets)
        concurrency, per_sleep = _calc_enrich_params(total_targets)
        days_span = _days_between(start_dt, end_dt)
        t_scale = _calc_timeout_scale(days_span, total_targets)

        sem = asyncio.Semaphore(max(1, concurrency))
        done = 0
        if progress_cb:
            progress_cb("enrich", done, total_targets, "enrich start")

        async def enrich(it):
            nonlocal done
            async with sem:
                if items_mode == "all":
                    items = await _all_items_details(str(it["id"]), return_candidates=bool(return_candidates), timeout_scale=t_scale)
                    it["items"] = items
                    if items:
                        it["sku"] = items[0].get("sku")
                        it["title"] = items[0].get("title")
                else:
                    extra = await _first_item_details(str(it["id"]), return_candidates=bool(return_candidates), timeout_scale=t_scale)
                    if extra:
                        it["sku"] = extra.get("sku")
                        it["title"] = extra.get("title")
                        if return_candidates:
                            it["first_item"] = {
                                "title_candidates": extra.get("title_candidates") or {},
                                "sku_candidates": extra.get("sku_candidates") or {},
                            }
                done += 1
                if progress_cb:
                    progress_cb("enrich", done, total_targets, f"enrich {done}/{total_targets}")
                await asyncio.sleep(per_sleep)

        await asyncio.gather(*(enrich(it) for it in targets))

    if limit and limit > 0:
        out = out[:limit]

    period_total_amount = round(sum(float(it.get("amount", 0) or 0) for it in out), 2)
    period_total_count = len(out)

    return {
        "items": out,
        "groups": groups,
        "period_total_count": period_total_count,
        "period_total_amount": period_total_amount,
        "currency": CURRENCY,
    }

# -------------------- /orders/ids --------------------
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
    grouped: int = Query(0),
    with_items: int = Query(1, description="0=без обогащения; 1=обогащение позициями"),
    enrich_scope: str = Query("all", pattern="^(none|last_day|last_week|last_month|all)$"),
    items_mode: str = Query("all", pattern="^(first|all)$"),
    return_candidates: int = Query(0, description="1=вернуть title_candidates/sku_candidates"),
    assign_mode: str = Query("smart", pattern="^(smart|business|raw)$"),
    store_accept_until: Optional[str] = Query(None),
):
    return await _list_ids_core(
        start, end, tz, date_field, states, exclude_states,
        use_bd, business_day_start, limit, order, grouped, with_items,
        enrich_scope, items_mode, return_candidates, assign_mode, store_accept_until, None
    )

# -------------------- CSV --------------------
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
):
    data = await _list_ids_core(
        start, end, tz, date_field, states, exclude_states,
        use_bd, business_day_start,
        limit=100000, order=order, grouped=0,
        with_items=0, enrich_scope="none", items_mode="all", return_candidates=0,
        assign_mode=assign_mode, store_accept_until=store_accept_until, progress_cb=None
    )
    csv = "\n".join([str(it["number"]) for it in data["items"]])
    return csv

# -------------------- Async с прогрессом --------------------
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
    grouped: int = Query(0),
    with_items: int = Query(1),
    enrich_scope: str = Query("all", pattern="^(none|last_day|last_week|last_month|all)$"),
    items_mode: str = Query("all"),
    return_candidates: int = Query(0),
    assign_mode: str = Query("smart"),
    store_accept_until: Optional[str] = Query(None),
):
    job_id = _new_job()

    async def worker():
        try:
            _job_update(job_id, status="running", message="started")
            result = await _list_ids_core(
                start, end, tz, date_field, states, exclude_states,
                use_bd, business_day_start, limit, order, grouped,
                with_items, enrich_scope, items_mode, return_candidates,
                assign_mode, store_accept_until,
                progress_cb=_job_progress_cb(job_id)
            )
            if Jobs.get(job_id, {}).get("cancel"):
                _job_update(job_id, status="canceled", message="canceled by user", result=None)
            else:
                _job_update(job_id, status="done", progress=1.0, message="done", result=result)
        except Exception as e:
            _job_update(job_id, status="error", message=str(e))

    asyncio.create_task(worker())
    return {"job_id": job_id}

@app.get("/jobs/{job_id}")
async def job_status(job_id: str):
    st = Jobs.get(job_id)
    if not st:
        raise HTTPException(status_code=404, detail="job not found")
    payload = {k: v for k, v in st.items() if k != "result"}
    if st.get("status") == "done":
        payload["result_ready"] = True
    return JSONResponse(payload)

@app.get("/jobs/{job_id}/result")
async def job_result(job_id: str):
    st = Jobs.get(job_id)
    if not st:
        raise HTTPException(status_code=404, detail="job not found")
    if st.get("status") != "done":
        raise HTTPException(status_code=409, detail="job not finished")
    return JSONResponse(st.get("result") or {})

@app.delete("/jobs/{job_id}")
async def job_cancel(job_id: str):
    st = Jobs.get(job_id)
    if not st:
        raise HTTPException(status_code=404, detail="job not found")
    st["cancel"] = True
    return {"ok": True}

# -------------------- ROOT --------------------
@app.get("/", include_in_schema=False)
async def root():
    return RedirectResponse(url="/ui/")
