# app/api/bridge_v2.py
from __future__ import annotations

from typing import List, Dict, Any, Optional, Tuple, DefaultDict, Literal
from collections import defaultdict
from pydantic import BaseModel
from fastapi import APIRouter, Body, HTTPException, Query, Request
import os, sqlite3, httpx, math
from datetime import datetime

# Роутер — совпадает с вызовами на фронте
router = APIRouter(prefix="/profit/bridge")

# ─────────────────────────────────────────────────────────────────────
# Конфигурация
# ─────────────────────────────────────────────────────────────────────
# Фильтр по состоянию (например, "KASPI_DELIVERY"); если пусто — не фильтруем
BRIDGE_ONLY_STATE = (os.getenv("BRIDGE_ONLY_STATE") or "").strip() or None

# Ключ для охраны «изменяющих» эндпоинтов; если не задан — защита выключена
BRIDGE_API_KEY = (os.getenv("BRIDGE_API_KEY") or os.getenv("PROFIT_API_KEY") or "").strip()

# Fallback в Kaspi для строк без SKU
KASPI_FALLBACK_ENABLED = (os.getenv("KASPI_FALLBACK_ENABLED", "1").lower() in ("1", "true", "yes"))
KASPI_TOKEN   = os.getenv("KASPI_TOKEN", "").strip()
KASPI_BASEURL = (os.getenv("KASPI_BASE_URL") or "https://kaspi.kz/shop/api/v2").rstrip("/")

# Вспомогательный сервис «Номера заказов (для сверки)» — опционально
ORDERS_SERVICE_URL = (os.getenv("ORDERS_SERVICE_URL") or "http://127.0.0.1:8000").rstrip("/")

HTTPX_TIMEOUT = httpx.Timeout(connect=10.0, read=40.0, write=15.0, pool=40.0)
HTTPX_LIMITS  = httpx.Limits(max_connections=30, max_keepalive_connections=10)

# ─────────────────────────────────────────────────────────────────────
# База данных: SQLite по умолчанию с автопереключением
# ─────────────────────────────────────────────────────────────────────
PRI_DB_URL      = os.getenv("PROFIT_DB_URL") or os.getenv("DATABASE_URL") or "sqlite:///./profit.db"
FALLBACK_DB_URL = os.getenv("BRIDGE_FALLBACK_DB_URL", "sqlite:///./profit_bridge.db")

_ACTUAL_DB_URL = PRI_DB_URL
_FALLBACK_USED = False

def _driver_name() -> str:
    return "sqlite" if _ACTUAL_DB_URL.startswith("sqlite") else "pg"

def _sqlite_path(url: str) -> str:
    return url.split("sqlite:///")[-1]

def _get_conn():
    """Подключение к PG; если psycopg2 недоступен — мягко падаем в SQLite."""
    global _ACTUAL_DB_URL, _FALLBACK_USED
    if _ACTUAL_DB_URL.startswith("sqlite"):
        c = sqlite3.connect(_sqlite_path(_ACTUAL_DB_URL))
        c.row_factory = sqlite3.Row
        return c
    try:
        import psycopg2  # type: ignore
        return psycopg2.connect(_ACTUAL_DB_URL)
    except ModuleNotFoundError:
        _ACTUAL_DB_URL = FALLBACK_DB_URL
        _FALLBACK_USED = True
        c = sqlite3.connect(_sqlite_path(_ACTUAL_DB_URL))
        c.row_factory = sqlite3.Row
        return c

def _init_bridge_sales() -> None:
    """Плоская таблица позиций заказов (каждая позиция — отдельная строка)."""
    with _get_conn() as c:
        cur = c.cursor()
        if _driver_name() == "sqlite":
            cur.execute("""
            CREATE TABLE IF NOT EXISTS bridge_sales(
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              order_id   TEXT NOT NULL,
              line_index INTEGER NOT NULL,
              order_code TEXT,
              date_utc_ms INTEGER,
              state TEXT,
              sku TEXT NOT NULL,
              title TEXT,
              qty INTEGER NOT NULL,
              unit_price REAL NOT NULL,
              total_price REAL NOT NULL,
              UNIQUE(order_id, line_index)
            )""")
            cur.execute("CREATE INDEX IF NOT EXISTS ix_bridge_sales_date ON bridge_sales(date_utc_ms)")
            cur.execute("CREATE INDEX IF NOT EXISTS ix_bridge_sales_sku  ON bridge_sales(sku)")
            cur.execute("CREATE INDEX IF NOT EXISTS ix_bridge_sales_code ON bridge_sales(order_code)")
        else:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS bridge_sales(
              id SERIAL PRIMARY KEY,
              order_id   TEXT NOT NULL,
              line_index INTEGER NOT NULL,
              order_code TEXT,
              date_utc_ms BIGINT,
              state TEXT,
              sku TEXT NOT NULL,
              title TEXT,
              qty INTEGER NOT NULL,
              unit_price DOUBLE PRECISION NOT NULL,
              total_price DOUBLE PRECISION NOT NULL,
              CONSTRAINT bridge_sales_uniq UNIQUE(order_id, line_index)
            )""")
            cur.execute("CREATE INDEX IF NOT EXISTS ix_bridge_sales_date ON bridge_sales(date_utc_ms)")
            cur.execute("CREATE INDEX IF NOT EXISTS ix_bridge_sales_sku  ON bridge_sales(sku)")
            cur.execute("CREATE INDEX IF NOT EXISTS ix_bridge_sales_code ON bridge_sales(order_code)")
        c.commit()

def _chunked(items: List[Dict[str, Any]], n: int = 500):
    for i in range(0, len(items), n):
        yield items[i:i + n]

# ─────────────────────────────────────────────────────────────────────
# Вспомогательные функции
# ─────────────────────────────────────────────────────────────────────
def _canon_str(x: Optional[str], maxlen: int = 512) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    if not s:
        return None
    return s[:maxlen]

def _canon_sku(x: Optional[str]) -> Optional[str]:
    s = _canon_str(x, 128)
    if s is None:
        return None
    if s.endswith(".0") and s.replace(".", "", 1).isdigit():
        s = s[:-2]
    return s

def _to_ms(x) -> Optional[int]:
    if x is None:
        return None
    try:
        xi = int(x)
        return xi if xi > 10_000_000_000 else xi * 1000
    except Exception:
        try:
            return int(datetime.fromisoformat(str(x).replace("Z", "+00:00")).timestamp() * 1000)
        except Exception:
            return None

def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        if math.isfinite(v):
            return v
        return default
    except Exception:
        return default

def _line_total(qty: int, unit: float, total: Optional[float]) -> float:
    if total is not None and _safe_float(total, -1) >= 0:
        return float(total)
    return float(_safe_float(unit) * max(1, int(qty or 0)))

def _kaspi_headers() -> Dict[str, str]:
    if not KASPI_TOKEN:
        raise HTTPException(500, "KASPI_TOKEN is not set")
    return {
        "X-Auth-Token": KASPI_TOKEN,
        "Accept": "application/vnd.api+json",
        "Content-Type": "application/vnd.api+json",
    }

def _auth_ok(request: Request) -> bool:
    """API-ключ обязателен только если BRIDGE_API_KEY задан в окружении."""
    if not BRIDGE_API_KEY:
        return True
    key = request.headers.get("X-API-Key") or request.query_params.get("api_key")
    return isinstance(key, str) and key.strip() == BRIDGE_API_KEY

# ─────────────────────────────────────────────────────────────────────
# Fallback в Kaspi (для записей без SKU)
# ─────────────────────────────────────────────────────────────────────
_SKU_KEYS   = ("merchantProductCode", "article", "sku", "code", "productCode", "offerId", "vendorCode", "barcode", "skuId", "id", "merchantProductId")
_TITLE_KEYS = ("productName", "name", "title", "itemName", "productTitle", "merchantProductName")

def _index_included(inc_list):
    idx: Dict[Tuple[str, str], dict] = {}
    for it in inc_list or []:
        t, i = it.get("type"), it.get("id")
        if t and i:
            idx[(str(t), str(i))] = it
    return idx

def _rel_id(entry, rel):
    data = ((entry or {}).get("relationships", {}).get(rel, {}) or {}).get("data")
    if isinstance(data, dict):
        return data.get("type"), data.get("id")
    return None, None

def _extract_entry(entry, inc_index) -> Optional[Dict[str, Any]]:
    attrs = entry.get("attributes", {}) if "attributes" in (entry or {}) else (entry or {})
    qty = int(attrs.get("quantity") or 1)
    unit_price = _safe_float(attrs.get("unitPrice") or attrs.get("basePrice") or attrs.get("price"), 0.0)

    sku = ""
    for k in _SKU_KEYS:
        v = attrs.get(k)
        if v is not None and str(v).strip():
            sku = str(v).strip()
            break

    def from_rel(rel):
        t, i = _rel_id(entry, rel)
        if not (t and i):
            return None
        inc = inc_index.get((str(t), str(i))) or {}
        a = inc.get("attributes", {}) if isinstance(inc, dict) else {}
        if "master" in str(t).lower():
            return i or a.get("id") or a.get("code") or a.get("sku") or a.get("productCode")
        return a.get("code") or a.get("sku") or a.get("productCode") or i

    if not sku:
        sku = from_rel("product") or from_rel("merchantProduct") or from_rel("masterProduct") or ""

    _pt, pid = _rel_id(entry, "product")
    _mt, mid = _rel_id(entry, "merchantProduct")
    offer_like = attrs.get("offerId") or attrs.get("merchantProductId") or mid
    if (pid or mid) and offer_like and (not sku or str(offer_like) not in sku):
        sku = f"{(pid or mid)}_{offer_like}"

    if unit_price <= 0:
        total = _safe_float(attrs.get("totalPrice") or attrs.get("price"), 0.0)
        unit_price = round(total / max(1, qty), 4) if total else 0.0

    if not sku:
        return None

    titles: Dict[str, str] = {}
    for k in _TITLE_KEYS:
        v = attrs.get(k)
        if isinstance(v, str) and v.strip():
            titles[k] = v.strip()
    off = attrs.get("offer") or {}
    if isinstance(off, dict) and isinstance(off.get("name"), str):
        titles["offer.name"] = off["name"]

    for rel in ("product", "merchantProduct", "masterProduct"):
        t, i = _rel_id(entry, rel)
        if not (t and i):
            continue
        inc = inc_index.get((str(t), str(i))) or {}
        a = inc.get("attributes", {}) if isinstance(inc, dict) else {}
        for k in _TITLE_KEYS:
            v = a.get(k)
            if isinstance(v, str) and v.strip():
                titles[f"{rel}.{k}"] = v.strip()

    title = ""
    for key in ("offer.name", "name", "productName", "title", "productTitle"):
        if titles.get(key):
            title = titles[key]
            break
    if not title and titles:
        title = next(iter(titles.values()), "")
    total_price = round(unit_price * qty, 4)

    return {"sku": str(sku), "title": title, "qty": qty, "unit_price": unit_price, "total_price": total_price}

async def _fetch_entries_fallback(order_id: str) -> List[Dict[str, Any]]:
    if not (KASPI_FALLBACK_ENABLED and KASPI_TOKEN):
        return []
    async with httpx.AsyncClient(base_url=KASPI_BASEURL, timeout=HTTPX_TIMEOUT, limits=HTTPX_LIMITS) as cli:
        r = await cli.get(
            f"/orders/{order_id}/entries",
            params={"page[size]": "200", "include": "product,merchantProduct,masterProduct"},
            headers=_kaspi_headers(),
        )
        r.raise_for_status()
        j = r.json()
        inc = _index_included(j.get("included", []))
        out: List[Dict[str, Any]] = []
        for idx, e in enumerate(j.get("data", []) or []):
            ex = _extract_entry(e, inc)
            if ex:
                ex["__index"] = idx
                out.append(ex)
        return out

# ─────────────────────────────────────────────────────────────────────
# Модели входа
# ─────────────────────────────────────────────────────────────────────
class SyncItem(BaseModel):
    """Строка позиции из результата /orders/ids (для сверки)."""
    id: str
    code: Optional[str] = None  # номер заказа
    date: Optional[Any] = None
    state: Optional[str] = None
    sku: Optional[str] = None
    title: Optional[str] = None
    qty: Optional[int] = 1
    unit_price: Optional[float] = None
    total_price: Optional[float] = None
    amount: Optional[float] = None
    line_index: Optional[int] = None

# ─────────────────────────────────────────────────────────────────────
# Диагностика
# ─────────────────────────────────────────────────────────────────────
@router.get("/db/ping")
def db_ping():
    return {
        "ok": True,
        "driver": _driver_name(),
        "db_path": (_sqlite_path(_ACTUAL_DB_URL) if _driver_name() == "sqlite" else _ACTUAL_DB_URL),
        "fallback_used": _FALLBACK_USED,
        "fallback_enabled": bool(KASPI_FALLBACK_ENABLED),
        "only_state": BRIDGE_ONLY_STATE,
        "orders_service": ORDERS_SERVICE_URL,
        "auth_required": bool(BRIDGE_API_KEY),
    }

# ─────────────────────────────────────────────────────────────────────
# UPSERT строк в bridge_sales
# ─────────────────────────────────────────────────────────────────────
def _upsert_rows(rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    _init_bridge_sales()
    total = 0
    with _get_conn() as c:
        cur = c.cursor()
        if _driver_name() == "sqlite":
            sql = """
            INSERT INTO bridge_sales
              (order_id,line_index,order_code,date_utc_ms,state,sku,title,qty,unit_price,total_price)
            VALUES (:order_id,:line_index,:order_code,:date_utc_ms,:state,:sku,:title,:qty,:unit_price,:total_price)
            ON CONFLICT(order_id,line_index) DO UPDATE SET
              order_code=excluded.order_code,
              date_utc_ms=excluded.date_utc_ms,
              state=excluded.state,
              sku=excluded.sku,
              title=excluded.title,
              qty=excluded.qty,
              unit_price=excluded.unit_price,
              total_price=excluded.total_price
            """
            for ch in _chunked(rows):
                cur.executemany(sql, ch)
                total += cur.rowcount or 0
        else:
            sql = """
            INSERT INTO bridge_sales
              (order_id,line_index,order_code,date_utc_ms,state,sku,title,qty,unit_price,total_price)
            VALUES (%(order_id)s,%(line_index)s,%(order_code)s,%(date_utc_ms)s,%(state)s,%(sku)s,%(title)s,%(qty)s,%(unit_price)s,%(total_price)s)
            ON CONFLICT (order_id,line_index) DO UPDATE SET
              order_code=EXCLUDED.order_code,
              date_utc_ms=EXCLUDED.date_utc_ms,
              state=EXCLUDED.state,
              sku=EXCLUDED.sku,
              title=EXCLUDED.title,
              qty=EXCLUDED.qty,
              unit_price=EXCLUDED.unit_price,
              total_price=EXCLUDED.total_price
            """
            for ch in _chunked(rows):
                cur.executemany(sql, ch)
                total += cur.rowcount or 0
        c.commit()
    return int(total or 0)

# ─────────────────────────────────────────────────────────────────────
# Синхронизация строк продаж
# ─────────────────────────────────────────────────────────────────────
@router.post("/sync-by-ids")
async def bridge_sync_by_ids(request: Request, items: List[SyncItem] = Body(...)):
    if not _auth_ok(request):
        raise HTTPException(401, "Unauthorized")
    if not items:
        return {"synced_orders": 0, "items_inserted": 0, "skipped_no_sku": 0, "skipped_by_state": 0, "fallback_used": 0, "errors": []}

    inserted = 0
    touched_orders: set[str] = set()
    skipped_no_sku = 0
    skipped_by_state = 0
    fallback_used = 0
    errors: List[str] = []
    counters: DefaultDict[str, int] = defaultdict(int)

    offline_rows: List[Dict[str, Any]] = []
    fallback_refs: List[SyncItem] = []

    for it in items:
        try:
            if BRIDGE_ONLY_STATE and it.state and it.state != BRIDGE_ONLY_STATE:
                skipped_by_state += 1
                continue

            if it.sku and _canon_sku(it.sku):
                oid = str(it.id)
                touched_orders.add(oid)
                idx = it.line_index if it.line_index is not None else counters[oid]
                counters[oid] = int(idx) + 1

                qty = int(it.qty or 1)
                unit = _safe_float(it.unit_price)
                total = _line_total(qty, unit, it.total_price if it.total_price is not None else it.amount)

                offline_rows.append({
                    "order_id": oid,
                    "line_index": int(idx),
                    "order_code": _canon_str(it.code, 64),
                    "date_utc_ms": _to_ms(it.date),
                    "state": _canon_str(it.state, 64),
                    "sku": _canon_sku(it.sku),
                    "title": _canon_str(it.title, 512),
                    "qty": qty,
                    "unit_price": float(unit),
                    "total_price": float(total),
                })
            else:
                # если SKU не заполнен — пробуем Kaspi fallback
                if KASPI_FALLBACK_ENABLED and KASPI_TOKEN:
                    fallback_refs.append(it)
                else:
                    skipped_no_sku += 1
        except Exception:
            errors.append("payload_item_error")
            continue

    try:
        inserted += _upsert_rows(offline_rows)
    except Exception:
        errors.append("upsert_offline_error")

    # Добираем SKU из Kaspi
    for ref in fallback_refs:
        try:
            if BRIDGE_ONLY_STATE and ref.state and ref.state != BRIDGE_ONLY_STATE:
                skipped_by_state += 1
                continue
            entries = await _fetch_entries_fallback(ref.id)
            if not entries:
                skipped_no_sku += 1
                continue
            fallback_used += 1
            order_ms = _to_ms(ref.date)
            for e in entries:
                idx = e.get("__index")
                if idx is None:
                    idx = counters[ref.id]
                    counters[ref.id] = int(idx) + 1
                offline_rows = [{
                    "order_id": str(ref.id),
                    "line_index": int(idx),
                    "order_code": _canon_str(ref.code, 64),
                    "date_utc_ms": order_ms,
                    "state": _canon_str(ref.state, 64),
                    "sku": _canon_sku(e.get("sku")),
                    "title": _canon_str(e.get("title"), 512),
                    "qty": int(e.get("qty") or 1),
                    "unit_price": float(_safe_float(e.get("unit_price"))),
                    "total_price": float(_safe_float(e.get("total_price"))),
                }]
                inserted += _upsert_rows(offline_rows)
                touched_orders.add(str(ref.id))
        except Exception:
            errors.append("fallback_error")

    return {
        "synced_orders": len(touched_orders),
        "items_inserted": inserted,
        "skipped_no_sku": skipped_no_sku,
        "skipped_by_state": skipped_by_state,
        "fallback_used": fallback_used,
        "errors": errors,
    }

@router.post("/sync-from-ids")
def bridge_sync_from_ids(
    request: Request,
    date_from: str = Body(..., embed=True),
    date_to:   str = Body(..., embed=True),
    state: Optional[str] = Body(None, embed=True),
):
    if not _auth_ok(request):
        raise HTTPException(401, "Unauthorized")

    ms_from = _to_ms(date_from); ms_to = _to_ms(date_to)
    if ms_from is None or ms_to is None:
        raise HTTPException(400, "date_from/date_to должны быть YYYY-MM-DD")

    params = {
        "start": date_from,
        "end": date_to,
        "grouped": "0",
        "with_items": "1",
        "items_mode": "all",
        "limit": "100000",
        "order": "asc",
    }
    if state: params["states"] = state
    try:
        with httpx.Client(base_url=ORDERS_SERVICE_URL, timeout=HTTPX_TIMEOUT, limits=HTTPX_LIMITS) as cli:
            r = cli.get("/orders/ids", params=params)
            r.raise_for_status()
            data = r.json()
    except Exception as e:
        raise HTTPException(502, f"orders/ids fetch failed: {e}")

    rows: List[Dict[str, Any]] = []
    counters: DefaultDict[str, int] = defaultdict(int)

    for it in (data.get("items") or []):
        if BRIDGE_ONLY_STATE and it.get("state") and it["state"] != BRIDGE_ONLY_STATE:
            continue
        order_id = str(it.get("id") or "")
        order_code = _canon_str(it.get("number"), 64)
        state_val = _canon_str(it.get("state"), 64)
        date_ms = _to_ms(it.get("date"))

        if isinstance(it.get("items"), list) and it["items"]:
            for li in it["items"]:
                sku = _canon_sku(li.get("sku"))
                if not sku:
                    continue
                qty = int(li.get("
