# app/api/bridge_v2.py
from __future__ import annotations

from typing import List, Dict, Any, Optional, Tuple, DefaultDict, Literal
from collections import defaultdict
from pydantic import BaseModel
from fastapi import APIRouter, Body, HTTPException, Query
import os, sqlite3, httpx, math
from datetime import datetime

router = APIRouter()

# ─────────────────────────────────────────────────────────────────────
# Конфигурация
# ─────────────────────────────────────────────────────────────────────
BRIDGE_ONLY_STATE = (os.getenv("BRIDGE_ONLY_STATE") or "KASPI_DELIVERY").strip() or None

KASPI_FALLBACK_ENABLED = (os.getenv("KASPI_FALLBACK_ENABLED", "0").lower() in ("1", "true", "yes"))
KASPI_TOKEN   = os.getenv("KASPI_TOKEN", "").strip()
KASPI_BASEURL = (os.getenv("KASPI_BASE_URL") or "https://kaspi.kz/shop/api/v2").rstrip("/")

# имя таблицы FIFO-ledger (можно переопределить через env)
FIFO_LEDGER_TABLE = (os.getenv("FIFO_LEDGER_TABLE") or "fifo_ledger").strip()

# откуда читать «Номера заказов»
ORDERS_SERVICE_URL = (os.getenv("ORDERS_SERVICE_URL") or "http://127.0.0.1:8000").rstrip("/")
BRIDGE_SOURCE_IDS  = os.getenv("BRIDGE_SOURCE_IDS", "1").lower() in ("1", "true", "yes")

HTTPX_TIMEOUT = httpx.Timeout(connect=10.0, read=40.0, write=15.0, pool=40.0)
HTTPX_LIMITS  = httpx.Limits(max_connections=30, max_keepalive_connections=10)

def _kaspi_headers() -> Dict[str, str]:
    if not KASPI_TOKEN:
        raise HTTPException(500, "KASPI_TOKEN is not set")
    return {
        "X-Auth-Token": KASPI_TOKEN,
        "Accept": "application/vnd.api+json",
        "Content-Type": "application/vnd.api+json",
    }

# ─────────────────────────────────────────────────────────────────────
# База данных с fallback на SQLite при отсутствии psycopg2
# ─────────────────────────────────────────────────────────────────────
PRI_DB_URL = os.getenv("PROFIT_DB_URL") or os.getenv("DATABASE_URL") or "sqlite:///./profit.db"
FALLBACK_DB_URL = os.getenv("BRIDGE_FALLBACK_DB_URL", "sqlite:///./profit_bridge.db")

_ACTUAL_DB_URL = PRI_DB_URL
_FALLBACK_USED = False

def _driver_name() -> str:
    return "sqlite" if _ACTUAL_DB_URL.startswith("sqlite") else "pg"

def _sqlite_path(url: str) -> str:
    return url.split("sqlite:///")[-1]

def _get_conn():
    """Вернёт conn к PG, а при отсутствии psycopg2 — переключится на SQLite без 500 ошибок."""
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
    """Таблица с плоскими строками продаж: каждая позиция заказа — отдельная строка."""
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
# Хелперы нормализации/арифметики
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
    id: str                      # order_id
    code: Optional[str] = None   # order_code (номер)
    date: Optional[Any] = None   # дата заказа
    state: Optional[str] = None  # статус
    sku: Optional[str] = None    # если есть — пишем сразу
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
        "bridge_source_ids": BRIDGE_SOURCE_IDS,
    }

# ─────────────────────────────────────────────────────────────────────
# Синхронизация строк продаж: payload (ручной и из fallback)
# ─────────────────────────────────────────────────────────────────────
@router.post("/bridge/sync-by-ids")
async def bridge_sync_by_ids(items: List[SyncItem] = Body(...)):
    """Принимает заказы/позиции и кладёт их построчно в bridge_sales.
    Если какая-то позиция пришла без SKU — достраиваем по API Kaspi (если включён fallback)."""
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

    # достраиваем через Kaspi API те заказы, где SKU не было
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

# ─────────────────────────────────────────────────────────────────────
# Синхронизация строк продаж: из «Номера заказов» (HTTP)
# ─────────────────────────────────────────────────────────────────────
@router.post("/bridge/sync-from-ids")
def bridge_sync_from_ids(
    date_from: str = Body(..., embed=True),
    date_to:   str = Body(..., embed=True),
    state: Optional[str] = Body(None, embed=True),
):
    """
    Заполняет bridge_sales данными из /orders/ids (источник фронта «Номера заказов»).
    Это делает таблицу /bridge/list 100% консистентной с тем, что видит пользователь.
    """
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

        # разбор позиций
        if isinstance(it.get("items"), list) and it["items"]:
            for li in it["items"]:
                sku = _canon_sku(li.get("sku"))
                if not sku:
                    continue
                qty = int(li.get("qty") or 1)
                unit = _safe_float(li.get("unit_price"))
                total = _line_total(qty, unit, li.get("sum"))
                idx = counters[order_id]; counters[order_id] += 1
                rows.append({
                    "order_id": order_id,
                    "line_index": idx,
                    "order_code": order_code,
                    "date_utc_ms": date_ms,
                    "state": state_val,
                    "sku": sku,
                    "title": _canon_str(li.get("title") or li.get("name"), 512),
                    "qty": qty,
                    "unit_price": unit,
                    "total_price": total,
                })
        else:
            # одиночная строка (на всякий случай)
            sku = _canon_sku(it.get("sku"))
            if not sku:
                continue
            qty = 1
            unit = _safe_float(it.get("amount"))
            total = _line_total(qty, unit, it.get("amount"))
            idx = counters[order_id]; counters[order_id] += 1
            rows.append({
                "order_id": order_id,
                "line_index": idx,
                "order_code": order_code,
                "date_utc_ms": date_ms,
                "state": state_val,
                "sku": sku,
                "title": _canon_str(it.get("title") or it.get("name"), 512),
                "qty": qty,
                "unit_price": unit,
                "total_price": total,
            })

    inserted = _upsert_rows(rows)
    return {"orders": len(counters), "rows": len(rows), "inserted": inserted}

# ─────────────────────────────────────────────────────────────────────
# Плоский список строк (фильтр по SKU)
# ─────────────────────────────────────────────────────────────────────
@router.get("/bridge/list")
def bridge_list(
    sku: str = Query(..., description="Искомый SKU"),
    date_from: str = Query(..., description="YYYY-MM-DD"),
    date_to: str = Query(..., description="YYYY-MM-DD"),
    limit: int = Query(1000, ge=1, le=100000),
    order: Literal["asc", "desc"] = Query("asc"),
):
    _init_bridge_sales()
    ms_from = _to_ms(date_from)
    ms_to = _to_ms(date_to)
    if ms_from is None or ms_to is None:
        raise HTTPException(400, "date_from/date_to должны быть YYYY-MM-DD")
    ms_to = ms_to + 24 * 3600 * 1000 - 1  # включительно

    with _get_conn() as c:
        cur = c.cursor()
        if _driver_name() == "sqlite":
            cur.execute(
                f"""SELECT order_id,order_code,date_utc_ms,state,sku,title,qty,unit_price,total_price
                    FROM bridge_sales
                    WHERE sku=? AND date_utc_ms BETWEEN ? AND ?
                    ORDER BY date_utc_ms {"ASC" if order=="asc" else "DESC"}, order_id, line_index
                    LIMIT ?""",
                (_canon_sku(sku), ms_from, ms_to, int(limit)),
            )
            rows = [dict(r) for r in cur.fetchall()]
        else:
            cur.execute(
                f"""SELECT order_id,order_code,date_utc_ms,state,sku,title,qty,unit_price,total_price
                    FROM bridge_sales
                    WHERE sku=%(sku)s AND date_utc_ms BETWEEN %(ms_from)s AND %(ms_to)s
                    ORDER BY date_utc_ms {"ASC" if order=="asc" else "DESC"}, order_id, line_index
                    LIMIT %(lim)s""",
                dict(sku=_canon_sku(sku), ms_from=ms_from, ms_to=ms_to, lim=int(limit)),
            )
            rows = [dict(zip([d[0] for d in cur.description], r)) for r in cur.fetchall()]

    items = []
    for r in rows:
        d_iso = datetime.utcfromtimestamp((r.get("date_utc_ms") or 0) / 1000).isoformat(timespec="seconds")
        items.append({
            "order_id":   r.get("order_id"),
            "order_code": r.get("order_code"),  # номер заказа
            "date":       d_iso,
            "state":      r.get("state"),
            "sku":        r.get("sku"),
            "title":      r.get("title"),
            "qty":        r.get("qty"),
            "unit_price": r.get("unit_price"),
            "total_price":r.get("total_price"),
        })

    return {
        "sku": sku,
        "date_from": date_from,
        "date_to": date_to,
        "count": len(items),
        "items": items,
        "driver": _driver_name(),
        "fallback_used": _FALLBACK_USED,
    }

# ─────────────────────────────────────────────────────────────────────
# «№ заказа → позиции» (из bridge_sales, быстрый просмотр)
# ─────────────────────────────────────────────────────────────────────
@router.get("/bridge/by-orders")
def bridge_by_orders(
    date_from: str = Query(..., description="YYYY-MM-DD"),
    date_to: str   = Query(..., description="YYYY-MM-DD"),
    state: Optional[str] = Query(None, description="фильтр по состоянию (например KASPI_DELIVERY)"),
    limit_orders: int = Query(100000, ge=1, le=200000),
    order: Literal["asc", "desc"] = Query("asc"),
):
    """
    Список заказов за период с массивом всех позиций (SKU) каждого заказа.
    Итоги всегда считаются из суммы строк (а не order.amount).
    """
    _init_bridge_sales()
    ms_from = _to_ms(date_from)
    ms_to = _to_ms(date_to)
    if ms_from is None or ms_to is None:
        raise HTTPException(400, "date_from/date_to должны быть YYYY-MM-DD")
    ms_to = ms_to + 24 * 3600 * 1000 - 1

    where = ["date_utc_ms BETWEEN :ms_from AND :ms_to"]
    params: Dict[str, Any] = {"ms_from": ms_from, "ms_to": ms_to, "lim": int(limit_orders)}
    if state:
        where.append("state = :state")
        params["state"] = state

    with _get_conn() as c:
        cur = c.cursor()
        if _driver_name() == "sqlite":
            sql = f"""
            SELECT order_id, order_code, date_utc_ms, state, sku, title, qty, unit_price, total_price
            FROM bridge_sales
            WHERE {" AND ".join(where)}
            ORDER BY date_utc_ms {"ASC" if order=="asc" else "DESC"}, order_id, line_index
            LIMIT :lim
            """
            cur.execute(sql, params)
            rows = [dict(r) for r in cur.fetchall()]
        else:
            sql = f"""
            SELECT order_id, order_code, date_utc_ms, state, sku, title, qty, unit_price, total_price
            FROM bridge_sales
            WHERE {" AND ".join([w.replace(":", "%(")+")" for w in where])}
            ORDER BY date_utc_ms {"ASC" if order=="asc" else "DESC"}, order_id, line_index
            LIMIT %(lim)s
            """
            cur.execute(sql, params)
            rows = [dict(zip([d[0] for d in cur.description], r)) for r in cur.fetchall()]

    grouped: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        code = r.get("order_code") or ""  # пустой — тоже допустим
        if code not in grouped:
            grouped[code] = {
                "order_code": code,
                "order_id": r.get("order_id"),
                "date": datetime.utcfromtimestamp((r.get("date_utc_ms") or 0)/1000).isoformat(timespec="seconds"),
                "state": r.get("state"),
                "items": [],
                "totals": {"revenue": 0.0, "count_items": 0, "count_lines": 0}
            }
        qty = int(r.get("qty") or 1)
        unit = _safe_float(r.get("unit_price"))
        total = _line_total(qty, unit, r.get("total_price"))
        grouped[code]["items"].append({
            "sku": r.get("sku"),
            "title": r.get("title"),
            "qty": qty,
            "unit_price": unit,
            "total_price": total,
        })
        grouped[code]["totals"]["revenue"] += total
        grouped[code]["totals"]["count_items"] += qty
        grouped[code]["totals"]["count_lines"] += 1

    orders = list(grouped.values())

    return {
        "date_from": date_from,
        "date_to": date_to,
        "count_orders": len(orders),
        "orders": orders,
        "driver": _driver_name(),
        "fallback_used": _FALLBACK_USED,
        "note": "Стоимость/комиссия считаются в FIFO; здесь — связь №заказа → SKU.",
    }

# ─────────────────────────────────────────────────────────────────────
# Справка по комиссиям/себестоимости из «Мой склад» (fallback)
# ─────────────────────────────────────────────────────────────────────
def _fallback_commission_cost_for_skus(skus: List[str]) -> Dict[str, Dict[str, float]]:
    """
    Возвращает по SKU: {'commission_pct': float, 'avg_cost': float}
    Берём commission_pct из последней партии, если None — из категории.
    Среднюю себестоимость считаем по всем партиям: SUM(qty*unit_cost)/SUM(qty).
    """
    if not skus:
        return {}
    uniq = sorted({str(s) for s in skus})

    out: Dict[str, Dict[str, float]] = {s: {"commission_pct": 0.0, "avg_cost": 0.0} for s in uniq}
    with _get_conn() as c:
        cur = c.cursor()
        if _driver_name() == "sqlite":
            ph = ",".join(["?"] * len(uniq))
            cur.execute(f"SELECT sku, COALESCE(category,'') AS category FROM products WHERE sku IN ({ph})", uniq)
            cat_by_sku = {r["sku"]: r["category"] for r in cur.fetchall()}
            cur.execute("SELECT name, COALESCE(base_percent,0)+COALESCE(extra_percent,0)+COALESCE(tax_percent,0) AS pct FROM categories")
            pct_by_cat = {r["name"]: float(r["pct"] or 0.0) for r in cur.fetchall()}
            cur.execute(
                f"""SELECT b.sku, b.commission_pct
                    FROM batches b
                    JOIN (
                      SELECT sku, MAX(date) AS max_date, MAX(id) AS max_id
                      FROM batches WHERE sku IN ({ph})
                      GROUP BY sku
                    ) m ON m.sku=b.sku AND b.id=m.max_id
                    WHERE b.sku IN ({ph})""",
                uniq + uniq
            )
            last_comm = {r["sku"]: r["commission_pct"] for r in cur.fetchall()}
            cur.execute(
                f"""SELECT sku, (SUM(qty*unit_cost)*1.0)/NULLIF(SUM(qty),0) AS avg_cost
                    FROM batches WHERE sku IN ({ph}) GROUP BY sku""",
                uniq
            )
            avg_cost = {r["sku"]: float(r["avg_cost"] or 0.0) for r in cur.fetchall()}
        else:
            cur.execute("SELECT sku, COALESCE(category,'') AS category FROM products WHERE sku = ANY(%s)", (uniq,))
            cat_by_sku = {r[0]: r[1] for r in cur.fetchall()}
            cur.execute("""
                SELECT name, COALESCE(base_percent,0)+COALESCE(extra_percent,0)+COALESCE(tax_percent,0) AS pct
                FROM categories
            """)
            pct_by_cat = {r[0]: float(r[1] or 0.0) for r in cur.fetchall()}
            cur.execute("""
                SELECT DISTINCT ON (sku) sku, commission_pct
                FROM batches
                WHERE sku = ANY(%s)
                ORDER BY sku, date DESC, id DESC
            """, (uniq,))
            last_comm = {r[0]: r[1] for r in cur.fetchall()}
            cur.execute("""
                SELECT sku, SUM(qty*unit_cost)::float / NULLIF(SUM(qty),0)::float AS avg_cost
                FROM batches WHERE sku = ANY(%s) GROUP BY sku
            """, (uniq,))
            avg_cost = {r[0]: float(r[1] or 0.0) for r in cur.fetchall()}

    for s in uniq:
        pct = last_comm.get(s)
        if pct is None:
            pct = pct_by_cat.get(cat_by_sku.get(s, ""), 0.0)
        out[s]["commission_pct"] = float(pct or 0.0)
        out[s]["avg_cost"] = float(avg_cost.get(s, 0.0))
    return out

# ─────────────────────────────────────────────────────────────────────
# «№ заказа → позиции» + маржинальность (FIFO) с точным распределением
# (источник данных по заказам — «Номера заказов», при сбое — bridge_sales)
# ─────────────────────────────────────────────────────────────────────
@router.get("/bridge/by-orders-margins")
def bridge_by_orders_margins(
    date_from: str = Query(..., description="YYYY-MM-DD"),
    date_to:   str = Query(..., description="YYYY-MM-DD"),
    state: Optional[str] = Query(None, description="фильтр по состоянию (например KASPI_DELIVERY)"),
    order: Literal["asc", "desc"] = Query("asc"),
):
    """
    Возвращает заказы с позициями и маржой по каждой позиции.

    Источник позиций: /orders/ids (Номера заказов). Fallback: bridge_sales.
    Маржа берётся из FIFO-ledger и распределяется по строкам ПРОПОРЦИОНАЛЬНО сумме строки
    (если сумма недоступна — по qty). Так исключаем дублирование/расхождения.
    """
    # 0) временной диапазон и ms/sec для ledger
    ms_from = _to_ms(date_from); ms_to = _to_ms(date_to)
    if ms_from is None or ms_to is None:
        raise HTTPException(400, "date_from/date_to должны быть YYYY-MM-DD")
    ms_to = ms_to + 24*3600*1000 - 1
    sec_from, sec_to = ms_from // 1000, ms_to // 1000

    # 1) Грузим заказы из «Номера заказов» (или fallback на bridge_sales)
    orders_lines: List[Dict[str, Any]] = []  # элементы: {order_code, order_id, date_ms, state, sku, title, qty, unit_price, total_price}
    source_used = "ids"
    if BRIDGE_SOURCE_IDS:
        try:
            params = {
                "start": date_from,
                "end": date_to,
                "grouped": "0",
                "with_items": "1",
                "items_mode": "all",
                "limit": "100000",
                "order": order,
            }
            if state: params["states"] = state
            with httpx.Client(base_url=ORDERS_SERVICE_URL, timeout=HTTPX_TIMEOUT, limits=HTTPX_LIMITS) as cli:
                r = cli.get("/orders/ids", params=params)
                r.raise_for_status()
                j = r.json()
            for it in (j.get("items") or []):
                if BRIDGE_ONLY_STATE and it.get("state") and it["state"] != BRIDGE_ONLY_STATE:
                    continue
                order_id = str(it.get("id") or "")
                order_code = _canon_str(it.get("number"), 64) or ""
                date_ms = _to_ms(it.get("date"))
                st = _canon_str(it.get("state"), 64)
                if isinstance(it.get("items"), list) and it["items"]:
                    for li in it["items"]:
                        sku = _canon_sku(li.get("sku")); 
                        if not sku: 
                            continue
                        qty = int(li.get("qty") or 1)
                        unit = _safe_float(li.get("unit_price"))
                        total = _line_total(qty, unit, li.get("sum"))
                        orders_lines.append(dict(
                            order_code=order_code, order_id=order_id, date_ms=date_ms, state=st,
                            sku=sku, title=_canon_str(li.get("title") or li.get("name"), 512),
                            qty=qty, unit_price=unit, total_price=total
                        ))
                else:
                    sku = _canon_sku(it.get("sku"))
                    if not sku:
                        continue
                    qty = 1
                    unit = _safe_float(it.get("amount"))
                    total = _line_total(qty, unit, it.get("amount"))
                    orders_lines.append(dict(
                        order_code=order_code, order_id=order_id, date_ms=date_ms, state=st,
                        sku=sku, title=_canon_str(it.get("title") or it.get("name"), 512),
                        qty=qty, unit_price=unit, total_price=total
                    ))
        except Exception:
            source_used = "bridge"

    if not orders_lines:
        # fallback: читаем из bridge_sales
        source_used = "bridge"
        where = ["date_utc_ms BETWEEN :ms_from AND :ms_to"]
        params: Dict[str, Any] = {"ms_from": ms_from, "ms_to": ms_to}
        if state:
            where.append("state = :state")
            params["state"] = state
        _init_bridge_sales()
        with _get_conn() as c:
            cur = c.cursor()
            if _driver_name() == "sqlite":
                sql = f"""
                    SELECT order_id, order_code, date_utc_ms, state, sku, title, qty, unit_price, total_price
                    FROM bridge_sales
                    WHERE {" AND ".join(where)}
                    ORDER BY date_utc_ms {"ASC" if order=="asc" else "DESC"}, order_id, line_index
                """
                cur.execute(sql, params)
                rows = [dict(r) for r in cur.fetchall()]
            else:
                sql = f"""
                    SELECT order_id, order_code, date_utc_ms, state, sku, title, qty, unit_price, total_price
                    FROM bridge_sales
                    WHERE {" AND ".join([w.replace(":", "%(")+")" for w in where])}
                    ORDER BY date_utc_ms {"ASC" if order=="asc" else "DESC"}, order_id, line_index
                """
                cur.execute(sql, params)
                rows = [dict(zip([d[0] for d in cur.description], r)) for r in cur.fetchall()]
        for r in rows:
            qty = int(r.get("qty") or 1)
            unit = _safe_float(r.get("unit_price"))
            total = _line_total(qty, unit, r.get("total_price"))
            orders_lines.append(dict(
                order_code=str(r.get("order_code") or ""),
                order_id=str(r.get("order_id") or ""),
                date_ms=int(r.get("date_utc_ms") or 0),
                state=_canon_str(r.get("state"), 64),
                sku=str(r.get("sku") or ""),
                title=_canon_str(r.get("title"), 512),
                qty=qty, unit_price=unit, total_price=total
            ))

    # 2) Агрегаты FIFO-ledger по (order_code, sku) — учитываем и ms, и sec
    with _get_conn() as c:
        cur = c.cursor()
        if _driver_name() == "sqlite":
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (FIFO_LEDGER_TABLE,))
            ledger_exists = cur.fetchone() is not None
            if ledger_exists:
                sql_l = f"""
                    SELECT order_code, sku,
                           SUM(COALESCE(commission,0)) AS commission,
                           SUM(COALESCE(cost,0))       AS cost,
                           SUM(COALESCE(revenue,0))    AS revenue,
                           SUM(COALESCE(profit,0))     AS profit
                    FROM {FIFO_LEDGER_TABLE}
                    WHERE
                      (date_utc_ms BETWEEN ? AND ?)
                      OR (date_utc_ms BETWEEN ? AND ?)
                      OR (date_utc_ms IS NULL)
                    GROUP BY order_code, sku
                """
                cur.execute(sql_l, (ms_from, ms_to, sec_from, sec_to))
                ledger_rows = [dict(r) for r in cur.fetchall()]
            else:
                ledger_rows = []
        else:
            cur.execute("SELECT to_regclass(%s)", (FIFO_LEDGER_TABLE,))
            ledger_exists = cur.fetchone()[0] is not None
            if ledger_exists:
                sql_l = f"""
                    SELECT order_code, sku,
                           SUM(COALESCE(commission,0)) AS commission,
                           SUM(COALESCE(cost,0))       AS cost,
                           SUM(COALESCE(revenue,0))    AS revenue,
                           SUM(COALESCE(profit,0))     AS profit
                    FROM {FIFO_LEDGER_TABLE}
                    WHERE
                      (date_utc_ms BETWEEN %(msf)s AND %(mst)s)
                      OR (date_utc_ms BETWEEN %(sf)s AND %(st)s)
                      OR (date_utc_ms IS NULL)
                    GROUP BY order_code, sku
                """
                cur.execute(sql_l, dict(msf=ms_from, mst=ms_to, sf=sec_from, st=sec_to))
                ledger_rows = [dict(zip([d[0] for d in cur.description], r)) for r in cur.fetchall()]
            else:
                ledger_rows = []

    ledger_map: Dict[Tuple[str, str], Dict[str, float]] = {}
    for r in ledger_rows:
        key = (str(r.get("order_code") or ""), str(r.get("sku") or ""))
        ledger_map[key] = {
            "commission": _safe_float(r.get("commission")),
            "cost": _safe_float(r.get("cost")),
            "revenue": _safe_float(r.get("revenue")),
            "profit": _safe_float(r.get("profit")),
        }

    # 3) Группировка по заказам + подготовка распределения по ключу (order_code, sku)
    grouped_orders: Dict[str, Dict[str, Any]] = {}
    lines_by_key: DefaultDict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    weight_sum_by_key: DefaultDict[Tuple[str, str], float] = defaultdict(float)

    for ln in orders_lines:
        code = ln["order_code"]
        if code not in grouped_orders:
            grouped_orders[code] = {
                "order_code": code,
                "order_id": ln["order_id"],
                "date": datetime.utcfromtimestamp((ln["date_ms"] or 0)/1000).isoformat(timespec="seconds"),
                "state": ln.get("state"),
                "items": [],
                "totals": {"revenue": 0.0, "commission": 0.0, "cost": 0.0, "profit": 0.0, "count_items": 0, "count_lines": 0},
                "source": source_used,
            }
        grouped_orders[code]["items"].append({
            "sku": ln["sku"], "title": ln.get("title"),
            "qty": ln["qty"], "unit_price": ln["unit_price"],
            "total_price": ln["total_price"],
            "commission": 0.0, "cost": 0.0, "profit": 0.0,
        })
        grouped_orders[code]["totals"]["revenue"]    += ln["total_price"]
        grouped_orders[code]["totals"]["count_items"]+= int(ln["qty"] or 1)
        grouped_orders[code]["totals"]["count_lines"]+= 1

        key = (code, ln["sku"])
        lines_by_key[key].append(grouped_orders[code]["items"][-1])
        w = ln["total_price"] if ln["total_price"] > 0 else float(ln["qty"] or 0)
        weight_sum_by_key[key] += (w if w > 0 else 1.0)

    # 4) Распределяем агрегации ledger по строкам
    for key, agg in ledger_map.items():
        lines = lines_by_key.get(key) or []
        if not lines:
            continue
        total_weight = weight_sum_by_key.get(key, 0.0) or 0.0
        if total_weight <= 0:
            total_weight = float(len(lines))

        for ln in lines:
            w = ln["total_price"] if ln["total_price"] > 0 else float(ln["qty"] or 0)
            if w <= 0: w = 1.0
            share = min(1.0, max(0.0, w / total_weight))
            commission = agg["commission"] * share
            cost       = agg["cost"] * share
            ln["commission"] = commission
            ln["cost"]       = cost
            ln["profit"]     = ln["total_price"] - commission - cost

    # 5) Fallback: если по ключу нет ledger, добиваем из «Мой склад»
    need_fallback_skus = sorted({ ln["sku"] for key, lines in lines_by_key.items() if key not in ledger_map for ln in lines })
    fb_map = _fallback_commission_cost_for_skus(need_fallback_skus) if need_fallback_skus else {}
    for key, lines in lines_by_key.items():
        if key in ledger_map:
            continue
        for ln in lines:
            ref = fb_map.get(ln["sku"], {"commission_pct": 0.0, "avg_cost": 0.0})
            ln["commission"] = round(ln["total_price"] * (ref["commission_pct"] / 100.0), 4)
            ln["cost"]       = round(ref["avg_cost"] * int(ln["qty"] or 1), 4)
            ln["profit"]     = ln["total_price"] - ln["commission"] - ln["cost"]

    # 6) Финальные итоги по заказам
    orders_out = []
    for code, od in grouped_orders.items():
        t = od["totals"]
        for ln in od["items"]:
            t["commission"] += ln["commission"]
            t["cost"]       += ln["cost"]
            t["profit"]     += ln["profit"]
        orders_out.append(od)

    return {
        "date_from": date_from,
        "date_to": date_to,
        "count_orders": len(orders_out),
        "orders": orders_out,
        "driver": _driver_name(),
        "fallback_used": _FALLBACK_USED,
        "note": "Источник: Номера заказов (fallback bridge). Ledger ms/sec. Fallback комиссия/себестоимость из «Мой склад».",
    }
