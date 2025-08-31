# app/api/bridge_v2.py
from __future__ import annotations

from typing import List, Dict, Any, Optional, Tuple, DefaultDict, Literal
from collections import defaultdict
from pydantic import BaseModel
from fastapi import APIRouter, Body, HTTPException, Query
import os, sqlite3, httpx
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
        c.commit()

def _chunked(items: List[Dict[str, Any]], n: int = 500):
    for i in range(0, len(items), n):
        yield items[i:i + n]

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
# Утилиты
# ─────────────────────────────────────────────────────────────────────
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
        return float(x)
    except Exception:
        return default

def _norm(s: Optional[str], maxlen: int = 512) -> Optional[str]:
    if not s:
        return None
    s = str(s)
    return s if len(s) <= maxlen else s[:maxlen]

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
    }

# ─────────────────────────────────────────────────────────────────────
# Синхронизация строк продаж
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

            if it.sku and str(it.sku).strip():
                oid = it.id
                touched_orders.add(oid)
                idx = it.line_index if it.line_index is not None else counters[oid]
                counters[oid] = int(idx) + 1

                qty = int(it.qty or 1)
                total = it.total_price if it.total_price is not None else (it.amount if it.amount is not None else None)
                if total is None and it.unit_price is not None:
                    total = _safe_float(it.unit_price) * qty
                if total is None:
                    total = 0.0
                unit = it.unit_price if it.unit_price is not None else (float(total) / max(1, qty))

                offline_rows.append({
                    "order_id": oid,
                    "line_index": int(idx),
                    "order_code": _norm(it.code, 64),
                    "date_utc_ms": _to_ms(it.date),
                    "state": _norm(it.state, 64),
                    "sku": str(it.sku).strip(),
                    "title": _norm(it.title, 512),
                    "qty": qty,
                    "unit_price": float(unit or 0),
                    "total_price": float(total or 0),
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
                    "order_id": ref.id,
                    "line_index": int(idx),
                    "order_code": _norm(ref.code, 64),
                    "date_utc_ms": order_ms,
                    "state": _norm(ref.state, 64),
                    "sku": e["sku"],
                    "title": _norm(e.get("title"), 512),
                    "qty": int(e["qty"]),
                    "unit_price": float(e["unit_price"] or 0),
                    "total_price": float(e["total_price"] or 0),
                }]
                inserted += _upsert_rows(offline_rows)
                touched_orders.add(ref.id)
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
# Плоский список строк (фильтр по SKU) — как раньше
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
                    ORDER BY date_utc_ms {"ASC" if order=="asc" else "DESC"}
                    LIMIT ?""",
                (sku, ms_from, ms_to, int(limit)),
            )
            rows = [dict(r) for r in cur.fetchall()]
        else:
            cur.execute(
                f"""SELECT order_id,order_code,date_utc_ms,state,sku,title,qty,unit_price,total_price
                    FROM bridge_sales
                    WHERE sku=%(sku)s AND date_utc_ms BETWEEN %(ms_from)s AND %(ms_to)s
                    ORDER BY date_utc_ms {"ASC" if order=="asc" else "DESC"}
                    LIMIT %(lim)s""",
                dict(sku=sku, ms_from=ms_from, ms_to=ms_to, lim=int(limit)),
            )
            rows = [dict(zip([d[0] for d in cur.description], r)) for r in cur.fetchall()]

    items = []
    for r in rows:
        d_iso = datetime.utcfromtimestamp((r.get("date_utc_ms") or 0) / 1000).isoformat(timespec="seconds")
        items.append({
            "order_id":   r.get("order_id"),
            "order_code": r.get("order_code"),  # ← номер заказа
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
# ГРУППИРОВКА ПО НОМЕРУ ЗАКАЗА: «какой SKU кому принадлежит»
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
    Вернёт список заказов за период с массивом всех позиций (SKU) каждого заказа.
    Это то, что нужно фронту для отображения:
      - order_code (номер),
      - date,
      - state,
      - items: [{sku, title, qty, unit_price, total_price}, ...]
      - totals: revenue, count_items, count_lines
    Себестоимость и комиссия подтянутся в FIFO по этому же ключу (SKU/дата).
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

    # группируем по номеру заказа
    grouped: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        code = r.get("order_code") or ""  # может быть пустым — всё равно группируем
        if code not in grouped:
            grouped[code] = {
                "order_code": code,
                "order_id": r.get("order_id"),
                "date": datetime.utcfromtimestamp((r.get("date_utc_ms") or 0)/1000).isoformat(timespec="seconds"),
                "state": r.get("state"),
                "items": [],
                "totals": {"revenue": 0.0, "count_items": 0, "count_lines": 0}
            }
        grouped[code]["items"].append({
            "sku": r.get("sku"),
            "title": r.get("title"),
            "qty": int(r.get("qty") or 1),
            "unit_price": float(r.get("unit_price") or 0.0),
            "total_price": float(r.get("total_price") or 0.0),
        })
        grouped[code]["totals"]["revenue"] += float(r.get("total_price") or 0.0)
        grouped[code]["totals"]["count_items"] += int(r.get("qty") or 1)
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
# НОВОЕ: «Привязка № заказа → SKU» + маржинальность (FIFO)
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
    Источник позиций: bridge_sales. Маржа — из FIFO-ledger (таблица FIFO_LEDGER_TABLE),
    суммируется по ключу (order_code, sku) внутри периода.
    Поля items: sku, title, qty, unit_price, total_price, commission, cost, profit
    Поля totals: revenue, commission, cost, profit, count_items, count_lines
    """
    _init_bridge_sales()
    ms_from = _to_ms(date_from)
    ms_to   = _to_ms(date_to)
    if ms_from is None or ms_to is None:
        raise HTTPException(400, "date_from/date_to должны быть YYYY-MM-DD")
    ms_to = ms_to + 24*3600*1000 - 1  # включительно

    where = ["bs.date_utc_ms BETWEEN :ms_from AND :ms_to"]
    params: Dict[str, Any] = {"ms_from": ms_from, "ms_to": ms_to}
    if state:
        where.append("bs.state = :state")
        params["state"] = state

    with _get_conn() as c:
        cur = c.cursor()
        if _driver_name() == "sqlite":
            # проверим наличие ledger
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (FIFO_LEDGER_TABLE,))
            ledger_exists = cur.fetchone() is not None

            # агрегаты ledger по (order_code, sku) за период
            ledger_sql = f"""
                SELECT order_code, sku,
                       SUM(COALESCE(commission,0)) AS commission,
                       SUM(COALESCE(cost,0))       AS cost,
                       SUM(COALESCE(revenue,0))    AS revenue,
                       SUM(COALESCE(profit,0))     AS profit
                FROM {FIFO_LEDGER_TABLE}
                WHERE date_utc_ms BETWEEN :ms_from AND :ms_to
                GROUP BY order_code, sku
            """ if ledger_exists else "SELECT NULL AS order_code, NULL AS sku, 0 AS commission, 0 AS cost, 0 AS revenue, 0 AS profit"

            sql = f"""
            SELECT
              bs.order_id, bs.order_code, bs.date_utc_ms, bs.state,
              bs.sku, bs.title, bs.qty, bs.unit_price, bs.total_price,
              COALESCE(l.commission, 0.0) AS commission,
              COALESCE(l.cost, 0.0)       AS cost,
              COALESCE(l.revenue, 0.0)    AS rev_from_ledger,
              COALESCE(l.profit, 0.0)     AS profit
            FROM bridge_sales bs
            LEFT JOIN ({ledger_sql}) l
              ON l.order_code = bs.order_code AND l.sku = bs.sku
            WHERE {" AND ".join(where)}
            ORDER BY bs.date_utc_ms {"ASC" if order=="asc" else "DESC"}, bs.order_id, bs.line_index
            """
            cur.execute(sql, params)
            rows = [dict(r) for r in cur.fetchall()]
        else:
            # postgres: проверим наличие ledger
            cur.execute("SELECT to_regclass(%s)", (FIFO_LEDGER_TABLE,))
            ledger_exists = cur.fetchone()[0] is not None

            ledger_sql = f"""
                SELECT order_code, sku,
                       SUM(COALESCE(commission,0)) AS commission,
                       SUM(COALESCE(cost,0))       AS cost,
                       SUM(COALESCE(revenue,0))    AS revenue,
                       SUM(COALESCE(profit,0))     AS profit
                FROM {FIFO_LEDGER_TABLE}
                WHERE date_utc_ms BETWEEN %(ms_from)s AND %(ms_to)s
                GROUP BY order_code, sku
            """ if ledger_exists else "SELECT NULL::text order_code, NULL::text sku, 0::double precision commission, 0::double precision cost, 0::double precision revenue, 0::double precision profit"

            sql = f"""
            WITH L AS ({ledger_sql})
            SELECT
              bs.order_id, bs.order_code, bs.date_utc_ms, bs.state,
              bs.sku, bs.title, bs.qty, bs.unit_price, bs.total_price,
              COALESCE(l.commission, 0.0) AS commission,
              COALESCE(l.cost, 0.0)       AS cost,
              COALESCE(l.revenue, 0.0)    AS rev_from_ledger,
              COALESCE(l.profit, 0.0)     AS profit
            FROM bridge_sales bs
            LEFT JOIN L l
              ON l.order_code = bs.order_code AND l.sku = bs.sku
            WHERE {" AND ".join([w.replace(':', '%(')+')' for w in where])}
            ORDER BY bs.date_utc_ms {"ASC" if order=="asc" else "DESC"}, bs.order_id, bs.line_index
            """
            cur.execute(sql, params)
            rows = [dict(zip([d[0] for d in cur.description], r)) for r in cur.fetchall()]

    # группируем по номеру заказа и суммируем totals
    grouped: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        code = r.get("order_code") or ""
        if code not in grouped:
            grouped[code] = {
                "order_code": code,
                "order_id": r.get("order_id"),
                "date": datetime.utcfromtimestamp((r.get("date_utc_ms") or 0)/1000).isoformat(timespec="seconds"),
                "state": r.get("state"),
                "items": [],
                "totals": {"revenue": 0.0, "commission": 0.0, "cost": 0.0, "profit": 0.0, "count_items": 0, "count_lines": 0},
            }

        revenue   = float(r.get("total_price") or 0.0)  # фактическая выручка по строке продажи
        commission= float(r.get("commission") or 0.0)
        cost      = float(r.get("cost") or 0.0)
        # если ledger не хранит revenue — считаем прибыль как выручка-commission-cost
        profit    = float(r.get("profit")) if r.get("rev_from_ledger") not in (None, 0, 0.0) else (revenue - commission - cost)

        grouped[code]["items"].append({
            "sku": r.get("sku"),
            "title": r.get("title"),
            "qty": int(r.get("qty") or 1),
            "unit_price": float(r.get("unit_price") or 0.0),
            "total_price": revenue,
            "commission": commission,
            "cost": cost,
            "profit": profit,
        })

        t = grouped[code]["totals"]
        t["revenue"]    += revenue
        t["commission"] += commission
        t["cost"]       += cost
        t["profit"]     += profit
        t["count_items"]+= int(r.get("qty") or 1)
        t["count_lines"]+= 1

    orders = list(grouped.values())
    return {
        "date_from": date_from,
        "date_to": date_to,
        "count_orders": len(orders),
        "orders": orders,
        "driver": _driver_name(),
        "fallback_used": _FALLBACK_USED,
        "note": "Привязка заказ → SKU с маржой по FIFO",
    }
