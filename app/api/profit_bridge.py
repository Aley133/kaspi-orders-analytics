# app/api/profit_bridge.py
from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timedelta, time as dt_time

import pytz
from fastapi import APIRouter, HTTPException, Query, Depends, Request
from pydantic import BaseModel

# ---------- DB (PG/SQLite) ----------
try:
    from sqlalchemy import create_engine, text
    _SQLA_OK = True
except Exception:
    _SQLA_OK = False

import sqlite3
from contextlib import contextmanager

DATABASE_URL = os.getenv("DATABASE_URL")
_USE_PG = bool(DATABASE_URL and _SQLA_OK)
if _USE_PG:
    _engine = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)

def _resolve_db_path() -> str:
    target = os.getenv("DB_PATH", "/data/kaspi-orders.sqlite3")
    os.makedirs(os.path.dirname(target), exist_ok=True)
    return target

DB_PATH = _resolve_db_path()

@contextmanager
def _db():
    if _USE_PG:
        with _engine.begin() as conn:
            yield conn
    else:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

def _q(sql: str): return text(sql) if _USE_PG else sql
def _rows(rows): return [dict(r._mapping) for r in rows] if _USE_PG else [dict(r) for r in rows]

# ---------- Auth ----------
def require_api_key(req: Request) -> bool:
    required = os.getenv("API_KEY", "").strip()
    if not required:
        return True
    got = (req.headers.get("X-API-Key") or req.query_params.get("api_key") or "")
    got = got.strip().strip("<>").strip('"').strip("'")
    if got != required:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return True

# ---------- Kaspi ----------
KASPI_TOKEN = os.getenv("KASPI_TOKEN", "").strip()
KASPI_BASE_URL = os.getenv("KASPI_BASE_URL", "https://kaspi.kz/shop/api/v2").rstrip("/")

KaspiClient = None
try:
    from app.kaspi_client import KaspiClient as _KC  # type: ignore
    KaspiClient = _KC
except Exception:
    try:
        from ..kaspi_client import KaspiClient as _KC  # type: ignore
        KaspiClient = _KC
    except Exception:
        KaspiClient = None

import httpx

def _kaspi_headers() -> Dict[str, str]:
    if not KASPI_TOKEN:
        raise HTTPException(status_code=500, detail="KASPI_TOKEN is not set")
    return {
        "X-Auth-Token": KASPI_TOKEN,
        "Accept": "application/vnd.api+json",
        "Content-Type": "application/vnd.api+json",
    }

# ---------- Time utils ----------
def _tzinfo(name: str) -> pytz.BaseTzInfo:
    try:
        return pytz.timezone(name)
    except Exception:
        raise HTTPException(status_code=400, detail=f"Bad timezone: {name}")

def _parse_date_local(d: str, tz: str) -> datetime:
    z = _tzinfo(tz)
    y, m, dd = map(int, d.split("-"))
    return z.localize(datetime(y, m, dd, 0, 0, 0, 0))

def _bd_delta(hhmm: str) -> timedelta:
    hh, mm = map(int, (hhmm or "20:00").split(":"))
    return timedelta(hours=hh, minutes=mm)

def _build_window(start: str, end: str, tz: str, use_bd: bool, bd_start: str) -> Tuple[int, int]:
    z = _tzinfo(tz)
    s0 = _parse_date_local(start, tz)
    e0 = _parse_date_local(end, tz) + timedelta(days=1) - timedelta(milliseconds=1)
    if use_bd:
        delta = _bd_delta(bd_start)
        s = z.localize(datetime.combine((s0.date() - timedelta(days=1)), dt_time(0, 0))) + delta
        e = z.localize(datetime.combine(e0.date(), dt_time(0, 0))) + delta - timedelta(milliseconds=1)
    else:
        s, e = s0, e0
    return int(s.astimezone(pytz.UTC).timestamp() * 1000), int(e.astimezone(pytz.UTC).timestamp() * 1000)

# ---------- Schema (добавлена таблица sales) ----------
def _ensure_schema():
    with _db() as c:
        if _USE_PG:
            c.execute(_q("""CREATE TABLE IF NOT EXISTS orders(
                id TEXT PRIMARY KEY,
                date TIMESTAMP NOT NULL,
                customer TEXT
            )"""))
            c.execute(_q("""CREATE TABLE IF NOT EXISTS order_items(
                id SERIAL PRIMARY KEY,
                order_id TEXT NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
                sku TEXT NOT NULL,
                qty INTEGER NOT NULL,
                unit_price DOUBLE PRECISION NOT NULL,
                commission_pct DOUBLE PRECISION
            )"""))
            c.execute(_q("""CREATE TABLE IF NOT EXISTS sales(
                id SERIAL PRIMARY KEY,
                order_id TEXT,
                date TIMESTAMP NOT NULL,
                sku TEXT NOT NULL,
                qty INTEGER NOT NULL,
                unit_price DOUBLE PRECISION NOT NULL,
                commission_pct DOUBLE PRECISION
            )"""))
            c.execute(_q("CREATE INDEX IF NOT EXISTS idx_order_items_sku ON order_items(sku)"))
            c.execute(_q("CREATE INDEX IF NOT EXISTS idx_orders_date ON orders(date)"))
            c.execute(_q("CREATE INDEX IF NOT EXISTS idx_sales_date ON sales(date)"))
            c.execute(_q("CREATE INDEX IF NOT EXISTS idx_sales_sku ON sales(sku)"))
        else:
            c.executescript("""
            CREATE TABLE IF NOT EXISTS orders(
                id TEXT PRIMARY KEY,
                date TEXT NOT NULL,
                customer TEXT
            );
            CREATE TABLE IF NOT EXISTS order_items(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id TEXT NOT NULL,
                sku TEXT NOT NULL,
                qty INTEGER NOT NULL,
                unit_price REAL NOT NULL,
                commission_pct REAL,
                FOREIGN KEY(order_id) REFERENCES orders(id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS sales(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id TEXT,
                date TEXT NOT NULL,
                sku TEXT NOT NULL,
                qty INTEGER NOT NULL,
                unit_price REAL NOT NULL,
                commission_pct REAL
            );
            CREATE INDEX IF NOT EXISTS idx_order_items_sku ON order_items(sku);
            CREATE INDEX IF NOT EXISTS idx_orders_date ON orders(date);
            CREATE INDEX IF NOT EXISTS idx_sales_date ON sales(date);
            CREATE INDEX IF NOT EXISTS idx_sales_sku  ON sales(sku);
            """)

# ---------- Models ----------
class OrderItemIn(BaseModel):
    sku: str
    qty: int
    unit_price: float
    commission_pct: Optional[float] = None

class OrderIn(BaseModel):
    id: str
    date: str  # ISO
    customer: Optional[str] = None
    items: List[OrderItemIn]

# ---------- helpers ----------
def _get_num(d: Any, keys: List[str], default: float = 0.0) -> float:
    for k in keys:
        if k in d and d[k] is not None:
            try:
                return float(d[k])
            except Exception:
                pass
    return default

def _get_int(d: Any, keys: List[str], default: int = 0) -> int:
    for k in keys:
        if k in d and d[k] is not None:
            try:
                return int(d[k])
            except Exception:
                pass
    return default

def _get_str(d: Any, keys: List[str], default: str = "") -> str:
    for k in keys:
        v = d.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return default

def _norm_state(s: str) -> str:
    return (s or "").strip().upper()

def _parse_states_csv(s: Optional[str]) -> Optional[set[str]]:
    if not s:
        return None
    return { _norm_state(x) for x in s.replace(";", ",").split(",") if x.strip() }

def _extract_product_code(entry: Dict[str, Any], included: Dict[str, Dict[str, Any]]) -> str:
    attrs = entry.get("attributes", {}) or {}
    sku = _get_str(attrs, ["code", "productCode", "sku"])
    if sku:
        return sku
    rel = entry.get("relationships", {}) or {}
    for key in ("product", "masterProduct", "item", "sku"):
        node = rel.get(key)
        if not node:
            continue
        data = node.get("data")
        if isinstance(data, dict):
            ref_id = data.get("id")
            if ref_id and ref_id in included:
                a2 = included[ref_id].get("attributes", {}) or {}
                sku = _get_str(a2, ["code", "sku", "productCode"])
                if sku:
                    return sku
    return _get_str(entry, ["id"], "")

# ---------- fetch позиций ----------
async def _fetch_entries_httpx(order_id: str) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Возвращает (items, debug)."""
    debug: Dict[str, Any] = {}
    headers = _kaspi_headers()
    timeout = httpx.Timeout(20.0, connect=10.0)
    async with httpx.AsyncClient(base_url=KASPI_BASE_URL, timeout=timeout) as cli:
        # 1) /orderentries
        try:
            params = {"filter[order.id]": order_id, "include": "product", "page[size]": "200"}
            r = await cli.get("/orderentries", params=params, headers=headers)
            debug["orderentries_status"] = r.status_code
            if r.status_code == 200:
                j = r.json()
                data = j.get("data", []) or []
                included = {x["id"]: x for x in (j.get("included", []) or []) if isinstance(x, dict) and "id" in x}
                out = []
                for e in data:
                    attrs = e.get("attributes", {}) or {}
                    qty = _get_int(attrs, ["quantity", "qty", "count"], 1)
                    price = _get_num(attrs, ["unitPrice", "basePrice", "price"], 0.0)
                    sku = _extract_product_code(e, included) or _get_str(attrs, ["code", "productCode", "sku"])
                    if sku:
                        out.append({"sku": sku, "qty": qty, "unit_price": price})
                if out:
                    return out, debug
        except httpx.HTTPError as ex:
            debug["orderentries_error"] = str(ex)

        # 2) /orders/{id}?include=entries.product
        try:
            params = {"include": "entries.product"}
            r = await cli.get(f"/orders/{order_id}", params=params, headers=headers)
            debug["order_by_id_status"] = r.status_code
            if r.status_code == 200:
                j = r.json()
                included_raw = j.get("included", []) or []
                included = {x["id"]: x for x in included_raw if isinstance(x, dict) and "id" in x}
                out = []
                for inc in included_raw:
                    t = str(inc.get("type", "")).lower()
                    if "entry" not in t:
                        continue
                    attrs = inc.get("attributes", {}) or {}
                    qty = _get_int(attrs, ["quantity", "qty", "count"], 1)
                    price = _get_num(attrs, ["unitPrice", "basePrice", "price"], 0.0)
                    sku = _extract_product_code(inc, included) or _get_str(attrs, ["code", "productCode", "sku"])
                    if sku:
                        out.append({"sku": sku, "qty": qty, "unit_price": price})
                return out, debug
        except httpx.HTTPError as ex:
            debug["order_by_id_error"] = str(ex)

    return [], debug

# ---------- чтение списка заказов ----------
async def _iter_orders(start_ms: int, end_ms: int, tz: str, date_field: str,
                       inc_states: Optional[set[str]], exc_states: Optional[set[str]],
                       max_orders: Optional[int] = None) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    if KaspiClient:
        cli = KaspiClient(token=KASPI_TOKEN, base_url=KASPI_BASE_URL)
        s = datetime.utcfromtimestamp(start_ms / 1000.0)
        e = datetime.utcfromtimestamp(end_ms / 1000.0)
        step = timedelta(days=7)
        cur = s
        while cur <= e:
            nxt = min(cur + step, e)
            try_field = date_field or "creationDate"
            while True:
                try:
                    for order in cli.iter_orders(start=cur, end=nxt, filter_field=try_field):
                        oid = str(order.get("id"))
                        attrs = order.get("attributes", {}) or {}
                        st = _norm_state(attrs.get("state", ""))
                        if inc_states and st not in inc_states:
                            continue
                        if exc_states and st in exc_states:
                            continue
                        ms = attrs.get(try_field) or attrs.get("creationDate") or start_ms
                        try:
                            ms = int(ms)
                        except Exception:
                            try:
                                ms = int(datetime.fromisoformat(str(ms).replace("Z","+00:00")).timestamp()*1000)
                            except Exception:
                                ms = start_ms
                        date_iso = datetime.utcfromtimestamp(ms/1000.0).isoformat()
                        out.append({"id": oid, "date": date_iso, "customer": attrs.get("customer")})
                        if max_orders and len(out) >= max_orders:
                            return out
                    break
                except Exception:
                    if try_field != "creationDate":
                        try_field = "creationDate"
                        continue
                    raise
            cur = nxt + timedelta(milliseconds=1)
        return out

    # HTTPX прямой (если нет SDK)
    headers = _kaspi_headers()
    timeout = httpx.Timeout(20.0, connect=10.0)
    async with httpx.AsyncClient(base_url=KASPI_BASE_URL, timeout=timeout) as cli:
        page = 0
        while True:
            params = {
                "page[number]": str(page),
                "page[size]": "100",
                f"filter[{date_field or 'creationDate'}][ge]": str(start_ms),
                f"filter[{date_field or 'creationDate'}][le]": str(end_ms),
            }
            r = await cli.get("/orders", params=params, headers=headers)
            r.raise_for_status()
            j = r.json()
            data = j.get("data", []) or []
            if not data:
                break
            for d in data:
                oid = str(d.get("id"))
                attrs = d.get("attributes", {}) or {}
                st = _norm_state(attrs.get("state", ""))
                if inc_states and st not in inc_states:
                    continue
                if exc_states and st in exc_states:
                    continue
                ms = attrs.get(date_field or "creationDate") or start_ms
                try:
                    ms = int(ms)
                except Exception:
                    try:
                        ms = int(datetime.fromisoformat(str(ms).replace("Z","+00:00")).timestamp()*1000)
                    except Exception:
                        ms = start_ms
                date_iso = datetime.utcfromtimestamp(ms/1000.0).isoformat()
                out.append({"id": oid, "date": date_iso, "customer": attrs.get("customer")})
                if max_orders and len(out) >= max_orders:
                    return out
            page += 1

    return out

# ---------- запись в БД ----------
def _upsert_order_with_items(o: OrderIn) -> Tuple[int, int]:
    _ensure_schema()
    ins_o = ins_i = 0
    with _db() as c:
        # upsert order
        if _USE_PG:
            existed = c.execute(_q("SELECT 1 FROM orders WHERE id=:id"), {"id": o.id}).first()
            c.execute(_q("""
                INSERT INTO orders(id,date,customer)
                VALUES(:id,:date,:customer)
                ON CONFLICT (id) DO UPDATE SET date=EXCLUDED.date, customer=EXCLUDED.customer
            """), {"id": o.id, "date": o.date, "customer": o.customer})
        else:
            existed = c.execute("SELECT 1 FROM orders WHERE id=?", (o.id,)).fetchone()
            c.execute("""
                INSERT INTO orders(id,date,customer) VALUES(?,?,?)
                ON CONFLICT(id) DO UPDATE SET date=excluded.date, customer=excluded.customer
            """, (o.id, o.date, o.customer))
        ins_o += 0 if existed else 1

        # replace items for this order in order_items AND sales
        if _USE_PG:
            c.execute(_q("DELETE FROM order_items WHERE order_id=:id"), {"id": o.id})
            c.execute(_q("DELETE FROM sales       WHERE order_id=:id"), {"id": o.id})
        else:
            c.execute("DELETE FROM order_items WHERE order_id=?", (o.id,))
            c.execute("DELETE FROM sales       WHERE order_id=?", (o.id,))

        for it in o.items:
            # order_items
            if _USE_PG:
                c.execute(_q("""
                    INSERT INTO order_items(order_id,sku,qty,unit_price,commission_pct)
                    VALUES(:oid,:sku,:qty,:p,:comm)
                """), {
                    "oid": o.id, "sku": it.sku.strip(),
                    "qty": int(it.qty), "p": float(it.unit_price),
                    "comm": float(it.commission_pct) if it.commission_pct is not None else None
                })
            else:
                c.execute("""
                    INSERT INTO order_items(order_id,sku,qty,unit_price,commission_pct)
                    VALUES(?,?,?,?,?)
                """, (
                    o.id, it.sku.strip(), int(it.qty),
                    float(it.unit_price),
                    float(it.commission_pct) if it.commission_pct is not None else None
                ))
            # sales (для profit_fifo)
            if _USE_PG:
                c.execute(_q("""
                    INSERT INTO sales(order_id,date,sku,qty,unit_price,commission_pct)
                    VALUES(:oid,:date,:sku,:qty,:p,:comm)
                """), {
                    "oid": o.id, "date": o.date, "sku": it.sku.strip(),
                    "qty": int(it.qty), "p": float(it.unit_price),
                    "comm": float(it.commission_pct) if it.commission_pct is not None else None
                })
            else:
                c.execute("""
                    INSERT INTO sales(order_id,date,sku,qty,unit_price,commission_pct)
                    VALUES(?,?,?,?,?,?)
                """, (
                    o.id, o.date, it.sku.strip(), int(it.qty),
                    float(it.unit_price),
                    float(it.commission_pct) if it.commission_pct is not None else None
                ))
            ins_i += 1
    return ins_o, ins_i

# ---------- Router ----------
router = APIRouter(tags=["profit-bridge"])

@router.get("/ping")
@router.get("/bridge/ping")
async def ping_bridge():
    _ensure_schema()
    return {"ok": True, "driver": "pg" if _USE_PG else "sqlite"}

@router.get("/diag")
@router.get("/bridge/diag")
async def bridge_diag(
    order_id: Optional[str] = Query(None, description="конкретный ID заказа"),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
    tz: str = Query("Asia/Almaty"),
    date_field: str = Query("creationDate"),
    use_bd: bool = Query(False),
    business_day_start: str = Query("20:00"),
    _auth: bool = Depends(require_api_key),
):
    """Диагностика выборки позиций заказа."""
    _ensure_schema()
    checked_id = order_id
    orders: List[Dict[str, Any]] = []

    if not checked_id:
        if not (start and end):
            raise HTTPException(status_code=400, detail="Provide order_id OR (start & end)")
        s_ms, e_ms = _build_window(start, end, tz, use_bd, business_day_start)
        orders = await _iter_orders(s_ms, e_ms, tz, date_field, None, None, max_orders=1)
        if not orders:
            return {"orders_found": 0, "note": "no orders in range"}
        checked_id = str(orders[0]["id"])

    items, dbg = await _fetch_entries_httpx(checked_id)
    return {
        "order_id": checked_id,
        "items": items,
        "debug": dbg,
        "hint": "если statuses 401/403/404 — у токена нет доступа к позициям (orderentries).",
    }

@router.get("/db-stats")
@router.get("/bridge/db-stats")
async def db_stats(_auth: bool = Depends(require_api_key)):
    _ensure_schema()
    with _db() as c:
        if _USE_PG:
            o = c.execute(_q("SELECT COUNT(*) AS n FROM orders")).first().n
            oi = c.execute(_q("SELECT COUNT(*) AS n FROM order_items")).first().n
            s = c.execute(_q("SELECT COUNT(*) AS n FROM sales")).first().n
            last = _rows(c.execute(_q("SELECT order_id, date, sku, qty, unit_price FROM sales ORDER BY date DESC LIMIT 10")))
        else:
            o = c.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
            oi = c.execute("SELECT COUNT(*) FROM order_items").fetchone()[0]
            s = c.execute("SELECT COUNT(*) FROM sales").fetchone()[0]
            last = [dict(r) for r in c.execute("SELECT order_id, date, sku, qty, unit_price FROM sales ORDER BY date DESC LIMIT 10").fetchall()]
    return {"orders": o, "order_items": oi, "sales": s, "last_sales": last}

# Принимаем и POST, и GET — проще дебажить из браузера
@router.api_route("/sync", methods=["POST", "GET"])
@router.api_route("/bridge/sync", methods=["POST", "GET"])
async def profit_sync(
    start: str = Query(..., description="YYYY-MM-DD"),
    end: str   = Query(..., description="YYYY-MM-DD"),
    tz: str = Query("Asia/Almaty"),
    date_field: str = Query("creationDate"),
    states: Optional[str] = Query(None, description="CSV включаемых статусов"),
    exclude_states: Optional[str] = Query(None, description="CSV исключаемых статусов"),
    use_bd: Optional[bool] = Query(False),
    business_day_start: Optional[str] = Query("20:00"),
    max_orders: Optional[int] = Query(None, description="ограничение для отладки"),
    _auth: bool = Depends(require_api_key)
):
    """
    Синхронизируем: тянем заказы из Kaspi, извлекаем позиции, пишем в sales (и order_items).
    """
    if not KASPI_TOKEN:
        raise HTTPException(status_code=500, detail="KASPI_TOKEN is not set")

    inc = _parse_states_csv(states)
    exc = _parse_states_csv(exclude_states)

    s_ms, e_ms = _build_window(start, end, tz, bool(use_bd), business_day_start or "20:00")

    orders = await _iter_orders(s_ms, e_ms, tz, date_field, inc, exc, max_orders=max_orders)
    if not orders:
        return {"status": "ok", "synced_orders": 0, "items_inserted": 0, "skipped": 0}

    total_o = total_i = skipped = 0
    for od in orders:
        oid = str(od["id"])
        items, dbg = await _fetch_entries_httpx(oid)
        if not items:
            skipped += 1
            # можно вернуть debug по первому пустому — полезно в логах фронта
            continue
        o = OrderIn(
            id=oid,
            date=od["date"],
            customer=od.get("customer"),
            items=[OrderItemIn(sku=i["sku"], qty=int(i["qty"]), unit_price=float(i["unit_price"])) for i in items]
        )
        io, ii = _upsert_order_with_items(o)
        total_o += io
        total_i += ii

    return {
        "status": "ok",
        "synced_orders": total_o,
        "items_inserted": total_i,
        "skipped": skipped
    }

def get_profit_bridge_router() -> APIRouter:
    return router
