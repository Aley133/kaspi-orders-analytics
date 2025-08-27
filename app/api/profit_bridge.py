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
    got = got.strip().strip('<>').strip('"').strip("'")
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
    }

# ---------- Время (точь-в-точь логика из main.py) ----------
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

def _build_window_dt(start: str, end: str, tz: str, use_bd: bool, bd_start: str) -> Tuple[datetime, datetime]:
    """Вернём локальные TZ-aware границы периода (как в main.py)."""
    z = _tzinfo(tz)
    s0 = _parse_date_local(start, tz)
    e0 = _parse_date_local(end, tz) + timedelta(days=1) - timedelta(milliseconds=1)
    if use_bd:
        delta = _bd_delta(bd_start or "20:00")
        s = z.localize(datetime.combine((s0.date() - timedelta(days=1)), dt_time(0, 0))) + delta
        e = z.localize(datetime.combine(e0.date(), dt_time(0, 0))) + delta - timedelta(milliseconds=1)
    else:
        s, e = s0, e0
    return s, e

def _to_ms_utc(dt: datetime) -> int:
    return int(dt.astimezone(pytz.UTC).timestamp() * 1000)

# ---------- Схема (есть и sales) ----------
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

# ---------- модели ----------
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
    return {_norm_state(x) for x in s.replace(";", ",").split(",") if x.strip()}

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
async def _fetch_entries_httpx(order_id: str) -> List[Dict[str, Any]]:
    headers = _kaspi_headers()
    async with httpx.AsyncClient(base_url=KASPI_BASE_URL, timeout=30.0) as cli:
        # 1) /orderentries?filter[order.id]=...
        try:
            params = {"filter[order.id]": order_id, "include": "product", "page[size]": "200"}
            r = await cli.get("/orderentries", params=params, headers=headers)
            r.raise_for_status()
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
                return out
        except httpx.HTTPError:
            pass

        # 2) fallback: /orders/{id}?include=entries.product
        try:
            params = {"include": "entries.product"}
            r = await cli.get(f"/orders/{order_id}", params=params, headers=headers)
            r.raise_for_status()
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
            return out
        except httpx.HTTPError:
            pass

    return []

# ---------- чтение списка заказов ----------
async def _iter_orders(start_dt: datetime, end_dt: datetime, tz: str, date_field: str,
                       inc_states: Optional[set[str]], exc_states: Optional[set[str]],
                       max_orders: int = 0) -> List[Dict[str, Any]]:
    """start_dt/end_dt — TZ-aware локальные границы!"""
    out: List[Dict[str, Any]] = []

    if KaspiClient:
        cli = KaspiClient(token=KASPI_TOKEN, base_url=KASPI_BASE_URL)
        step = timedelta(days=7)
        cur = start_dt
        while cur <= end_dt:
            nxt = min(cur + step - timedelta(milliseconds=1), end_dt)
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
                        ms = attrs.get(try_field) or attrs.get("creationDate") or _to_ms_utc(start_dt)
                        try:
                            ms = int(ms)
                        except Exception:
                            try:
                                ms = int(datetime.fromisoformat(str(ms).replace("Z","+00:00")).timestamp()*1000)
                            except Exception:
                                ms = _to_ms_utc(start_dt)
                        date_iso = datetime.fromtimestamp(ms/1000.0, tz=pytz.UTC).astimezone(_tzinfo(tz)).isoformat()
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

    # httpx
    headers = _kaspi_headers()
    start_ms = _to_ms_utc(start_dt)
    end_ms   = _to_ms_utc(end_dt)
    async with httpx.AsyncClient(base_url=KASPI_BASE_URL, timeout=30.0) as cli:
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
                date_iso = datetime.fromtimestamp(ms/1000.0, tz=pytz.UTC).astimezone(_tzinfo(tz)).isoformat()
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

# Диагностика: что видим у Kaspi по заданному периоду
@router.get("/diag")
@router.get("/bridge/diag")
async def diag(
    start: str = Query(...),
    end: str = Query(...),
    tz: str = Query("Asia/Almaty"),
    date_field: str = Query("creationDate"),
    states: Optional[str] = Query(None),
    exclude_states: Optional[str] = Query(None),
    use_bd: bool = Query(False),
    business_day_start: str = Query("20:00"),
    sample: int = Query(3, ge=0, le=20),
    max_orders: int = Query(20, ge=0, le=200),
    _auth: bool = Depends(require_api_key)
):
    sdt, edt = _build_window_dt(start, end, tz, use_bd, business_day_start)
    inc = _parse_states_csv(states)
    exc = _parse_states_csv(exclude_states)
    orders = await _iter_orders(sdt, edt, tz, date_field, inc, exc, max_orders=max_orders)
    return {
        "window": {
            "start_local": sdt.isoformat(),
            "end_local": edt.isoformat(),
            "start_ms_utc": _to_ms_utc(sdt),
            "end_ms_utc": _to_ms_utc(edt),
        },
        "found": len(orders),
        "orders_preview": orders[:sample] if sample else [],
    }

# Статистика по локальной БД
@router.get("/db-stats")
@router.get("/bridge/db-stats")
async def db_stats(_auth: bool = Depends(require_api_key)):
    _ensure_schema()
    with _db() as c:
        if _USE_PG:
            n_orders = c.execute(_q("SELECT COUNT(*) AS n FROM orders")).scalar() or 0
            n_items  = c.execute(_q("SELECT COUNT(*) AS n FROM order_items")).scalar() or 0
            n_sales  = c.execute(_q("SELECT COUNT(*) AS n FROM sales")).scalar() or 0
            last_sales = _rows(c.execute(_q("SELECT * FROM sales ORDER BY id DESC LIMIT 5")))
        else:
            n_orders = c.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
            n_items  = c.execute("SELECT COUNT(*) FROM order_items").fetchone()[0]
            n_sales  = c.execute("SELECT COUNT(*) FROM sales").fetchone()[0]
            last_sales = [dict(r) for r in c.execute("SELECT * FROM sales ORDER BY id DESC LIMIT 5").fetchall()]
    return {"orders": n_orders, "order_items": n_items, "sales": n_sales, "last_sales": last_sales}

# Принимаем и POST, и GET (удобно для ручного прогона из браузера)
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
    max_orders: int = Query(0, ge=0, le=10000),
    _auth: bool = Depends(require_api_key)
):
    """
    Синхронизируем: тянем заказы из Kaspi, извлекаем позиции, пишем в sales (и order_items).
    """
    if not KASPI_TOKEN:
        raise HTTPException(status_code=500, detail="KASPI_TOKEN is not set")

    sdt, edt = _build_window_dt(start, end, tz, bool(use_bd), business_day_start or "20:00")
    inc = _parse_states_csv(states)
    exc = _parse_states_csv(exclude_states)

    orders = await _iter_orders(sdt, edt, tz, date_field, inc, exc, max_orders=max_orders)
    if not orders:
        return {"status": "ok", "synced_orders": 0, "items_inserted": 0, "skipped": 0}

    total_o = total_i = skipped = 0
    for od in orders:
        oid = str(od["id"])
        items = await _fetch_entries_httpx(oid)
        if not items:
            skipped += 1
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

    return {"status": "ok", "synced_orders": total_o, "items_inserted": total_i, "skipped": skipped}

def get_profit_bridge_router() -> APIRouter:
    return router
