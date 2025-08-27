# app/api/profit_bridge.py
from __future__ import annotations

"""
Profit Bridge:
- Тянет заказы из Kaspi по периоду
- По каждому заказу тянет позиции (SKU, qty, price)
- Складывает в локальную БД (orders, order_items) + плоскую таблицу sales
- Делает это ПАРАЛЛЕЛЬНО с ограничением по одновременным запросам
- Даёт служебную диагностику

Подключается из main.py так:
    app.include_router(get_profit_bridge_router(), prefix="/profit")

Фронт может дергать:
    POST/GET /profit/sync                (или /profit/bridge/sync)
    GET       /profit/bridge/ping
    GET       /profit/bridge/diag
    GET       /profit/bridge/db-stats
"""

import os
import asyncio
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timedelta, time as dt_time
from contextlib import contextmanager

import pytz
from fastapi import APIRouter, HTTPException, Query, Depends, Request
from pydantic import BaseModel

# ---------- Настройки параллелизма/таймаутов ----------
BRIDGE_CONCURRENCY = int(os.getenv("BRIDGE_CONCURRENCY", "8"))         # сколько запросов к Kaspi одновременно
BRIDGE_HTTP_TIMEOUT = float(os.getenv("BRIDGE_HTTP_TIMEOUT", "12"))     # сек. общий таймаут httpx

# ---------- DB (PG/SQLite) ----------
try:
    from sqlalchemy import create_engine, text
    _SQLA_OK = True
except Exception:
    _SQLA_OK = False

import sqlite3

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

def _q(sql: str):
    return text(sql) if _USE_PG else sql

def _rows(rows):
    return [dict(r._mapping) for r in rows] if _USE_PG else [dict(r) for r in rows]

# ---------- Auth (как на фронте, но терпим к <...>,"...",'...') ----------
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
    }

# ---------- Утилиты времени ----------
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
    """Вернёт (start_ms_utc, end_ms_utc) для фильтра Kaspi."""
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

# ---------- Схема (добавили sales!) ----------
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
    # 1) attributes.code / productCode / sku
    attrs = entry.get("attributes", {}) or {}
    sku = _get_str(attrs, ["code", "productCode", "sku"])
    if sku:
        return sku
    # 2) relationships → product/masterProduct/sku
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
    timeout = httpx.Timeout(BRIDGE_HTTP_TIMEOUT)
    async with httpx.AsyncClient(base_url=KASPI_BASE_URL, timeout=timeout) as cli:
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

        # 2) /orders/{id}?include=entries.product
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
async def _iter_orders(start_ms: int, end_ms: int, tz: str, date_field: str,
                       inc_states: Optional[set[str]], exc_states: Optional[set[str]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    # 1) если есть ваш SDK — используем его
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
                                ms = int(datetime.fromisoformat(str(ms).replace("Z", "+00:00")).timestamp() * 1000)
                            except Exception:
                                ms = start_ms
                        date_iso = datetime.utcfromtimestamp(ms / 1000.0).isoformat()
                        out.append({"id": oid, "date": date_iso, "customer": attrs.get("customer")})
                    break
                except Exception:
                    if try_field != "creationDate":
                        try_field = "creationDate"
                        continue
                    raise
            cur = nxt + timedelta(milliseconds=1)
        return out

    # 2) httpx напрямую
    headers = _kaspi_headers()
    timeout = httpx.Timeout(BRIDGE_HTTP_TIMEOUT)
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
                        ms = int(datetime.fromisoformat(str(ms).replace("Z", "+00:00")).timestamp() * 1000)
                    except Exception:
                        ms = start_ms
                date_iso = datetime.utcfromtimestamp(ms / 1000.0).isoformat()
                out.append({"id": oid, "date": date_iso, "customer": attrs.get("customer")})
            page += 1

    return out

# ---------- запись в БД (включая sales) ----------
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

        # полная замена позиций заказа
        if _USE_PG:
            c.execute(_q("DELETE FROM order_items WHERE order_id=:id"), {"id": o.id})
            c.execute(_q("DELETE FROM sales       WHERE order_id=:id"), {"id": o.id})
        else:
            c.execute("DELETE FROM order_items WHERE order_id=?", (o.id,))
            c.execute("DELETE FROM sales       WHERE order_id=?", (o.id,))

        for it in o.items:
            if _USE_PG:
                c.execute(_q("""
                    INSERT INTO order_items(order_id,sku,qty,unit_price,commission_pct)
                    VALUES(:oid,:sku,:qty,:p,:comm)
                """), {
                    "oid": o.id, "sku": it.sku.strip(),
                    "qty": int(it.qty), "p": float(it.unit_price),
                    "comm": float(it.commission_pct) if it.commission_pct is not None else None
                })
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
                    INSERT INTO order_items(order_id,sku,qty,unit_price,commission_pct)
                    VALUES(?,?,?,?,?)
                """, (
                    o.id, it.sku.strip(), int(it.qty),
                    float(it.unit_price),
                    float(it.commission_pct) if it.commission_pct is not None else None
                ))
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

# Диагностика: сколько заказов попадёт в выборку (без вытягивания позиций)
@router.get("/bridge/diag")
async def diag_bridge(
    start: str = Query(...), end: str = Query(...), tz: str = Query("Asia/Almaty"),
    date_field: str = Query("creationDate"),
    states: Optional[str] = Query(None), exclude_states: Optional[str] = Query(None),
    use_bd: Optional[bool] = Query(False), business_day_start: Optional[str] = Query("20:00"),
    _auth: bool = Depends(require_api_key),
):
    s_ms, e_ms = _build_window(start, end, tz, bool(use_bd), business_day_start or "20:00")
    orders = await _iter_orders(s_ms, e_ms, tz, date_field, _parse_states_csv(states), _parse_states_csv(exclude_states))
    sample = orders[:5]
    return {"orders_found": len(orders), "sample": sample}

# Быстрые статистики по БД (для проверки, что записи идут в sales)
@router.get("/bridge/db-stats")
async def db_stats(_auth: bool = Depends(require_api_key)):
    _ensure_schema()
    with _db() as c:
        if _USE_PG:
            ro = c.execute(_q("SELECT COUNT(*) AS n FROM orders")).first()
            ri = c.execute(_q("SELECT COUNT(*) AS n FROM order_items")).first()
            rs = c.execute(_q("SELECT COUNT(*) AS n FROM sales")).first()
            return {"orders": ro[0] if ro else 0, "order_items": ri[0] if ri else 0, "sales": rs[0] if rs else 0}
        else:
            ro = c.execute("SELECT COUNT(*) AS n FROM orders").fetchone()
            ri = c.execute("SELECT COUNT(*) AS n FROM order_items").fetchone()
            rs = c.execute("SELECT COUNT(*) AS n FROM sales").fetchone()
            return {"orders": ro["n"] if ro else 0, "order_items": ri["n"] if ri else 0, "sales": rs["n"] if rs else 0}

# Принимаем и POST, и GET — удобнее для дебага из браузера
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
    max_orders: Optional[int] = Query(None, ge=1, le=2000),  # можно ограничить на время теста
    _auth: bool = Depends(require_api_key)
):
    """
    Синхронизируем: тянем заказы из Kaspi, извлекаем позиции ПАРАЛЛЕЛЬНО (ограничение BRIDGE_CONCURRENCY),
    пишем в sales (и order_items).
    """
    if not KASPI_TOKEN:
        raise HTTPException(status_code=500, detail="KASPI_TOKEN is not set")

    inc = _parse_states_csv(states)
    exc = _parse_states_csv(exclude_states)

    s_ms, e_ms = _build_window(start, end, tz, bool(use_bd), business_day_start or "20:00")
    orders = await _iter_orders(s_ms, e_ms, tz, date_field, inc, exc)
    if not orders:
        return {"status": "ok", "synced_orders": 0, "items_inserted": 0, "skipped": 0}

    if max_orders:
        orders = orders[:max_orders]

    sem = asyncio.Semaphore(BRIDGE_CONCURRENCY)

    async def process_one(od: Dict[str, Any]) -> Tuple[int, int]:
        oid = str(od["id"])
        async with sem:
            items = await _fetch_entries_httpx(oid)
        if not items:
            return (0, 0)
        o = OrderIn(
            id=oid,
            date=od["date"],
            customer=od.get("customer"),
            items=[OrderItemIn(sku=i["sku"], qty=int(i["qty"]), unit_price=float(i["unit_price"])) for i in items]
        )
        io, ii = _upsert_order_with_items(o)
        return (io, ii)

    results = await asyncio.gather(*[process_one(od) for od in orders], return_exceptions=True)

    total_o = total_i = 0
    skipped = 0
    for r in results:
        if isinstance(r, Exception):
            skipped += 1
        else:
            io, ii = r
            total_o += io
            total_i += ii

    return {"status": "ok", "synced_orders": total_o, "items_inserted": total_i, "skipped": skipped}

def get_profit_bridge_router() -> APIRouter:
    """Фабрика для include_router(get_profit_bridge_router(), prefix='/profit')"""
    return router
