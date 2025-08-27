# app/api/profit_bridge.py
from __future__ import annotations

import os
import json
import sqlite3
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timedelta, time as dt_time
from contextlib import contextmanager

import pytz
import httpx
from fastapi import APIRouter, HTTPException, Query, Depends, Request
from pydantic import BaseModel

# ---------- DB (PG/SQLite) ----------
try:
    from sqlalchemy import create_engine, text  # type: ignore
    _SQLA_OK = True
except Exception:
    _SQLA_OK = False

DATABASE_URL = os.getenv("DATABASE_URL")
_USE_PG = bool(DATABASE_URL and _SQLA_OK)
if _USE_PG:
    _engine = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)  # type: ignore

def _resolve_db_path() -> str:
    target = os.getenv("DB_PATH", "/data/kaspi-orders.sqlite3")
    os.makedirs(os.path.dirname(target), exist_ok=True)
    return target

DB_PATH = _resolve_db_path()

@contextmanager
def _db():
    if _USE_PG:
        with _engine.begin() as conn:  # type: ignore
            yield conn
    else:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

def _q(sql: str): return text(sql) if _USE_PG else sql  # type: ignore
def _rows(rows): return [dict(r._mapping) for r in rows] if _USE_PG else [dict(r) for r in rows]

# ---------- Auth (как на фронте) ----------
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

def _kaspi_headers() -> Dict[str, str]:
    if not KASPI_TOKEN:
        raise HTTPException(status_code=500, detail="KASPI_TOKEN is not set")
    return {
        "X-Auth-Token": KASPI_TOKEN,
        "Accept": "application/vnd.api+json",
    }

# ---------- Утилиты времени/парсинга ----------
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

def _preview(obj: Any, limit: int = 600) -> str:
    try:
        s = json.dumps(obj) if not isinstance(obj, (str, bytes)) else (obj.decode() if isinstance(obj, bytes) else obj)
        return (s[:limit] + ("…" if len(s) > limit else ""))
    except Exception:
        return "<unrepr>"

# ---------- Схема (включая sales) ----------
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

# ---------- Модели ----------
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

# ---------- Список заказов Kaspi ----------
async def _iter_orders(start_ms: int, end_ms: int, tz: str, date_field: str,
                       inc_states: Optional[set[str]], exc_states: Optional[set[str]]) -> List[Dict[str, Any]]:
    """
    Возвращает элементы вида: {"id","date","number","customer"}
    """
    out: List[Dict[str, Any]] = []
    headers = _kaspi_headers()
    timeout = httpx.Timeout(connect=10.0, read=60.0, write=30.0, pool=60.0)
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
                        ms = int(datetime.fromisoformat(str(ms).replace("Z","+00:00")).timestamp() * 1000)
                    except Exception:
                        ms = start_ms
                date_iso = datetime.utcfromtimestamp(ms / 1000.0).isoformat()
                number = _get_str(attrs, ["code", "orderNumber", "number", "id"], "")
                out.append({"id": oid, "date": date_iso, "customer": attrs.get("customer"), "number": number})
            page += 1
    return out

# ---------- Получение позиций (5 стратегий + подробный debug) ----------
async def _fetch_entries_httpx(order_id: str, order_number: Optional[str], debug: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Возвращаем список позиций: [{"sku","qty","unit_price"}...]
    Стратегии:
      A)  /orderentries?filter[order.id]=<id>
      A2) /orderentries?filter[orders.id]=<id>
      C)  /orders/{id}/entries?include=product
      B)  /orders/{id}?include=entries.product
      D)  /orders?filter[code]=<number>&include=entries.product
    """
    headers = _kaspi_headers()
    timeout = httpx.Timeout(connect=10.0, read=60.0, write=30.0, pool=60.0)
    items: List[Dict[str, Any]] = []

    def _extract_from_entry(entry: dict, included_map: Dict[str, dict]) -> Optional[Dict[str, Any]]:
        attrs = entry.get("attributes", {}) or {}
        qty = _get_int(attrs, ["quantity", "qty", "count"], 1)
        price = _get_num(attrs, ["unitPrice", "basePrice", "price"], 0.0)
        sku = _get_str(attrs, ["code", "productCode", "sku"], "")
        if not sku:
            rel = entry.get("relationships", {}) or {}
            node = rel.get("product", {}) or rel.get("masterProduct", {})
            data_node = node.get("data")
            if isinstance(data_node, dict):
                rid = data_node.get("id")
                a2 = included_map.get(rid, {}).get("attributes", {}) if rid else {}
                sku = _get_str(a2, ["code", "sku", "productCode"], "")
        return {"sku": sku, "qty": qty, "unit_price": price} if sku else None

    async with httpx.AsyncClient(base_url=KASPI_BASE_URL, timeout=timeout) as cli:
        # ---- A) orderentries: filter[order.id]
        try:
            params = {"filter[order.id]": order_id, "include": "product", "page[size]": "200"}
            r = await cli.get("/orderentries", params=params, headers=headers)
            debug["A_status"] = r.status_code
            j = {}
            try: j = r.json()
            except Exception: j = {"raw": (await r.aread())[:400]}
            debug["A_preview"] = _preview(j)
            if r.status_code == 200 and isinstance(j, dict):
                data = j.get("data", []) or []
                included = {x["id"]: x for x in (j.get("included", []) or []) if isinstance(x, dict) and "id" in x}
                for e in data:
                    it = _extract_from_entry(e, included)
                    if it: items.append(it)
            if items:
                debug["A_count"] = len(items)
                return items
            debug["A_count"] = 0
        except httpx.HTTPError as e:
            debug["A_error"] = repr(e)

        # ---- A2) orderentries: filter[orders.id]  (встречается у некоторых)
        try:
            params = {"filter[orders.id]": order_id, "include": "product", "page[size]": "200"}
            r = await cli.get("/orderentries", params=params, headers=headers)
            debug["A2_status"] = r.status_code
            j = {}
            try: j = r.json()
            except Exception: j = {"raw": (await r.aread())[:400]}
            debug["A2_preview"] = _preview(j)
            if r.status_code == 200 and isinstance(j, dict):
                data = j.get("data", []) or []
                included = {x["id"]: x for x in (j.get("included", []) or []) if isinstance(x, dict) and "id" in x}
                for e in data:
                    it = _extract_from_entry(e, included)
                    if it: items.append(it)
            if items:
                debug["A2_count"] = len(items)
                return items
            debug["A2_count"] = 0
        except httpx.HTTPError as e:
            debug["A2_error"] = repr(e)

        # ---- C) сабресурс: /orders/{id}/entries?include=product
        try:
            params = {"include": "product", "page[size]": "200"}
            r = await cli.get(f"/orders/{order_id}/entries", params=params, headers=headers)
            debug["C_status"] = r.status_code
            j = {}
            try: j = r.json()
            except Exception: j = {"raw": (await r.aread())[:400]}
            debug["C_preview"] = _preview(j)
            if r.status_code == 200 and isinstance(j, dict):
                data = j.get("data", []) or []
                included = {x["id"]: x for x in (j.get("included", []) or []) if isinstance(x, dict) and "id" in x}
                for e in data:
                    it = _extract_from_entry(e, included)
                    if it: items.append(it)
            if items:
                debug["C_count"] = len(items)
                return items
            debug["C_count"] = 0
        except httpx.HTTPError as e:
            debug["C_error"] = repr(e)

        # ---- B) /orders/{id}?include=entries.product
        try:
            params = {"include": "entries.product"}
            r = await cli.get(f"/orders/{order_id}", params=params, headers=headers)
            debug["B_status"] = r.status_code
            j = {}
            try: j = r.json()
            except Exception: j = {"raw": (await r.aread())[:400]}
            debug["B_preview"] = _preview(j)
            if r.status_code == 200 and isinstance(j, dict):
                included = j.get("included", []) or []
                incl_map = {x["id"]: x for x in included if isinstance(x, dict) and "id" in x}
                for inc in included:
                    if "entry" not in str(inc.get("type", "")).lower():
                        continue
                    it = _extract_from_entry(inc, incl_map)
                    if it: items.append(it)
            if items:
                debug["B_count"] = len(items)
                return items
            debug["B_count"] = 0
        except httpx.HTTPError as e:
            debug["B_error"] = repr(e)

        # ---- D) /orders?filter[code]=<number>&include=entries.product
        if order_number:
            try:
                params = {"include": "entries.product", "page[size]": "1", "filter[code]": order_number}
                r = await cli.get("/orders", params=params, headers=headers)
                debug["D_status"] = r.status_code
                j = {}
                try: j = r.json()
                except Exception: j = {"raw": (await r.aread())[:400]}
                debug["D_preview"] = _preview(j)
                if r.status_code == 200 and isinstance(j, dict):
                    included = j.get("included", []) or []
                    incl_map = {x["id"]: x for x in included if isinstance(x, dict) and "id" in x}
                    for inc in included:
                        if "entry" not in str(inc.get("type", "")).lower():
                            continue
                        it = _extract_from_entry(inc, incl_map)
                        if it: items.append(it)
                debug["D_count"] = len(items)
                if items:
                    return items
            except httpx.HTTPError as e:
                debug["D_error"] = repr(e)

    return items

# ---------- Запись в БД ----------
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

        # replace items for this order in both tables
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
                """, (o.id, it.sku.strip(), int(it.qty), float(it.unit_price),
                      float(it.commission_pct) if it.commission_pct is not None else None))
            # sales
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
                """, (o.id, o.date, it.sku.strip(), int(it.qty),
                      float(it.unit_price),
                      float(it.commission_pct) if it.commission_pct is not None else None))
            ins_i += 1
    return ins_o, ins_i

# ---------- Router ----------
router = APIRouter(tags=["profit-bridge"])

@router.get("/bridge/ping")
@router.get("/ping")
async def ping_bridge():
    _ensure_schema()
    return {"ok": True, "driver": "pg" if _USE_PG else "sqlite"}

@router.get("/bridge/db-stats")
@router.get("/db-stats")
async def db_stats(_auth: bool = Depends(require_api_key)):
    _ensure_schema()
    with _db() as c:
        if _USE_PG:
            orders = c.execute(_q("SELECT COUNT(*) AS n FROM orders")).scalar_one()
            items  = c.execute(_q("SELECT COUNT(*) AS n FROM order_items")).scalar_one()
            sales  = c.execute(_q("SELECT COUNT(*) AS n FROM sales")).scalar_one()
            last   = _rows(c.execute(_q("SELECT * FROM sales ORDER BY date DESC LIMIT 10")))
        else:
            orders = c.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
            items  = c.execute("SELECT COUNT(*) FROM order_items").fetchone()[0]
            sales  = c.execute("SELECT COUNT(*) FROM sales").fetchone()[0]
            last   = [dict(r) for r in c.execute("SELECT * FROM sales ORDER BY date DESC LIMIT 10").fetchall()]
    return {"orders": orders, "order_items": items, "sales": sales, "last_sales": last}

# Диагностика получения позиций по одному заказу
@router.get("/bridge/diag")
@router.get("/diag")
async def diag_bridge(order_id: Optional[str] = Query(None),
                      start: Optional[str] = Query(None),
                      end: Optional[str] = Query(None),
                      tz: str = Query("Asia/Almaty"),
                      date_field: str = Query("creationDate"),
                      use_bd: bool = Query(False),
                      business_day_start: str = Query("20:00"),
                      _auth: bool = Depends(require_api_key)):
    debug: Dict[str, Any] = {}
    order_number: Optional[str] = None
    if not order_id:
        if not (start and end):
            raise HTTPException(400, "Provide order_id OR (start & end)")
        s_ms, e_ms = _build_window(start, end, tz, use_bd, business_day_start or "20:00")
        orders = await _iter_orders(s_ms, e_ms, tz, date_field, None, None)
        if not orders:
            return {"order_id": None, "items": [], "debug": {"msg": "no orders in period"}}
        cand = orders[0]
        order_id = cand["id"]
        order_number = cand.get("number") or None

    items = await _fetch_entries_httpx(order_id, order_number, debug)
    return {"order_id": order_id, "items": items, "debug": debug,
            "hint": "если статус 401/403/404 — проверь права токена на orderentries / include entries"}

# Синхронизация периода в локальную БД (sales + order_items)
@router.api_route("/bridge/sync", methods=["POST", "GET"])
@router.api_route("/sync", methods=["POST", "GET"])
async def profit_sync(
    start: str = Query(..., description="YYYY-MM-DD"),
    end: str   = Query(..., description="YYYY-MM-DD"),
    tz: str = Query("Asia/Almaty"),
    date_field: str = Query("creationDate"),
    states: Optional[str] = Query(None, description="CSV включаемых статусов"),
    exclude_states: Optional[str] = Query(None, description="CSV исключаемых статусов"),
    use_bd: Optional[bool] = Query(False),
    business_day_start: Optional[str] = Query("20:00"),
    max_orders: int = Query(100),
    _auth: bool = Depends(require_api_key)
):
    if not KASPI_TOKEN:
        raise HTTPException(status_code=500, detail="KASPI_TOKEN is not set")

    inc = _parse_states_csv(states)
    exc = _parse_states_csv(exclude_states)

    s_ms, e_ms = _build_window(start, end, tz, bool(use_bd), business_day_start or "20:00")
    orders = await _iter_orders(s_ms, e_ms, tz, date_field, inc, exc)
    if not orders:
        return {"status": "ok", "synced_orders": 0, "items_inserted": 0, "skipped": 0, "skipped_timeouts": 0, "skipped_errors": 0}

    total_o = total_i = skipped = skipped_timeouts = skipped_errors = 0
    for od in orders[:max_orders]:
        oid = str(od["id"])
        onum = od.get("number") or None
        dbg: Dict[str, Any] = {}
        try:
            items = await _fetch_entries_httpx(oid, onum, dbg)
        except httpx.ReadTimeout:
            skipped_timeouts += 1
            continue
        except Exception:
            skipped_errors += 1
            continue

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

    return {
        "status": "ok",
        "synced_orders": total_o,
        "items_inserted": total_i,
        "skipped": skipped,
        "skipped_timeouts": skipped_timeouts,
        "skipped_errors": skipped_errors
    }

def get_profit_bridge_router() -> APIRouter:
    return router
