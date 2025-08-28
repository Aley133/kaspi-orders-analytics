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

# ─────────────────────────────────────────────────────────────────────────────
# HTTPX: единые таймауты и лимиты
# ─────────────────────────────────────────────────────────────────────────────
HTTPX_TIMEOUT = httpx.Timeout(connect=10.0, read=60.0, write=20.0, pool=60.0)
HTTPX_LIMITS  = httpx.Limits(max_connections=20, max_keepalive_connections=10)
HTTPX_KW = dict(timeout=HTTPX_TIMEOUT, limits=HTTPX_LIMITS)

# ─────────────────────────────────────────────────────────────────────────────
# БД (SQLite по умолчанию, PG при наличии DATABASE_URL + SQLAlchemy)
# ─────────────────────────────────────────────────────────────────────────────
try:
    from sqlalchemy import create_engine, text  # type: ignore
    _SQLA_OK = True
except Exception:
    _SQLA_OK = False

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
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

def _q(sql: str):
    return text(sql) if _USE_PG else sql  # type: ignore

def _rows(rows):
    return [dict(r._mapping) for r in rows] if _USE_PG else [dict(r) for r in rows]

# ─────────────────────────────────────────────────────────────────────────────
# Auth (как на фронте: X-API-Key и ?api_key=)
# ─────────────────────────────────────────────────────────────────────────────
def require_api_key(req: Request) -> bool:
    required = os.getenv("API_KEY", "").strip()
    if not required:
        return True
    got = (req.headers.get("X-API-Key") or req.query_params.get("api_key") or "")
    got = got.strip().strip("<>").strip('"').strip("'")
    if got != required:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return True

# ─────────────────────────────────────────────────────────────────────────────
# Kaspi API
# ─────────────────────────────────────────────────────────────────────────────
KASPI_TOKEN = os.getenv("KASPI_TOKEN", "").strip()
KASPI_BASE_URL = os.getenv("KASPI_BASE_URL", "https://kaspi.kz/shop/api/v2").rstrip("/")

def _kaspi_headers() -> Dict[str, str]:
    if not KASPI_TOKEN:
        raise HTTPException(status_code=500, detail="KASPI_TOKEN is not set")
    return {
        "X-Auth-Token": KASPI_TOKEN,
        "Accept": "application/vnd.api+json",
        "Content-Type": "application/vnd.api+json",
    }

# ─────────────────────────────────────────────────────────────────────────────
# Время/утилиты
# ─────────────────────────────────────────────────────────────────────────────
def _tzinfo(name: str) -> pytz.BaseTzInfo:
    try: return pytz.timezone(name)
    except Exception: raise HTTPException(status_code=400, detail=f"Bad timezone: {name}")

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
        delta = _bd_delta(bd_start or "20:00")
        s = z.localize(datetime.combine((s0.date() - timedelta(days=1)), dt_time(0, 0))) + delta
        e = z.localize(datetime.combine(e0.date(), dt_time(0, 0))) + delta - timedelta(milliseconds=1)
    else:
        s, e = s0, e0
    return int(s.astimezone(pytz.UTC).timestamp() * 1000), int(e.astimezone(pytz.UTC).timestamp() * 1000)

def _get_num(d: Any, keys: List[str], default: float = 0.0) -> float:
    for k in keys:
        if isinstance(d, dict) and d.get(k) is not None:
            try: return float(d[k])
            except Exception: pass
    return default

def _get_int(d: Any, keys: List[str], default: int = 0) -> int:
    for k in keys:
        if isinstance(d, dict) and d.get(k) is not None:
            try: return int(d[k])
            except Exception: pass
    return default

def _get_str(d: Any, keys: List[str], default: str = "") -> str:
    for k in keys:
        v = d.get(k) if isinstance(d, dict) else None
        if isinstance(v, str) and v.strip():
            return v.strip()
    return default

def _norm_state(s: str) -> str:
    return (s or "").strip().upper()

def _parse_states_csv(s: Optional[str]) -> Optional[set[str]]:
    if not s: return None
    return { _norm_state(x) for x in s.replace(";", ",").split(",") if x.strip() }

def _preview(obj: Any, limit: int = 800) -> str:
    try:
        s = json.dumps(obj) if not isinstance(obj, (str, bytes)) else (obj.decode() if isinstance(obj, bytes) else obj)
        return (s[:limit] + ("…" if len(s) > limit else ""))
    except Exception:
        return "<unrepr>"

# ─────────────────────────────────────────────────────────────────────────────
# Схема БД (orders / order_items / sales)
# ─────────────────────────────────────────────────────────────────────────────
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

# ─────────────────────────────────────────────────────────────────────────────
# Pydantic модели
# ─────────────────────────────────────────────────────────────────────────────
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

# ─────────────────────────────────────────────────────────────────────────────
# Получение списка заказов (минимальный набор полей)
# ─────────────────────────────────────────────────────────────────────────────
async def _iter_orders(start_ms: int, end_ms: int, tz: str, date_field: str,
                       inc_states: Optional[set[str]], exc_states: Optional[set[str]]
                       ) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    headers = _kaspi_headers()
    async with httpx.AsyncClient(base_url=KASPI_BASE_URL, **HTTPX_KW) as cli:
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
                number = _get_str(attrs, ["code", "orderNumber", "number", "id"], "")
                out.append({"id": oid, "date": date_iso, "customer": attrs.get("customer"), "number": number})
            page += 1
    return out

# ─────────────────────────────────────────────────────────────────────────────
# Извлечение SKU из записи позиции
# ─────────────────────────────────────────────────────────────────────────────
SKU_KEYS   = ("code", "productCode", "sku", "merchantProductCode", "article", "offerId", "vendorCode", "barcode", "id")
QTY_KEYS   = ("quantity", "qty", "count")
PRICE_KEYS = ("unitPrice", "basePrice", "price", "unit_price")

def _index_included(included: List[dict]) -> Dict[Tuple[str, str], dict]:
    idx: Dict[Tuple[str, str], dict] = {}
    for it in included or []:
        t = it.get("type"); i = it.get("id")
        if t and i: idx[(str(t), str(i))] = it
    return idx

def _extract_entry(entry: dict, incl: Dict[Tuple[str, str], dict]) -> Optional[Dict[str, Any]]:
    """
    Универсально вытаскивает {sku, qty, unit_price}:
      1) сначала из собственных атрибутов
      2) затем через relationships → product / merchantProduct / masterproducts
      3) если есть offer/merchantProductId — добавляет композит sku вида "<productId>_<offerId>"
    """
    attrs = entry.get("attributes", {}) if "attributes" in entry else entry
    qty   = _get_int(attrs, list(QTY_KEYS), 1)
    price = _get_num(attrs, list(PRICE_KEYS), 0.0)
    sku   = _get_str(attrs, list(SKU_KEYS), "")

    rels = entry.get("relationships", {}) if isinstance(entry, dict) else {}
    rel_product  = (rels.get("product") or {}).get("data")
    rel_mprod    = (rels.get("merchantProduct") or {}).get("data")
    rel_master   = (rels.get("masterProduct") or rels.get("masterproduct") or {}).get("data")

    def _from_rel(node) -> Optional[str]:
        if not isinstance(node, dict):
            return None
        t, i = str(node.get("type") or ""), str(node.get("id") or "")
        ref = incl.get((t, i), {})
        a   = ref.get("attributes", {}) or {}
        # Для masterproducts нередко нужен "id" как артикул каталога
        if "master" in t.lower():
            return i or _get_str(a, ["id", "code", "sku", "productCode"], "")
        # Для product — code/sku/productCode/id
        return _get_str(a, ["code", "sku", "productCode", "id"], "") or (i if t and i else "")

    # Если в самих атрибутах SKU нет — пробуем из связей:
    if not sku:
        sku = _from_rel(rel_product) or _from_rel(rel_master) or _from_rel(rel_mprod) or ""

    # Композит: productId + offer-like
    offer_like = attrs.get("offerId") or attrs.get("merchantProductId")
    if not offer_like and isinstance(rel_mprod, dict):
        offer_like = rel_mprod.get("id")
    prod_id = None
    if isinstance(rel_product, dict):
        prod_id = rel_product.get("id")
    if not prod_id and isinstance(rel_master, dict):
        prod_id = rel_master.get("id")

    if prod_id and offer_like:
        composed = f"{prod_id}_{offer_like}"
        # если базовый sku уже есть — оставим его, но композит используем, если базовый пустой или выглядит чуждо
        if not sku or str(offer_like) not in sku:
            sku = composed

    # Попробуем оценочно вычислить unit_price, если не дали
    if price <= 0:
        total = _get_num(attrs, ["totalPrice", "price"], 0.0)
        if total and qty:
            price = round(total / max(1, qty), 4)

    if not sku:
        return None
    return {"sku": str(sku), "qty": int(qty), "unit_price": float(price)}

# ─────────────────────────────────────────────────────────────────────────────
# Получение позиций заказа: 6 стратегий (быстрые → тяжёлые)
# ─────────────────────────────────────────────────────────────────────────────
async def _fetch_items(order_id: Optional[str] = None,
                       order_code: Optional[str] = None,
                       debug: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Возвращает [{sku, qty, unit_price}, ...].
    Стратегии:
      S1: GET /orders/{id}?include=entries
      S2: GET /orderentries?filter[order.id]={id}
      S3: GET /orders/{id}?include=entries.product
      S4: GET /orders?filter[code]={code}&include=entries
      S5: GET /orders?filter[code]={code}&include=entries.product
      S6: GET /orders/{id}/entries?include=product
    """
    if not (order_id or order_code):
        raise HTTPException(400, "Provide order_id or code")

    headers = _kaspi_headers()
    items: List[Dict[str, Any]] = []
    dbg = debug if debug is not None else {}

    async with httpx.AsyncClient(base_url=KASPI_BASE_URL, **HTTPX_KW) as cli:
        # S1
        if order_id:
            try:
                params = {"include": "entries"}
                r = await cli.get(f"/orders/{order_id}", params=params, headers=headers)
                dbg["S1_status"] = r.status_code
                j = {}
                try: j = r.json()
                except Exception: j = {"raw": (await r.aread())[:800].decode(errors="ignore")}
                dbg["S1_preview"] = _preview(j)
                included = j.get("included", []) if isinstance(j, dict) else []
                idx = _index_included(included)
                for inc in included:
                    if "entry" not in str(inc.get("type", "")).lower():
                        continue
                    got = _extract_entry(inc, idx)
                    if got: items.append(got)
                if items: return items
            except httpx.HTTPError as e:
                dbg["S1_error"] = repr(e)

        # S2
        if order_id and not items:
            try:
                params = {"filter[order.id]": order_id, "page[size]": "200"}
                r = await cli.get("/orderentries", params=params, headers=headers)
                dbg["S2_status"] = r.status_code
                j = {}
                try: j = r.json()
                except Exception: j = {"raw": (await r.aread())[:800].decode(errors="ignore")}
                dbg["S2_preview"] = _preview(j)
                data = j.get("data", []) if isinstance(j, dict) else []
                for e in data:
                    got = _extract_entry(e, {})
                    if got: items.append(got)
                if items: return items
            except httpx.HTTPError as e:
                dbg["S2_error"] = repr(e)

        # S3
        if order_id and not items:
            try:
                params = {"include": "entries.product"}
                r = await cli.get(f"/orders/{order_id}", params=params, headers=headers)
                dbg["S3_status"] = r.status_code
                j = {}
                try: j = r.json()
                except Exception: j = {"raw": (await r.aread())[:800].decode(errors="ignore")}
                dbg["S3_preview"] = _preview(j)
                included = j.get("included", []) if isinstance(j, dict) else []
                idx = _index_included(included)
                for inc in included:
                    if "entry" not in str(inc.get("type", "")).lower():
                        continue
                    got = _extract_entry(inc, idx)
                    if got: items.append(got)
                if items: return items
            except httpx.HTTPError as e:
                dbg["S3_error"] = repr(e)

        # S4
        if order_code and not items:
            try:
                params = {"filter[code]": order_code, "include": "entries", "page[size]": "1"}
                r = await cli.get("/orders", params=params, headers=headers)
                dbg["S4_status"] = r.status_code
                j = {}
                try: j = r.json()
                except Exception: j = {"raw": (await r.aread())[:800].decode(errors="ignore")}
                dbg["S4_preview"] = _preview(j)
                included = j.get("included", []) if isinstance(j, dict) else []
                idx = _index_included(included)
                for inc in included:
                    if "entry" not in str(inc.get("type", "")).lower():
                        continue
                    got = _extract_entry(inc, idx)
                    if got: items.append(got)
                if items: return items
            except httpx.HTTPError as e:
                dbg["S4_error"] = repr(e)

        # S5
        if order_code and not items:
            try:
                params = {"filter[code]": order_code, "include": "entries.product", "page[size]": "1"}
                r = await cli.get("/orders", params=params, headers=headers)
                dbg["S5_status"] = r.status_code
                j = {}
                try: j = r.json()
                except Exception: j = {"raw": (await r.aread())[:800].decode(errors="ignore")}
                dbg["S5_preview"] = _preview(j)
                included = j.get("included", []) if isinstance(j, dict) else []
                idx = _index_included(included)
                for inc in included:
                    if "entry" not in str(inc.get("type", "")).lower():
                        continue
                    got = _extract_entry(inc, idx)
                    if got: items.append(got)
                if items: return items
            except httpx.HTTPError as e:
                dbg["S5_error"] = repr(e)

        # S6 (сабресурс)
        if order_id and not items:
            try:
                params = {"include": "product,merchantProduct,masterProduct", "page[size]": "200"}
                r = await cli.get(f"/orders/{order_id}/entries", params=params, headers=headers)
                dbg["S6_status"] = r.status_code
                j = {}
                try: j = r.json()
                except Exception: j = {"raw": (await r.aread())[:800].decode(errors="ignore")}
                dbg["S6_preview"] = _preview(j)
                data = j.get("data", []) if isinstance(j, dict) else []
                incl = _index_included(j.get("included", []) if isinstance(j, dict) else [])
                for e in data:
                    got = _extract_entry(e, incl)
                    if got: items.append(got)
                if items: return items
            except httpx.HTTPError as e:
                dbg["S6_error"] = repr(e)

    return items

# ─────────────────────────────────────────────────────────────────────────────
# Запись в БД
# ─────────────────────────────────────────────────────────────────────────────
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
            # sales (для FIFO/отчётов)
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

# ─────────────────────────────────────────────────────────────────────────────
# Router + endpoints
# ─────────────────────────────────────────────────────────────────────────────
router = APIRouter(tags=["profit-bridge"])

@router.get("/bridge/ping", name="profit_bridge_ping")
async def ping_bridge():
    _ensure_schema()
    return {"ok": True, "driver": "pg" if _USE_PG else "sqlite"}

@router.get("/bridge/db-stats", name="profit_bridge_db_stats")
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

# 🔎 Прямой тест: получить позиции по order_id или code
@router.get("/bridge/order-items", name="profit_bridge_order_items")
async def order_items(order_id: Optional[str] = Query(None),
                      code: Optional[str] = Query(None),
                      _auth: bool = Depends(require_api_key)):
    dbg: Dict[str, Any] = {}
    items = await _fetch_items(order_id=order_id, order_code=code, debug=dbg)
    return {"order_id": order_id, "code": code, "items": items, "debug": dbg}

# Диагностика «сырых» ответов для одного заказа
@router.get("/bridge/diag-raw", name="profit_bridge_diag_raw")
async def diag_raw(order_id: str = Query(...),
                   _auth: bool = Depends(require_api_key)):
    headers = _kaspi_headers()
    out: Dict[str, Any] = {}
    async with httpx.AsyncClient(base_url=KASPI_BASE_URL, **HTTPX_KW) as cli:
        for path, params, key in [
            (f"/orders/{order_id}", {"include": "entries"}, "orders_id_entries"),
            ("/orderentries", {"filter[order.id]": order_id, "page[size]": "200"}, "orderentries"),
            (f"/orders/{order_id}", {"include": "entries.product"}, "orders_id_entries_product"),
            (f"/orders/{order_id}/entries", {"include": "product,merchantProduct,masterProduct", "page[size]": "200"}, "orders_id_sub_entries"),
        ]:
            try:
                r = await cli.get(path, params=params, headers=headers)
                raw = await r.aread()
                out[key] = {"status": r.status_code, "len": len(raw), "preview": raw[:800].decode(errors="ignore")}
            except httpx.TimeoutException as e:
                out[key] = {"timeout": True, "error": repr(e)}
            except httpx.HTTPError as e:
                out[key] = {"http_error": repr(e)}
    return out

# Диагностика извлечения позиций: можно указать order_id, либо period→берём первый заказ
@router.get("/bridge/diag", name="profit_bridge_diag")
async def diag_bridge(order_id: Optional[str] = Query(None),
                      code: Optional[str] = Query(None),
                      start: Optional[str] = Query(None),
                      end: Optional[str] = Query(None),
                      tz: str = Query("Asia/Almaty"),
                      date_field: str = Query("creationDate"),
                      use_bd: bool = Query(False),
                      business_day_start: str = Query("20:00"),
                      _auth: bool = Depends(require_api_key)):
    dbg: Dict[str, Any] = {}
    oid = order_id
    ocode = code

    if not (oid or ocode):
        if not (start and end):
            raise HTTPException(400, "Provide order_id or code OR (start & end)")
        s_ms, e_ms = _build_window(start, end, tz, use_bd, business_day_start or "20:00")
        orders = await _iter_orders(s_ms, e_ms, tz, date_field, None, None)
        if not orders:
            return {"order_id": None, "items": [], "debug": {"msg": "no orders in period"}}
        cand = orders[0]
        oid = cand["id"]
        ocode = cand.get("number") or None

    items = await _fetch_items(order_id=oid, order_code=ocode, debug=dbg)
    return {"order_id": oid, "code": ocode, "items": items, "debug": dbg}

# Синхронизация периода → локальная БД
@router.api_route("/bridge/sync", methods=["POST", "GET"], name="profit_bridge_sync")
async def profit_bridge_sync(
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

    s_ms, e_ms = _build_window(start, end, tz, bool(use_bd), (business_day_start or "20:00"))
    orders = await _iter_orders(s_ms, e_ms, tz, date_field, inc, exc)
    if not orders:
        return {"status": "ok", "synced_orders": 0, "items_inserted": 0, "skipped": 0, "skipped_timeouts": 0, "skipped_errors": 0}

    total_o = total_i = skipped = skipped_timeouts = skipped_errors = 0
    for od in orders[:max_orders]:
        oid = str(od["id"])
        ocode = od.get("number") or None
        dbg: Dict[str, Any] = {}
        try:
            items = await _fetch_items(order_id=oid, order_code=ocode, debug=dbg)
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
    """
    Монтируйте в main.py так:
        app.include_router(get_profit_bridge_router(), prefix="/profit")
    Новые полезные ручки:
        GET  /profit/bridge/order-items?order_id=...          ← прямое извлечение SKU
        GET  /profit/bridge/order-items?code=624374271        ← по номеру заказа
        GET  /profit/bridge/diag-raw?order_id=...
        GET  /profit/bridge/diag?order_id=... | ?code=... | ?start=&end=
        GET|POST /profit/bridge/sync?start=...&end=...
    """
    return router
