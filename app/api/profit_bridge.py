# app/api/profit_bridge.py
from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timedelta

from fastapi import APIRouter, HTTPException, Query, Depends, Request
from pydantic import BaseModel

# DB: та же логика, что и в profit_fifo
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
        try: yield conn
        finally: conn.close()

def _q(sql: str): return text(sql) if _USE_PG else sql
def _rows(rows): return [dict(r._mapping) for r in rows] if _USE_PG else [dict(r) for r in rows]

# --- API KEY (совместимо с фронтом) ---
def require_api_key(req: Request) -> bool:
    key = os.getenv("API_KEY")
    if not key:
        return True
    sent = req.headers.get("X-API-Key") or req.query_params.get("api_key")
    if sent != key:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return True

# --- KASPI client (без жесткой привязки к реализации) ---
KASPI_TOKEN = os.getenv("KASPI_TOKEN", "").strip()
KASPI_BASE_URL = os.getenv("KASPI_BASE_URL", "https://kaspi.kz/shop/api/v2").rstrip("/")

# Пытаемся импортировать ваш KaspiClient; если нет — используем httpx напрямую.
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

def _kaspi_headers() -> Dict[str,str]:
    if not KASPI_TOKEN:
        raise HTTPException(status_code=500, detail="KASPI_TOKEN is not set")
    # заголовок может отличаться в конкретном SDK; в большинстве случаев X-Auth-Token
    return {
        "X-Auth-Token": KASPI_TOKEN,
        "Accept": "application/vnd.api+json",
    }

# --- Схема (минимум таблиц для FIFO) ---
def _ensure_schema():
    with _db() as c:
        if _USE_PG:
            c.execute(_q("""CREATE TABLE IF NOT EXISTS orders(
                id TEXT PRIMARY KEY, date TIMESTAMP NOT NULL, customer TEXT)"""))
            c.execute(_q("""CREATE TABLE IF NOT EXISTS order_items(
                id SERIAL PRIMARY KEY,
                order_id TEXT NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
                sku TEXT NOT NULL, qty INTEGER NOT NULL,
                unit_price DOUBLE PRECISION NOT NULL,
                commission_pct DOUBLE PRECISION)"""))
            c.execute(_q("CREATE INDEX IF NOT EXISTS idx_order_items_sku ON order_items(sku)"))
            c.execute(_q("CREATE INDEX IF NOT EXISTS idx_orders_date ON orders(date)"))
        else:
            c.executescript("""
            CREATE TABLE IF NOT EXISTS orders(
                id TEXT PRIMARY KEY, date TEXT NOT NULL, customer TEXT);
            CREATE TABLE IF NOT EXISTS order_items(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id TEXT NOT NULL,
                sku TEXT NOT NULL, qty INTEGER NOT NULL,
                unit_price REAL NOT NULL,
                commission_pct REAL,
                FOREIGN KEY(order_id) REFERENCES orders(id) ON DELETE CASCADE);
            CREATE INDEX IF NOT EXISTS idx_order_items_sku ON order_items(sku);
            CREATE INDEX IF NOT EXISTS idx_orders_date ON orders(date);
            """)

# ---- Модель для апсерта (совпадает с profit_fifo) ----
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

# --- Утилиты извлечения полей из "позиции" Kaspi ---
def _get_num(d: Any, keys: List[str], default: float = 0.0) -> float:
    for k in keys:
        if k in d and d[k] is not None:
            try: return float(d[k])
            except Exception: pass
    return default

def _get_int(d: Any, keys: List[str], default: int = 0) -> int:
    for k in keys:
        if k in d and d[k] is not None:
            try: return int(d[k])
            except Exception: pass
    return default

def _get_str(d: Any, keys: List[str], default: str = "") -> str:
    for k in keys:
        v = d.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return default

def _extract_product_code(entry: Dict[str, Any], included: Dict[str, Dict[str, Any]]) -> str:
    """
    Пытаемся достать код товара (SKU) из разных возможных мест.
    """
    # 1) attributes.code / attributes.productCode / attributes.sku
    sku = _get_str(entry.get("attributes", {}), ["code", "productCode", "sku"])
    if sku:
        return sku
    # 2) связь на product в relationships
    rel = entry.get("relationships", {}) or {}
    for key in ("product", "masterProduct", "item", "sku"):
        node = rel.get(key)
        if not node: 
            continue
        data = node.get("data")
        if isinstance(data, dict):
            ref_id = data.get("id")
            if ref_id and ref_id in included:
                attrs = included[ref_id].get("attributes", {})
                sku = _get_str(attrs, ["code", "sku", "productCode"])
                if sku:
                    return sku
    # 3) ничего не нашли — пробуем id строки
    return _get_str(entry, ["id"], "")

async def _fetch_entries_httpx(order_id: str) -> List[Dict[str, Any]]:
    """
    Вытягиваем позиции заказа через HTTPX, не полагаясь на конкретный SDK.
    Сначала пробуем /orderentries, затем fallback на /orders/{id}?include=...
    """
    headers = _kaspi_headers()
    async with httpx.AsyncClient(base_url=KASPI_BASE_URL, timeout=30.0) as cli:
        # Попытка 1: фильтр по order.id
        try:
            params = {"filter[order.id]": order_id, "include": "product", "page[size]": "200"}
            r = await cli.get("/orderentries", params=params, headers=headers)
            r.raise_for_status()
            j = r.json()
            data = j.get("data", [])
            included_raw = j.get("included", []) or []
            included = {x["id"]: x for x in included_raw if isinstance(x, dict) and "id" in x}
            out = []
            for e in data:
                if not isinstance(e, dict): 
                    continue
                attrs = e.get("attributes", {}) or {}
                qty = _get_int(attrs, ["quantity", "qty", "count"], 1)
                price = _get_num(attrs, ["unitPrice", "basePrice", "price"], 0.0)
                sku = _extract_product_code(e, included) or _get_str(attrs, ["code", "productCode", "sku"])
                if not sku:
                    continue
                out.append({"sku": sku, "qty": qty, "unit_price": price})
            if out:
                return out
        except httpx.HTTPError:
            pass

        # Попытка 2: include через orders/{id}
        try:
            params = {"include": "entries.product"}
            r = await cli.get(f"/orders/{order_id}", params=params, headers=headers)
            r.raise_for_status()
            j = r.json()
            included_raw = j.get("included", []) or []
            included = {x["id"]: x for x in included_raw if isinstance(x, dict) and "id" in x}
            # entries сами могут лежать в included, достаём все типы похожие на entry
            out = []
            for inc in included_raw:
                t = str(inc.get("type", "")).lower()
                if "entry" not in t: 
                    continue
                attrs = inc.get("attributes", {}) or {}
                qty = _get_int(attrs, ["quantity", "qty", "count"], 1)
                price = _get_num(attrs, ["unitPrice", "basePrice", "price"], 0.0)
                sku = _extract_product_code(inc, included) or _get_str(attrs, ["code", "productCode", "sku"])
                if not sku:
                    continue
                out.append({"sku": sku, "qty": qty, "unit_price": price})
            return out
        except httpx.HTTPError:
            pass

    return []

async def _iter_orders(start_iso: str, end_iso: str) -> List[Dict[str, Any]]:
    """
    Возвращаем список {id, date_iso, customer?} за период.
    Используем ваш KaspiClient если есть, иначе — прямой вызов.
    """
    out: List[Dict[str, Any]] = []
    # 1) SDK
    if KaspiClient:
        cli = KaspiClient(token=KASPI_TOKEN, base_url=KASPI_BASE_URL)
        # в вашем main.py уже есть iter_orders; повторим упрощённо:
        start_dt = datetime.fromisoformat(start_iso + "T00:00:00+00:00")
        end_dt = datetime.fromisoformat(end_iso + "T23:59:59+00:00")
        step = timedelta(days=7)
        cur = start_dt
        while cur <= end_dt:
            nxt = min(cur + step, end_dt)
            for order in cli.iter_orders(start=cur, end=nxt, filter_field="creationDate"):
                oid = str(order.get("id"))
                attrs = order.get("attributes", {}) or {}
                # дата: берём creationDate/ plannedShipmentDate / shipmentDate / deliveryDate — что есть
                ms = None
                for key in ("creationDate","plannedShipmentDate","shipmentDate","deliveryDate"):
                    v = attrs.get(key)
                    if v is None:
                        continue
                    try:
                        ms = int(v)
                        break
                    except Exception:
                        try:
                            ms = int(datetime.fromisoformat(str(v).replace("Z","+00:00")).timestamp()*1000)
                            break
                        except Exception:
                            pass
                if ms is None:
                    continue
                date_iso = datetime.utcfromtimestamp(ms/1000.0).isoformat()
                out.append({"id": oid, "date": date_iso, "customer": attrs.get("customer")})
            cur = nxt + timedelta(seconds=1)
        return out

    # 2) HTTP напрямую
    headers = _kaspi_headers()
    async with httpx.AsyncClient(base_url=KASPI_BASE_URL, timeout=30.0) as cli:
        # Упрощённо: фильтр по creationDate (миллисекунды)
        # Конвертнём границы в ms UTC
        sdt = datetime.fromisoformat(start_iso + "T00:00:00+00:00")
        edt = datetime.fromisoformat(end_iso + "T23:59:59+00:00")
        s_ms = int(sdt.timestamp()*1000); e_ms = int(edt.timestamp()*1000)

        page = 0
        while True:
            params = {
                "page[number]": str(page),
                "page[size]": "100",
                "filter[creationDate][ge]": str(s_ms),
                "filter[creationDate][le]": str(e_ms),
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
                ms = attrs.get("creationDate") or s_ms
                date_iso = datetime.utcfromtimestamp(int(ms)/1000.0).isoformat()
                out.append({"id": oid, "date": date_iso, "customer": attrs.get("customer")})
            page += 1

    return out

# --- апсерт ордеров/позиций в общую FIFO-базу ---
def _upsert_order_with_items(o: OrderIn) -> Tuple[int,int]:
    """return (orders_inserted_or_updated, items_inserted)"""
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

        # replace items
        if _USE_PG: c.execute(_q("DELETE FROM order_items WHERE order_id=:id"), {"id": o.id})
        else:       c.execute("DELETE FROM order_items WHERE order_id=?", (o.id,))
        for it in o.items:
            if _USE_PG:
                c.execute(_q("""
                  INSERT INTO order_items(order_id,sku,qty,unit_price,commission_pct)
                  VALUES(:oid,:sku,:qty,:p,:comm)
                """), {"oid": o.id, "sku": it.sku.strip(), "qty": int(it.qty),
                       "p": float(it.unit_price),
                       "comm": float(it.commission_pct) if it.commission_pct is not None else None})
            else:
                c.execute("""
                  INSERT INTO order_items(order_id,sku,qty,unit_price,commission_pct)
                  VALUES(?,?,?,?,?)
                """, (o.id, it.sku.strip(), int(it.qty), float(it.unit_price),
                      float(it.commission_pct) if it.commission_pct is not None else None))
            ins_i += 1
    return ins_o, ins_i

# --- Router ---
router = APIRouter(tags=["profit-bridge"])

@router.get("/bridge/ping")
async def ping_bridge():
    _ensure_schema()
    return {"ok": True, "driver": "pg" if _USE_PG else "sqlite"}

@router.post("/bridge/sync")
async def bridge_sync(
    date_from: str = Query(..., description="YYYY-MM-DD"),
    date_to: str = Query(..., description="YYYY-MM-DD"),
    _: bool = Depends(require_api_key)
):
    """
    Тянем заказы из Kaspi за период, вытягиваем позиции, пишем в FIFO-базу.
    """
    if not KASPI_TOKEN:
        raise HTTPException(status_code=500, detail="KASPI_TOKEN is not set")
    # собираем список заказов
    orders = await _iter_orders(date_from, date_to)
    if not orders:
        return {"status":"ok","orders":0,"items":0}

    total_o = total_i = 0
    for od in orders:
        oid = str(od["id"])
        # позиции
        items = await _fetch_entries_httpx(oid)
        if not items:
            # без позиций смысла нет; пропускаем
            continue
        # дата ISO (берём как есть)
        dt_iso = od["date"]
        o = OrderIn(id=oid, date=dt_iso, customer=od.get("customer"), 
                    items=[OrderItemIn(sku=i["sku"], qty=int(i["qty"]), unit_price=float(i["unit_price"])) for i in items])
        io, ii = _upsert_order_with_items(o)
        total_o += io
        total_i += ii

    return {"status":"ok","orders_upserted": total_o, "items_inserted": total_i}

def get_profit_bridge_router() -> APIRouter:
    # чтобы можно было и так, и так: include_router(get_profit_bridge_router(), ...)
    return router
