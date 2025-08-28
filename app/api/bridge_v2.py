# app/api/bridge_v2.py
from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel

# ===== DB (SQLite/PG) =====
_USE_PG = bool(os.getenv("PG_DSN"))
_engine = None
text = lambda s: s

if _USE_PG:
    from sqlalchemy import create_engine, text as _text
    _engine = create_engine(os.getenv("PG_DSN"), pool_pre_ping=True, pool_size=3, max_overflow=5)
    text = _text

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

# ===== Таблицы: проверка существования (безопасные заглушки) =====
def _table_exists_on(conn, name: str) -> bool:
    if _USE_PG:
        row = conn.execute(_q("""
            SELECT EXISTS (
              SELECT 1 FROM information_schema.tables
              WHERE table_schema = 'public' AND table_name = :n
            ) AS ok
        """), {"n": name}).first()
        return bool(row and row._mapping["ok"])
    else:
        row = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone()
        return bool(row)

def _table_exists(name: str) -> bool:
    with _db() as c:
        return _table_exists_on(c, name)

# ===== API key =====
def require_api_key(req: Request) -> bool:
    key = os.getenv("API_KEY")
    if not key:
        return True
    sent = req.headers.get("X-API-Key") or req.query_params.get("api_key")
    if sent != key:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return True

# ===== Schema =====
def _ensure_schema():
    with _db() as c:
        if _USE_PG:
            c.execute(_q("""
                CREATE TABLE IF NOT EXISTS orders(
                  id TEXT PRIMARY KEY,
                  date TIMESTAMP NOT NULL,
                  customer TEXT
                );
            """))
            c.execute(_q("""
                CREATE TABLE IF NOT EXISTS order_items(
                  id SERIAL PRIMARY KEY,
                  order_id TEXT NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
                  sku TEXT NOT NULL,
                  qty INTEGER NOT NULL,
                  unit_price DOUBLE PRECISION NOT NULL,
                  commission_pct DOUBLE PRECISION
                );
            """))
            c.execute(_q("CREATE INDEX IF NOT EXISTS idx_order_items_sku ON order_items(sku);"))
            c.execute(_q("CREATE INDEX IF NOT EXISTS idx_orders_date ON orders(date);"))
            c.execute(_q("""
                CREATE TABLE IF NOT EXISTS batch_consumption(
                  id SERIAL PRIMARY KEY,
                  sale_item_id INTEGER NOT NULL REFERENCES order_items(id) ON DELETE CASCADE,
                  batch_id INTEGER NOT NULL,
                  sku TEXT NOT NULL,
                  qty INTEGER NOT NULL,
                  unit_cost DOUBLE PRECISION
                );
            """))
            c.execute(_q("CREATE INDEX IF NOT EXISTS idx_bc_sale ON batch_consumption(sale_item_id);"))
            c.execute(_q("CREATE INDEX IF NOT EXISTS idx_bc_sku ON batch_consumption(sku);"))
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
                CREATE INDEX IF NOT EXISTS idx_order_items_sku ON order_items(sku);
                CREATE INDEX IF NOT EXISTS idx_orders_date ON orders(date);
                CREATE TABLE IF NOT EXISTS batch_consumption(
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  sale_item_id INTEGER NOT NULL,
                  batch_id INTEGER NOT NULL,
                  sku TEXT NOT NULL,
                  qty INTEGER NOT NULL,
                  unit_cost REAL
                );
                CREATE INDEX IF NOT EXISTS idx_bc_sale ON batch_consumption(sale_item_id);
                CREATE INDEX IF NOT EXISTS idx_bc_sku ON batch_consumption(sku);
            """)

# ===== Categories (комиссия) и продукты =====
def _categories() -> Dict[str, Dict[str, float]]:
    # Если категорий ещё нет — вернём пусто (комиссия = 0)
    if not _table_exists("categories"):
        return {}
    with _db() as c:
        if _USE_PG:
            rows = c.execute(_q(
                "SELECT name, base_percent, extra_percent, tax_percent FROM categories"
            )).all()
            return {r._mapping["name"]: dict(r._mapping) for r in rows}
        else:
            rows = c.execute(
                "SELECT name, base_percent, extra_percent, tax_percent FROM categories"
            ).fetchall()
            return {r["name"]: dict(r) for r in rows}

def _sku_to_category(skus: List[str]) -> Dict[str, str]:
    if not skus or not _table_exists("products"):
        return {}
    with _db() as c:
        if _USE_PG:
            rows = c.execute(_q(
                "SELECT sku, category FROM products WHERE sku = ANY(:arr)"
            ), {"arr": skus}).all()
            return {r._mapping["sku"]: (r._mapping.get("category") or "") for r in rows}
        else:
            qm = ",".join("?" for _ in skus)
            rows = c.execute(
                f"SELECT sku, category FROM products WHERE sku IN ({qm})", skus
            ).fetchall()
            return {r["sku"]: (r.get("category") or "") for r in rows}

# ===== Kaspi: все позиции заказа (SKU/qty/price) =====
KASPI_BASE_URL = os.getenv("KASPI_BASE_URL", "https://kaspi.kz/shop/api/v2")
KASPI_TOKEN = os.getenv("KASPI_TOKEN", "").strip()

def _kaspi_headers() -> Dict[str, str]:
    if not KASPI_TOKEN:
        raise HTTPException(status_code=500, detail="KASPI_TOKEN is not set")
    return {"X-Auth-Token": KASPI_TOKEN, "Accept": "application/vnd.api+json"}

def _norm_state(s: str) -> str:
    return (s or "").strip().upper()

def _get_str(attrs: Dict[str, Any], keys: List[str], default: str = "") -> str:
    for k in keys:
        v = attrs.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return default

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

async def _iter_orders(start_ms: int, end_ms: int, date_field: str,
                       inc_states: Optional[set[str]], exc_states: Optional[set[str]]) -> List[Dict[str, Any]]:
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
                number = _get_str(attrs, ["code", "orderNumber", "number", "id"], oid)
                out.append({"id": oid, "date": date_iso, "number": number, "customer": _get_str(attrs, ["customer"])})
            page += 1
    return out

async def _fetch_entries(order_id: str) -> List[Dict[str, Any]]:
    """Возвращает список позиций заказа."""
    headers = _kaspi_headers()
    items: List[Dict[str, Any]] = []
    timeout = httpx.Timeout(connect=10.0, read=60.0, write=30.0, pool=60.0)
    async with httpx.AsyncClient(base_url=KASPI_BASE_URL, timeout=timeout) as cli:
        params = {"filter[order.id]": order_id, "page[size]": "200", "include": "product,merchantProduct,masterProduct,offer"}
        r = await cli.get("/orderentries", params=params, headers=headers)
        r.raise_for_status()
        j = r.json()
        data = j.get("data", []) or []
        included = {x.get("id"): x for x in (j.get("included", []) or []) if isinstance(x, dict) and x.get("id")}
        for entry in data:
            attrs = entry.get("attributes", {}) or {}
            rels = entry.get("relationships", {}) or {}
            sku = None
            # 1) offer.code (всегда вида "<sku>_<id>")
            offer = rels.get("offer", {}).get("data")
            if isinstance(offer, dict):
                offer_id = offer.get("id")
                inc = included.get(offer_id) or {}
                offer_code = (inc.get("attributes", {}) or {}).get("code")
                if isinstance(offer_code, str) and "_" in offer_code:
                    sku = offer_code.split("_", 1)[0]
            # 2) запасной путь — product.code
            if not sku:
                prod = rels.get("product", {}).get("data")
                if isinstance(prod, dict):
                    prod_id = prod.get("id")
                    inc = included.get(prod_id) or {}
                    pc = (inc.get("attributes", {}) or {}).get("code")
                    if isinstance(pc, str) and pc.strip():
                        sku = pc.strip()
            if not sku:
                continue
            qty = int(attrs.get("quantity", 1))
            unit_price = float(attrs.get("baseUnitPrice", attrs.get("unitPrice", 0)))
            items.append({"sku": sku.strip(), "qty": qty, "unit_price": unit_price, "commission_pct": None})
    return items

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

        # replace items for this order
        if _USE_PG:
            c.execute(_q("DELETE FROM order_items WHERE order_id=:id"), {"id": o.id})
        else:
            c.execute("DELETE FROM order_items WHERE order_id=?", (o.id,))
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
            else:
                c.execute("""
                    INSERT INTO order_items(order_id,sku,qty,unit_price,commission_pct)
                    VALUES(?,?,?,?,?)
                """, (o.id, it.sku.strip(), int(it.qty), float(it.unit_price),
                      float(it.commission_pct) if it.commission_pct is not None else None))
            ins_i += 1
    return ins_o, ins_i

# ===== FIFO consumption из batches =====
def _fifo_consume_for_item(conn, sale_item_id: int, sku: str, need_qty: int):
    if need_qty <= 0:
        return
    # Если склад ещё не завезён — списывать нечего, выходим без ошибки
    if not _table_exists_on(conn, "batches"):
        return
    if _USE_PG:
        rows = conn.execute(_q("""
            SELECT b.id, b.qty,
                   COALESCE(SUM(CASE WHEN bc.sku=b.sku THEN bc.qty END),0) AS used,
                   b.unit_cost
            FROM batches b
            LEFT JOIN batch_consumption bc ON bc.batch_id=b.id
            WHERE b.sku = :sku
            GROUP BY b.id
            ORDER BY b.date, b.id
        """), {"sku": sku}).all()
        batches = [dict(r._mapping) for r in rows]
    else:
        cur = conn.execute("""
            SELECT b.id, b.qty, b.unit_cost
            FROM batches b WHERE b.sku=?
            ORDER BY b.date, b.id
        """, (sku,))
        tmp = [dict(r) for r in cur.fetchall()]
        batches = []
        for r in tmp:
            used = conn.execute("SELECT COALESCE(SUM(qty),0) AS u FROM batch_consumption WHERE batch_id=?", (r["id"],)).fetchone()["u"]
            r["used"] = used
            batches.append(r)

    remain = int(need_qty)
    for b in batches:
        free = int(b["qty"]) - int(b.get("used", 0) or 0)
        if free <= 0:
            continue
        take = min(remain, free)
        if take <= 0:
            break
        if _USE_PG:
            conn.execute(_q("""
                INSERT INTO batch_consumption(sale_item_id,batch_id,sku,qty,unit_cost)
                VALUES(:sid,:bid,:sku,:qty,:cost)
            """), {"sid": sale_item_id, "bid": b["id"], "sku": sku, "qty": take, "cost": float(b["unit_cost"])})
        else:
            conn.execute("""
                INSERT INTO batch_consumption(sale_item_id,batch_id,sku,qty,unit_cost)
                VALUES(?,?,?,?,?)
            """, (sale_item_id, b["id"], sku, take, float(b["unit_cost"])))
        remain -= take
        if remain <= 0:
            break

def _rebuild_ledger(date_from: Optional[str], date_to: Optional[str]) -> Dict[str, Any]:
    _ensure_schema()
    with _db() as c:
        if _USE_PG:
            cond = ""
            params: Dict[str, Any] = {}
            if date_from:
                cond += " AND o.date::date >= :df"; params["df"] = date_from
            if date_to:
                cond += " AND o.date::date <= :dt"; params["dt"] = date_to
            ids = c.execute(_q(f"""
                SELECT i.id, i.sku, i.qty
                FROM order_items i
                JOIN orders o ON o.id = i.order_id
                WHERE 1=1 {cond}
                ORDER BY o.date, i.id
            """), params).all()
            ids = [dict(r._mapping) for r in ids]
            if ids:
                c.execute(_q("DELETE FROM batch_consumption WHERE sale_item_id = ANY(:ids)"),
                          {"ids": [x["id"] for x in ids]})
        else:
            cond = "1=1"; params: Tuple = tuple()
            if date_from and date_to:
                cond = "date(o.date) BETWEEN date(?) AND date(?)"; params = (date_from, date_to)
            elif date_from:
                cond = "date(o.date) >= date(?)"; params = (date_from,)
            elif date_to:
                cond = "date(o.date) <= date(?)"; params = (date_to,)
            rows = c.execute(f"""
                SELECT i.id, i.sku, i.qty
                FROM order_items i JOIN orders o ON o.id=i.order_id
                WHERE {cond}
                ORDER BY o.date, i.id
            """, params).fetchall()
            ids = [dict(r) for r in rows]
            if ids:
                qm = ",".join("?" for _ in ids)
                c.execute(f"DELETE FROM batch_consumption WHERE sale_item_id IN ({qm})", [x["id"] for x in ids])
        for r in ids:
            _fifo_consume_for_item(c, r["id"], r["sku"], int(r["qty"]))
        return {"rebuilt_items": len(ids)}

# ===== Публичный роутер =====
def get_profit_router_v2() -> APIRouter:
    router = APIRouter(prefix="/profit", tags=["profit-v2"])

    @router.post("/bridge/sync")
    async def bridge_sync(
        start: str = Query(..., description="YYYY-MM-DD"),
        end: str = Query(..., description="YYYY-MM-DD"),
        tz: str = Query("Asia/Almaty"),
        date_field: str = Query("creationDate"),
        use_bd: int = Query(1, description="ignored (для совместимости UI)"),
        _: bool = Depends(require_api_key),
    ):
        # Синхронизация строго по одному дню — так надёжнее для Kaspi
        if start != end:
            raise HTTPException(status_code=400, detail="sync only supports a single day to avoid API timeouts")

        s_dt = datetime.fromisoformat(start + "T00:00:00")
        e_dt = datetime.fromisoformat(end + "T23:59:59")
        s_ms = int(s_dt.timestamp() * 1000)
        e_ms = int(e_dt.timestamp() * 1000)

        orders = await _iter_orders(s_ms, e_ms, date_field, inc_states=None, exc_states={"CANCELED"})
        inserted = updated = items_total = 0
        for o in orders:
            items = await _fetch_entries(o["id"])
            if not items:
                continue
            model = OrderIn(id=o["id"], date=o["date"], customer=o.get("customer"),
                            items=[OrderItemIn(**it) for it in items])
            ins_o, ins_i = _upsert_order_with_items(model)
            updated += 1 if ins_o == 0 else 0
            inserted += 1 if ins_o == 1 else 0
            items_total += ins_i

        _rebuild_ledger(start, end)
        return {"status": "ok", "orders_found": len(orders),
                "orders_inserted": inserted, "orders_updated": updated,
                "items_written": items_total}

    @router.post("/rebuild-ledger")
    async def rebuild_ledger(date_from: Optional[str] = Query(None), date_to: Optional[str] = Query(None),
                             _: bool = Depends(require_api_key)):
        return _rebuild_ledger(date_from, date_to)

    def _effective_commission_pct(sku: str, direct_comm: Optional[float],
                                  cat_map: Dict[str, str], cats: Dict[str, Dict[str, float]]) -> float:
        if direct_comm is not None:
            return float(direct_comm)
        cat = cat_map.get(sku, "")
        c = cats.get(cat) or {}
        return float(c.get("base_percent", 0.0)) + float(c.get("extra_percent", 0.0)) + float(c.get("tax_percent", 0.0))

    @router.get("/summary")
    async def summary(date_from: str = Query(...), date_to: str = Query(...),
                      group_by: str = Query("total", pattern="^(total|day)$"),
                      _: bool = Depends(require_api_key)):
        _ensure_schema()
        with _db() as c:
            if _USE_PG:
                rows = c.execute(_q("""
                    SELECT o.date::date AS day, i.id AS iid, i.sku, i.qty, i.unit_price, i.commission_pct,
                           COALESCE(SUM(bc.qty * bc.unit_cost) OVER (PARTITION BY i.id), 0) AS cost
                    FROM order_items i
                    JOIN orders o ON o.id=i.order_id
                    LEFT JOIN batch_consumption bc ON bc.sale_item_id=i.id
                    WHERE o.date::date BETWEEN :df AND :dt
                """), {"df": date_from, "dt": date_to}).all()
                data = [dict(r._mapping) for r in rows]
            else:
                rows = c.execute("""
                    SELECT substr(o.date,1,10) AS day, i.id as iid, i.sku, i.qty, i.unit_price, i.commission_pct
                    FROM order_items i JOIN orders o ON o.id=i.order_id
                    WHERE date(o.date) BETWEEN date(?) AND date(?)
                """, (date_from, date_to)).fetchall()
                data = []
                for r in rows:
                    iid = r["iid"]
                    # если таблицы нет — cost остаётся 0
                    if _table_exists_on(c, "batch_consumption"):
                        cost = c.execute("SELECT COALESCE(SUM(qty*unit_cost),0) AS c FROM batch_consumption WHERE sale_item_id=?",
                                         (iid,)).fetchone()["c"]
                    else:
                        cost = 0.0
                    it = dict(r); it["cost"] = float(cost); data.append(it)

        skus = [d["sku"] for d in data]
        cats = _categories()
        cat_map = _sku_to_category(skus)

        out = []
        for d in data:
            qty = float(d["qty"]); price = float(d["unit_price"])
            revenue = qty * price
            comm_pct = _effective_commission_pct(d["sku"], d.get("commission_pct"), cat_map, cats)
            commission = revenue * (comm_pct / 100.0)
            cost = float(d.get("cost", 0.0))
            profit = revenue - commission - cost
            out.append({"day": d["day"], "revenue": revenue, "commission": commission, "cost": cost, "profit": profit})

        if group_by == "day":
            by = {}
            for r in out:
                agg = by.setdefault(r["day"], {"revenue":0.0,"commission":0.0,"cost":0.0,"profit":0.0})
                for k in agg: agg[k] += float(r[k])
            rows = [{"day": d, **{k: round(v,2) for k,v in vvv.items()}} for d, vvv in sorted(by.items())]
            total = {k: round(sum(x[k] for x in rows), 2) for k in ("revenue","commission","cost","profit")}
            return {"currency": os.getenv("CURRENCY","KZT"), "group_by":"day", "rows": rows, "total": total}
        else:
            total = {"revenue": round(sum(r["revenue"] for r in out),2),
                     "commission": round(sum(r["commission"] for r in out),2),
                     "cost": round(sum(r["cost"] for r in out),2),
                     "profit": round(sum(r["profit"] for r in out),2)}
            return {"currency": os.getenv("CURRENCY","KZT"), "group_by":"total", "rows": [], "total": total}

    @router.get("/by-sku")
    async def by_sku(date_from: str = Query(...), date_to: str = Query(...),
                     limit: int = Query(50, ge=1, le=500),
                     _: bool = Depends(require_api_key)):
        _ensure_schema()
        with _db() as c:
            if _USE_PG:
                rows = c.execute(_q("""
                    SELECT i.id, i.sku, i.qty, i.unit_price, i.commission_pct, o.date::date AS day,
                           COALESCE(SUM(bc.qty*bc.unit_cost) OVER (PARTITION BY i.id),0) AS cost
                    FROM order_items i JOIN orders o ON o.id=i.order_id
                    LEFT JOIN batch_consumption bc ON bc.sale_item_id=i.id
                    WHERE o.date::date BETWEEN :df AND :dt
                """), {"df": date_from, "dt": date_to}).all()
                data = [dict(r._mapping) for r in rows]
            else:
                rows = c.execute("""
                    SELECT i.id, i.sku, i.qty, i.unit_price, i.commission_pct, substr(o.date,1,10) AS day
                    FROM order_items i JOIN orders o ON o.id=i.order_id
                    WHERE date(o.date) BETWEEN date(?) AND date(?)
                """, (date_from, date_to)).fetchall()
                data = []
                for r in rows:
                    iid = r["id"]
                    if _table_exists_on(c, "batch_consumption"):
                        cost = c.execute("SELECT COALESCE(SUM(qty*unit_cost),0) AS c FROM batch_consumption WHERE sale_item_id=?",
                                         (iid,)).fetchone()["c"]
                    else:
                        cost = 0.0
                    it = dict(r); it["cost"] = float(cost); data.append(it)

        cats = _categories()
        cat_map = _sku_to_category([d["sku"] for d in data])

        agg: Dict[str, Dict[str, float]] = {}
        for d in data:
            sku = d["sku"]; qty = float(d["qty"]); price = float(d["unit_price"])
            revenue = qty * price
            comm_pct = float(d.get("commission_pct")) if d.get("commission_pct") is not None else (
                float(cats.get(cat_map.get(sku,""),{}).get("base_percent",0.0)) +
                float(cats.get(cat_map.get(sku,""),{}).get("extra_percent",0.0)) +
                float(cats.get(cat_map.get(sku,""),{}).get("tax_percent",0.0))
            )
            commission = revenue * (comm_pct/100.0)
            cost = float(d.get("cost", 0.0))
            profit = revenue - commission - cost
            a = agg.setdefault(sku, {"sku":sku,"qty":0.0,"revenue":0.0,"commission":0.0,"cost":0.0,"profit":0.0})
            a["qty"] += qty; a["revenue"] += revenue; a["commission"] += commission; a["cost"] += cost; a["profit"] += profit

        rows = sorted(
            [{"sku":k, **{kk: round(vv,2) for kk,vv in vals.items() if kk!='sku'}} for k,vals in agg.items()],
            key=lambda x: -x["profit"]
        )[:limit]
        return {"currency": os.getenv("CURRENCY","KZT"), "rows": rows}

    # ===== Совместимый пинг для фронта: /profit/db/ping =====
    @router.get("/db/ping")
    async def db_ping(_: bool = Depends(require_api_key)):
        return {
            "ok": True,
            "engine": "postgres" if _USE_PG else "sqlite",
            "path": DB_PATH if not _USE_PG else None,
        }

    return router
