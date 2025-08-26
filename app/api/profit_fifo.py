# app/api/profit_fifo.py
from __future__ import annotations

from typing import List, Dict, Any, Optional, Tuple
from contextlib import contextmanager
from datetime import datetime, timedelta, date

import os
import sqlite3

from fastapi import APIRouter, HTTPException, Query, Body, Depends, Request
from pydantic import BaseModel

# ========= ENV / бизнес-день =========
DEFAULT_TZ = os.getenv("TZ", "Asia/Almaty")
USE_BUSINESS_DAY = os.getenv("USE_BUSINESS_DAY", "true").lower() in ("1", "true", "yes", "on")
BUSINESS_DAY_START = os.getenv("BUSINESS_DAY_START", "20:00")
CURRENCY = os.getenv("CURRENCY", "KZT")

def _bd_delta(hhmm: str) -> timedelta:
    try:
        hh, mm = map(int, hhmm.split(":", 1))
    except Exception:
        hh, mm = 20, 0
    return timedelta(hours=hh, minutes=mm)

def _bucket_date(dt: datetime, use_bd: bool, bd_start: str) -> date:
    if use_bd:
        # сдвигаем на 24ч - начало_дня, чтобы отнести к "дню магазина"
        shift = timedelta(hours=24) - _bd_delta(bd_start)
        return (dt + shift).date()
    return dt.date()

# ========= SQLAlchemy / PG переключатель =========
try:
    from sqlalchemy import create_engine, text
    _SQLA_OK = True
except Exception:
    _SQLA_OK = False

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
    """Для PG возвращает SQLAlchemy Connection (begin), для SQLite — sqlite3.Connection."""
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

# ========= API Key =========
def require_api_key(req: Request) -> bool:
    key = os.getenv("API_KEY")
    if not key:
        return True
    sent = req.headers.get("X-API-Key") or req.query_params.get("api_key")
    if sent != key:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return True

# ========= Схема БД (добавка к products/batches/categories) =========
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

# ========= Утилиты из складской части =========
def _categories() -> Dict[str, Dict[str, float]]:
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
    if not skus:
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

def _batches_for_skus(skus: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    if not skus:
        return {}
    with _db() as c:
        if _USE_PG:
            rows = c.execute(_q("""
                SELECT id, sku, date::date AS d, qty, unit_cost
                FROM batches
                WHERE sku = ANY(:arr)
                ORDER BY sku, date, id
            """), {"arr": skus}).all()
            out: Dict[str, List[Dict[str, Any]]] = {}
            for r in rows:
                m = r._mapping
                out.setdefault(m["sku"], []).append({
                    "id": m["id"],
                    "date": m["d"],
                    "qty": int(m["qty"]),
                    "unit_cost": float(m["unit_cost"]),
                })
            return out
        else:
            qm = ",".join("?" for _ in skus)
            rows = c.execute(f"""
                SELECT id, sku, date, qty, unit_cost
                FROM batches WHERE sku IN ({qm})
                ORDER BY sku, date, id
            """, skus).fetchall()
            out: Dict[str, List[Dict[str, Any]]] = {}
            for r in rows:
                d = datetime.strptime(str(r["date"])[:10], "%Y-%m-%d").date()
                out.setdefault(r["sku"], []).append({
                    "id": r["id"],
                    "date": d,
                    "qty": int(r["qty"]),
                    "unit_cost": float(r["unit_cost"]),
                })
            return out

# ========= Модели входа =========
class OrderItemIn(BaseModel):
    sku: str
    qty: int
    unit_price: float
    commission_pct: float | None = None

class OrderIn(BaseModel):
    id: str
    date: str        # ISO 8601
    customer: str | None = None
    items: List[OrderItemIn]

class OrdersBulkIn(BaseModel):
    orders: List[OrderIn]

# ========= Роутер =========
def get_profit_fifo_router() -> APIRouter:
    router = APIRouter(tags=["profit"])

    @router.get("/db/ping")
    async def ping():
        _ensure_schema()
        return {"ok": True, "driver": "pg" if _USE_PG else "sqlite"}

    # ---- bulk upsert заказов (мутация, защищаем ключом)
    @router.post("/orders/upsert-bulk")
    async def upsert_orders_bulk(payload: OrdersBulkIn, _: bool = Depends(require_api_key)):
        _ensure_schema()
        ins_o = upd_o = ins_i = 0
        with _db() as c:
            for o in payload.orders:
                if _USE_PG:
                    existed = c.execute(_q("SELECT 1 FROM orders WHERE id = :id"), {"id": o.id}).first()
                    c.execute(_q("""
                        INSERT INTO orders(id, date, customer)
                        VALUES(:id, :date, :customer)
                        ON CONFLICT (id) DO UPDATE
                          SET date = EXCLUDED.date,
                              customer = EXCLUDED.customer
                    """), {"id": o.id, "date": o.date, "customer": o.customer})
                else:
                    existed = c.execute("SELECT 1 FROM orders WHERE id = ?", (o.id,)).fetchone()
                    c.execute("""
                        INSERT INTO orders(id, date, customer)
                        VALUES(?,?,?)
                        ON CONFLICT(id) DO UPDATE
                          SET date = excluded.date,
                              customer = excluded.customer
                    """, (o.id, o.date, o.customer))
                upd_o += 1 if existed else 0
                ins_o += 0 if existed else 1

                # Полная замена позиций
                if _USE_PG:
                    c.execute(_q("DELETE FROM order_items WHERE order_id = :id"), {"id": o.id})
                else:
                    c.execute("DELETE FROM order_items WHERE order_id = ?", (o.id,))

                for it in o.items:
                    if _USE_PG:
                        c.execute(_q("""
                            INSERT INTO order_items(order_id, sku, qty, unit_price, commission_pct)
                            VALUES(:oid, :sku, :qty, :price, :comm)
                        """), {
                            "oid": o.id,
                            "sku": it.sku.strip(),
                            "qty": int(it.qty),
                            "price": float(it.unit_price),
                            "comm": float(it.commission_pct) if it.commission_pct is not None else None,
                        })
                    else:
                        c.execute("""
                            INSERT INTO order_items(order_id, sku, qty, unit_price, commission_pct)
                            VALUES(?,?,?,?,?)
                        """, (
                            o.id,
                            it.sku.strip(),
                            int(it.qty),
                            float(it.unit_price),
                            float(it.commission_pct) if it.commission_pct is not None else None,
                        ))
                    ins_i += 1
        return {"status": "ok", "orders_inserted": ins_o, "orders_updated": upd_o, "items_inserted": ins_i}

    # ---- перестроить FIFO-реестр (мутация, защищаем ключом)
    @router.post("/rebuild-ledger")
    async def rebuild_ledger(
        date_from: Optional[str] = Query(None, description="YYYY-MM-DD"),
        date_to: Optional[str]   = Query(None, description="YYYY-MM-DD"),
        _: bool = Depends(require_api_key)
    ):
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
                    SELECT i.id
                    FROM order_items i
                    JOIN orders o ON o.id = i.order_id
                    WHERE 1=1 {cond}
                    ORDER BY o.date, i.id
                """), params).scalars().all()

                if ids:
                    c.execute(_q("DELETE FROM batch_consumption WHERE sale_item_id = ANY(:ids)"), {"ids": ids})

                rows = c.execute(_q(f"""
                    SELECT i.id AS sale_item_id, o.date AS dt, i.sku, i.qty
                    FROM order_items i
                    JOIN orders o ON o.id = i.order_id
                    WHERE 1=1 {cond}
                    ORDER BY o.date, i.id
                """), params).all()
                items = _rows(rows)
            else:
                cond = ""
                p: List[Any] = []
                if date_from:
                    cond += " AND date(substr(o.date,1,10)) >= date(?)"; p.append(date_from)
                if date_to:
                    cond += " AND date(substr(o.date,1,10)) <= date(?)"; p.append(date_to)

                ids = [r[0] for r in c.execute(f"""
                    SELECT i.id
                    FROM order_items i
                    JOIN orders o ON o.id = i.order_id
                    WHERE 1=1 {cond}
                    ORDER BY o.date, i.id
                """, p).fetchall()]

                if ids:
                    qm = ",".join("?" for _ in ids)
                    c.execute(f"DELETE FROM batch_consumption WHERE sale_item_id IN ({qm})", ids)

                rows = c.execute(f"""
                    SELECT i.id AS sale_item_id, o.date AS dt, i.sku, i.qty
                    FROM order_items i
                    JOIN orders o ON o.id = i.order_id
                    WHERE 1=1 {cond}
                    ORDER BY o.date, i.id
                """, p).fetchall()
                items = _rows(rows)

            if not items:
                return {"status": "ok", "recomputed": 0}

            # Подготовим очереди остатков по SKU (FIFO)
            skus = sorted({r["sku"] for r in items})
            batches = _batches_for_skus(skus)  # sku -> [{id,date,qty,unit_cost}]
            remains: Dict[str, List[Dict[str, Any]]] = {
                sku: [{"batch_id": b["id"], "date": b["date"], "remain": int(b["qty"]), "unit_cost": float(b["unit_cost"])}
                      for b in blist]
                for sku, blist in batches.items()
            }

            inserted = 0
            for it in items:
                sku = it["sku"]
                need = int(it["qty"])
                q = remains.get(sku, [])
                sale_dt = it["dt"]
                if isinstance(sale_dt, str):
                    sale_dt = datetime.fromisoformat(sale_dt.replace("Z", "+00:00"))

                while need > 0:
                    found = None
                    for row in q:
                        bd = row["date"]
                        if isinstance(bd, str):
                            bd = datetime.strptime(bd[:10], "%Y-%m-%d").date()
                        if row["remain"] > 0 and bd <= sale_dt.date():
                            found = row
                            break

                    if not found:
                        # дефицит — фиксируем расход с unit_cost=NULL
                        take = need
                        if _USE_PG:
                            c.execute(_q("""
                                INSERT INTO batch_consumption(sale_item_id, batch_id, sku, qty, unit_cost)
                                VALUES(:sid, 0, :sku, :qty, NULL)
                            """), {"sid": it["sale_item_id"], "sku": sku, "qty": take})
                        else:
                            c.execute("""
                                INSERT INTO batch_consumption(sale_item_id, batch_id, sku, qty, unit_cost)
                                VALUES(?,?,?,?,NULL)
                            """, (it["sale_item_id"], 0, sku, take))
                        inserted += 1
                        need = 0
                    else:
                        take = min(need, int(found["remain"]))
                        found["remain"] -= take
                        if _USE_PG:
                            c.execute(_q("""
                                INSERT INTO batch_consumption(sale_item_id, batch_id, sku, qty, unit_cost)
                                VALUES(:sid, :bid, :sku, :qty, :uc)
                            """), {
                                "sid": it["sale_item_id"],
                                "bid": found["batch_id"],
                                "sku": sku,
                                "qty": take,
                                "uc": found["unit_cost"],
                            })
                        else:
                            c.execute("""
                                INSERT INTO batch_consumption(sale_item_id, batch_id, sku, qty, unit_cost)
                                VALUES(?,?,?,?,?)
                            """, (it["sale_item_id"], found["batch_id"], sku, take, found["unit_cost"]))
                        inserted += 1
                        need -= take

            return {"status": "ok", "recomputed": inserted}

    # ---- сводная прибыль (FIFO)
    @router.get("/summary")
    async def summary(
        # поддерживаем оба варианта имён
        date_from: Optional[str] = Query(None, alias="date_from"),
        date_to: Optional[str]   = Query(None, alias="date_to"),
        start: Optional[str]     = Query(None, alias="start"),
        end: Optional[str]       = Query(None, alias="end"),
        group_by: str = Query("day", pattern="^(day|week|month|total)$"),
        use_bd: bool = Query(USE_BUSINESS_DAY),
        bd_start: str = Query(BUSINESS_DAY_START),
    ):
        _ensure_schema()
        df = (date_from or start or "").strip()
        dt = (date_to or end or "").strip()
        if not df or not dt:
            raise HTTPException(status_code=422, detail="date_from/start и date_to/end обязательны")

        with _db() as c:
            if _USE_PG:
                rows = c.execute(_q("""
                    SELECT o.date AS dt, i.id AS sale_item_id, i.sku, i.qty, i.unit_price, i.commission_pct
                    FROM order_items i
                    JOIN orders o ON o.id = i.order_id
                    WHERE o.date::date BETWEEN :df AND :dt
                    ORDER BY o.date, i.id
                """), {"df": df, "dt": dt}).all()
            else:
                rows = c.execute("""
                    SELECT o.date AS dt, i.id AS sale_item_id, i.sku, i.qty, i.unit_price, i.commission_pct
                    FROM order_items i
                    JOIN orders o ON o.id = i.order_id
                    WHERE date(substr(o.date,1,10)) BETWEEN date(?) AND date(?)
                    ORDER BY o.date, i.id
                """, (df, dt)).fetchall()
        items = _rows(rows)

        if not items:
            return {
                "currency": CURRENCY,
                "group_by": group_by,
                "rows": [],
                "total": {"revenue": 0, "commission": 0, "cost": 0, "profit": 0},
            }

        skus = sorted({r["sku"] for r in items})
        sku_cat = _sku_to_category(skus)
        cats = _categories()

        # Подтянем списания FIFO по позициям
        with _db() as c:
            ids = [it["sale_item_id"] for it in items]
            if not ids:
                cons = []
            elif _USE_PG:
                cons = c.execute(_q("""
                    SELECT sale_item_id, qty, unit_cost
                    FROM batch_consumption
                    WHERE sale_item_id = ANY(:ids)
                """), {"ids": ids}).all()
            else:
                qm = ",".join("?" for _ in ids)
                cons = c.execute(f"""
                    SELECT sale_item_id, qty, unit_cost
                    FROM batch_consumption
                    WHERE sale_item_id IN ({qm})
                """, ids).fetchall()
        cons = _rows(cons)
        by_sale: Dict[int, List[Tuple[int, Optional[float]]]] = {}
        for r in cons:
            by_sale.setdefault(int(r["sale_item_id"]), []).append(
                (int(r["qty"]), None if r["unit_cost"] is None else float(r["unit_cost"]))
            )

        # Агрегируем
        buckets: Dict[str, Dict[str, float]] = {}
        tot_rev = tot_comm = tot_cost = 0.0

        for it in items:
            # дата
            if isinstance(it["dt"], str):
                dtm = datetime.fromisoformat(it["dt"].replace("Z", "+00:00"))
            else:
                dtm = it["dt"]
            key = "total"
            if group_by != "total":
                d = _bucket_date(dtm, use_bd, bd_start)
                if group_by == "day":
                    key = d.isoformat()
                elif group_by == "week":
                    iso = d.isocalendar()
                    key = f"{iso.year}-W{iso.week:02d}"
                else:  # month
                    key = f"{d.year}-{d.month:02d}"

            qty = int(it["qty"])
            price = float(it["unit_price"])
            revenue = qty * price

            # комиссия: либо из позиции, либо из категории
            comm_pct = it.get("commission_pct")
            if comm_pct is None:
                cat = sku_cat.get(it["sku"], "")
                cinfo = cats.get(cat, {})
                comm_pct = float(
                    cinfo.get("base_percent", 0)
                    + cinfo.get("extra_percent", 0)
                    + cinfo.get("tax_percent", 0)
                )
            commission = revenue * (float(comm_pct) / 100.0)

            # себестоимость из FIFO
            cost = 0.0
            for q, uc in by_sale.get(int(it["sale_item_id"]), []):
                cost += (0.0 if uc is None else uc) * q

            a = buckets.setdefault(key, {"revenue": 0.0, "commission": 0.0, "cost": 0.0})
            a["revenue"] += revenue
            a["commission"] += commission
            a["cost"] += cost

            tot_rev += revenue
            tot_comm += commission
            tot_cost += cost

        rows_out = []
        for k in sorted(buckets.keys()):
            r = buckets[k]
            rows_out.append({
                "period": k,
                "revenue": round(r["revenue"], 2),
                "commission": round(r["commission"], 2),
                "cost": round(r["cost"], 2),
                "profit": round(r["revenue"] - r["commission"] - r["cost"], 2),
            })

        total = {
            "revenue": round(tot_rev, 2),
            "commission": round(tot_comm, 2),
            "cost": round(tot_cost, 2),
            "profit": round(tot_rev - tot_comm - tot_cost, 2),
        }
        return {"currency": CURRENCY, "group_by": group_by, "rows": rows_out, "total": total}

    # ---- ТОП SKU по прибыли
    @router.get("/by-sku")
    async def by_sku(
        date_from: Optional[str] = Query(None, alias="date_from"),
        date_to: Optional[str]   = Query(None, alias="date_to"),
        start: Optional[str]     = Query(None, alias="start"),
        end: Optional[str]       = Query(None, alias="end"),
        limit: int = Query(20, ge=1, le=200),
    ):
        _ensure_schema()
        df = (date_from or start or "").strip()
        dt = (date_to or end or "").strip()
        if not df or not dt:
            raise HTTPException(status_code=422, detail="date_from/start и date_to/end обязательны")

        with _db() as c:
            if _USE_PG:
                rows = c.execute(_q("""
                    SELECT o.date AS dt, i.id AS sale_item_id, i.sku, i.qty, i.unit_price, i.commission_pct
                    FROM order_items i
                    JOIN orders o ON o.id = i.order_id
                    WHERE o.date::date BETWEEN :df AND :dt
                """), {"df": df, "dt": dt}).all()
            else:
                rows = c.execute("""
                    SELECT o.date AS dt, i.id AS sale_item_id, i.sku, i.qty, i.unit_price, i.commission_pct
                    FROM order_items i
                    JOIN orders o ON o.id = i.order_id
                    WHERE date(substr(o.date,1,10)) BETWEEN date(?) AND date(?)
                """, (df, dt)).fetchall()
        items = _rows(rows)
        if not items:
            return {"rows": []}

        skus = sorted({r["sku"] for r in items})
        sku_cat = _sku_to_category(skus)
        cats = _categories()

        with _db() as c:
            ids = [it["sale_item_id"] for it in items]
            if not ids:
                cons = []
            elif _USE_PG:
                cons = c.execute(_q("""
                    SELECT sale_item_id, qty, unit_cost
                    FROM batch_consumption
                    WHERE sale_item_id = ANY(:ids)
                """), {"ids": ids}).all()
            else:
                qm = ",".join("?" for _ in ids)
                cons = c.execute(f"""
                    SELECT sale_item_id, qty, unit_cost
                    FROM batch_consumption
                    WHERE sale_item_id IN ({qm})
                """, ids).fetchall()
        cons = _rows(cons)
        by_sale: Dict[int, List[Tuple[int, Optional[float]]]] = {}
        for r in cons:
            by_sale.setdefault(int(r["sale_item_id"]), []).append(
                (int(r["qty"]), None if r["unit_cost"] is None else float(r["unit_cost"]))
            )

        acc: Dict[str, Dict[str, float]] = {}
        for it in items:
            sku = it["sku"]
            qty = int(it["qty"])
            price = float(it["unit_price"])
            revenue = qty * price

            comm_pct = it.get("commission_pct")
            if comm_pct is None:
                cat = sku_cat.get(sku, "")
                cinfo = cats.get(cat, {})
                comm_pct = float(
                    cinfo.get("base_percent", 0)
                    + cinfo.get("extra_percent", 0)
                    + cinfo.get("tax_percent", 0)
                )
            commission = revenue * (float(comm_pct) / 100.0)

            cost = sum((0.0 if uc is None else uc) * q for q, uc in by_sale.get(int(it["sale_item_id"]), []))

            a = acc.setdefault(sku, {"revenue": 0.0, "commission": 0.0, "cost": 0.0})
            a["revenue"] += revenue
            a["commission"] += commission
            a["cost"] += cost

        rows_out = [{
            "sku": sku,
            "revenue": round(v["revenue"], 2),
            "commission": round(v["commission"], 2),
            "cost": round(v["cost"], 2),
            "profit": round(v["revenue"] - v["commission"] - v["cost"], 2),
        } for sku, v in acc.items()]
        rows_out.sort(key=lambda x: x["profit"], reverse=True)
        return {"rows": rows_out[:limit]}

    return router
