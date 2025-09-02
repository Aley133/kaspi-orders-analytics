# app/api/profit_fifo.py
from __future__ import annotations

import os
from typing import List, Dict, Optional, Iterable, Any

from fastapi import APIRouter, Query
import psycopg
from psycopg.rows import dict_row

DB_URL = os.getenv("DATABASE_URL") or os.getenv("DB_URL")

def _pg():
    if not DB_URL:
        raise RuntimeError("DATABASE_URL is not set")
    return psycopg.connect(DB_URL, autocommit=False, row_factory=dict_row)

def _ensure_schema(con):
    cur = con.cursor()
    # На всякий: лёгкая обёртка bridge_sales (у тебя её создаёт bridge_v2)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS bridge_sales(
        order_id TEXT,
        order_code TEXT,
        date_utc_ms BIGINT,
        state TEXT,
        line_index INTEGER,
        sku TEXT,
        title TEXT,
        qty INTEGER,
        unit_price DOUBLE PRECISION,
        total_price DOUBLE PRECISION
    );
    """)
    # qty_sold в партиях (если нет)
    cur.execute("""
    DO $$
    BEGIN
      IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                       WHERE table_name='batches' AND column_name='qty_sold') THEN
        EXECUTE 'ALTER TABLE batches ADD COLUMN qty_sold INTEGER DEFAULT 0';
      END IF;
    END$$;
    """)
    # FIFO-леджер
    cur.execute("""
    CREATE TABLE IF NOT EXISTS profit_fifo_ledger(
        id BIGSERIAL PRIMARY KEY,
        order_id TEXT,
        order_code TEXT,
        date_utc_ms BIGINT,
        sku TEXT NOT NULL,
        line_index INTEGER DEFAULT 0,
        qty INTEGER NOT NULL,
        unit_price DOUBLE PRECISION DEFAULT 0,
        total_price DOUBLE PRECISION DEFAULT 0,
        batch_id BIGINT,
        batch_date TEXT,
        unit_cost DOUBLE PRECISION DEFAULT 0,
        commission_pct DOUBLE PRECISION DEFAULT 0,
        commission_amount DOUBLE PRECISION DEFAULT 0,
        cost_amount DOUBLE PRECISION DEFAULT 0,
        profit_amount DOUBLE PRECISION DEFAULT 0,
        created_at TIMESTAMPTZ DEFAULT now()
    );
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_fifo_order ON profit_fifo_ledger(order_code)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_fifo_sku ON profit_fifo_ledger(sku)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_fifo_batch ON profit_fifo_ledger(batch_id)")
    con.commit()

def _fetchall(cur, sql: str, params: Iterable[Any] = ()):
    cur.execute(sql, tuple(params or []))
    return list(cur.fetchall())

def _fetchone(cur, sql: str, params: Iterable[Any] = ()):
    cur.execute(sql, tuple(params or []))
    return cur.fetchone()

def _category_commission_sum(cur, sku: str) -> float:
    row = _fetchone(cur, """
        SELECT c.base_percent + c.extra_percent + c.tax_percent AS pct
          FROM products p
          JOIN categories c ON c.name = p.category
         WHERE p.sku = %s
         LIMIT 1
    """, [sku])
    return float(row["pct"]) if row and row.get("pct") is not None else 0.0

def _batches_for_sku(cur, sku: str) -> List[dict]:
    return _fetchall(cur, """
        SELECT id, sku, date, qty, COALESCE(qty_sold,0) AS qty_sold,
               COALESCE(unit_cost,0) AS unit_cost,
               COALESCE(commission_pct, NULL) AS commission_pct
          FROM batches
         WHERE sku = %s
         ORDER BY date ASC, id ASC
    """, [sku])

def _sales_for_codes(cur, codes: List[str]) -> List[dict]:
    fmt = ",".join(["%s"] * len(codes))
    return _fetchall(cur, f"""
        SELECT order_id, order_code, date_utc_ms, state, line_index,
               sku, title, COALESCE(qty,1) AS qty,
               COALESCE(unit_price,0) AS unit_price,
               COALESCE(total_price,0) AS total_price
          FROM bridge_sales
         WHERE order_code IN ({fmt})
         ORDER BY date_utc_ms ASC, order_code ASC, line_index ASC
    """, codes)

def _sales_for_period(cur, date_from: Optional[int], date_to: Optional[int]) -> List[dict]:
    where, params = [], []
    if date_from is not None:
        where.append("date_utc_ms >= %s")
        params.append(int(date_from))
    if date_to is not None:
        where.append("date_utc_ms <= %s")
        params.append(int(date_to))
    wh = "WHERE " + " AND ".join(where) if where else ""
    return _fetchall(cur, f"""
        SELECT order_id, order_code, date_utc_ms, state, line_index,
               sku, title, COALESCE(qty,1) AS qty,
               COALESCE(unit_price,0) AS unit_price,
               COALESCE(total_price,0) AS total_price
          FROM bridge_sales
         {wh}
         ORDER BY date_utc_ms ASC, order_code ASC, line_index ASC
    """, params)

def _update_qty_sold(cur, touched_batch_ids: Iterable[int]) -> None:
    ids = list({int(i) for i in touched_batch_ids if i is not None})
    if not ids:
        return
    fmt = ",".join(["%s"] * len(ids))
    cur.execute(f"""
        WITH agg AS (
            SELECT batch_id, COALESCE(SUM(qty),0) AS sold
              FROM profit_fifo_ledger
             WHERE batch_id IN ({fmt})
             GROUP BY batch_id
        )
        UPDATE batches b
           SET qty_sold = COALESCE(agg.sold,0)
          FROM agg
         WHERE b.id = agg.batch_id
    """, ids)

def _clear_ledger_for_orders(cur, codes: List[str]) -> List[int]:
    if not codes:
        return []
    fmt = ",".join(["%s"] * len(codes))
    rows = _fetchall(cur, f"SELECT DISTINCT batch_id FROM profit_fifo_ledger WHERE order_code IN ({fmt})", codes)
    touched = [r["batch_id"] for r in rows if r.get("batch_id") is not None]
    cur.execute(f"DELETE FROM profit_fifo_ledger WHERE order_code IN ({fmt})", codes)
    return touched

def _apply_fifo_for_sales(cur, sales: List[dict]) -> Dict[str, Any]:
    batches_cache: Dict[str, List[dict]] = {}
    touched_batches: set[int] = set()
    inserted = 0
    total_cost = 0.0
    total_comm = 0.0
    total_profit = 0.0

    for s in sales:
        sku = (s.get("sku") or "").strip()
        if not sku:
            continue
        qty_to_allocate = int(s.get("qty") or 1)
        if qty_to_allocate <= 0:
            continue

        if sku not in batches_cache:
            batches_cache[sku] = _batches_for_sku(cur, sku)

        batches = batches_cache[sku]
        local_usage: Dict[int, int] = {}

        for b in batches:
            if qty_to_allocate <= 0:
                break
            batch_id = int(b["id"])
            batch_qty = int(b.get("qty") or 0)
            batch_sold = int(b.get("qty_sold") or 0)
            already = local_usage.get(batch_id, 0)
            free = max(0, batch_qty - batch_sold - already)
            if free <= 0:
                continue

            take = min(free, qty_to_allocate)
            local_usage[batch_id] = already + take
            qty_to_allocate -= take

            commission_pct = b.get("commission_pct")
            if commission_pct is None:
                commission_pct = _category_commission_sum(cur, sku)

            unit_price = float(s.get("unit_price") or 0.0)
            total_for_piece = (float(s.get("total_price") or 0.0) / max(1, int(s.get("qty") or 1))) if (s.get("total_price") is not None) else unit_price
            part_total = total_for_piece * take

            unit_cost = float(b.get("unit_cost") or 0.0)
            cost_amount = unit_cost * take
            commission_amount = part_total * float(commission_pct or 0.0) / 100.0
            profit_amount = part_total - cost_amount - commission_amount

            cur.execute("""
                INSERT INTO profit_fifo_ledger(
                    order_id, order_code, date_utc_ms, sku, line_index,
                    qty, unit_price, total_price,
                    batch_id, batch_date, unit_cost,
                    commission_pct, commission_amount,
                    cost_amount, profit_amount
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, [
                s.get("order_id"), s.get("order_code"), s.get("date_utc_ms"), sku, int(s.get("line_index") or 0),
                take, unit_price, part_total,
                batch_id, b.get("date"), unit_cost,
                float(commission_pct or 0.0), commission_amount,
                cost_amount, profit_amount
            ])
            inserted += 1
            touched_batches.add(batch_id)
            total_cost += cost_amount
            total_comm += commission_amount
            total_profit += profit_amount

        # если не хватило остатков — фиксируем “недокомплект” для диагностики
        if qty_to_allocate > 0:
            unit_price = float(s.get("unit_price") or 0.0)
            total_for_piece = (float(s.get("total_price") or 0.0) / max(1, int(s.get("qty") or 1))) if (s.get("total_price") is not None) else unit_price
            part_total = total_for_piece * qty_to_allocate
            cur.execute("""
                INSERT INTO profit_fifo_ledger(
                    order_id, order_code, date_utc_ms, sku, line_index,
                    qty, unit_price, total_price,
                    batch_id, batch_date, unit_cost,
                    commission_pct, commission_amount,
                    cost_amount, profit_amount
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NULL,NULL,0,0,0,0,0)
            """, [
                s.get("order_id"), s.get("order_code"), s.get("date_utc_ms"), sku, int(s.get("line_index") or 0),
                qty_to_allocate, unit_price, part_total
            ])

    _update_qty_sold(cur, touched_batches)

    return {
        "inserted_rows": inserted,
        "touched_batches": len(touched_batches),
        "sum_cost": round(total_cost, 2),
        "sum_commission": round(total_comm, 2),
        "sum_profit": round(total_profit, 2),
    }

def get_profit_fifo_router() -> APIRouter:
    router = APIRouter(tags=["Profit FIFO"])

    @router.post("/bridge/fifo/rebuild")
    def fifo_rebuild(
        codes: Optional[str] = Query(None, description="CSV номеров заказов"),
        date_from_ms: Optional[int] = Query(None, description="bridge_sales.date_utc_ms (>=)"),
        date_to_ms: Optional[int] = Query(None, description="bridge_sales.date_utc_ms (<=)"),
        dry_run: int = Query(0, description="1=не коммитить"),
    ):
        with _pg() as con:
            _ensure_schema(con)
            cur = con.cursor()
            if codes:
                lst = [c.strip() for c in codes.split(",") if c.strip()]
                if not lst:
                    return {"ok": True, "seen": 0, "cleared": 0, "inserted": 0, "examples": {}}
                _clear_ledger_for_orders(cur, lst)
                sales = _sales_for_codes(cur, lst)
            else:
                sales = _sales_for_period(cur, date_from_ms, date_to_ms)
                if date_from_ms or date_to_ms:
                    codes_list = sorted({s["order_code"] for s in sales if s.get("order_code")})
                    _clear_ledger_for_orders(cur, codes_list)

            stats = _apply_fifo_for_sales(cur, sales)
            if dry_run:
                con.rollback()
            else:
                con.commit()

            example = _fetchall(cur, "SELECT * FROM profit_fifo_ledger ORDER BY id DESC LIMIT 5")
            return {"ok": True, "seen": len(sales), "inserted": stats.get("inserted_rows", 0), "stats": stats, "examples": example}

    @router.get("/bridge/fifo/ledger")
    def fifo_ledger(
        codes: Optional[str] = Query(None),
        limit: int = Query(200),
    ):
        with _pg() as con:
            _ensure_schema(con)
            cur = con.cursor()
            if codes:
                lst = [c.strip() for c in codes.split(",") if c.strip()]
                if not lst:
                    return {"items": []}
                fmt = ",".join(["%s"] * len(lst))
                rows = _fetchall(cur, f"""
                    SELECT *
                      FROM profit_fifo_ledger
                     WHERE order_code IN ({fmt})
                     ORDER BY date_utc_ms ASC, order_code ASC, line_index ASC, id ASC
                     LIMIT %s
                """, lst + [limit])
            else:
                rows = _fetchall(cur, """
                    SELECT * FROM profit_fifo_ledger
                     ORDER BY id DESC LIMIT %s
                """, [limit])
            return {"items": rows}

    @router.post("/bridge/fifo/clear")
    def fifo_clear(
        codes: Optional[str] = Query(None),
    ):
        with _pg() as con:
            _ensure_schema(con)
            cur = con.cursor()
            lst = [c.strip() for c in (codes or "").split(",") if c.strip()]
            if not lst:
                return {"ok": True, "deleted": 0}
            fmt = ",".join(["%s"] * len(lst))
            cur.execute(f"DELETE FROM profit_fifo_ledger WHERE order_code IN ({fmt})", lst)
            con.commit()
            return {"ok": True, "deleted_orders": len(lst)}

    return router
