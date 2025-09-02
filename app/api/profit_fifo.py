# app/api/profit_fifo.py
from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Set

from fastapi import APIRouter, HTTPException, Query
import psycopg
from psycopg.rows import dict_row

# ──────────────────────────────────────────────────────────────────────────────
# DB: нормализация URL для прямого psycopg
# ──────────────────────────────────────────────────────────────────────────────
_RAW_URL = os.getenv("DATABASE_URL") or os.getenv("DB_URL")

def _normalize_pg_url(u: str | None) -> str:
    if not u:
        raise RuntimeError("DATABASE_URL/DB_URL is not set")
    u = u.strip()
    # SQLAlchemy style → libpq
    if u.startswith("postgresql+"):
        u = "postgresql://" + u.split("://", 1)[1]
    # устаревшее postgres:// → postgresql://
    if u.startswith("postgres://"):
        u = "postgresql://" + u.split("://", 1)[1]
    return u

def _pg():
    url = _normalize_pg_url(_RAW_URL)
    return psycopg.connect(url, autocommit=False, row_factory=dict_row)

# ──────────────────────────────────────────────────────────────────────────────
# SQL helpers
# ──────────────────────────────────────────────────────────────────────────────
def _fetchall(cur, sql: str, params: Iterable[Any] = ()) -> List[dict]:
    cur.execute(sql, tuple(params or []))
    return list(cur.fetchall())

def _fetchone(cur, sql: str, params: Iterable[Any] = ()) -> Optional[dict]:
    cur.execute(sql, tuple(params or []))
    return cur.fetchone()

# ──────────────────────────────────────────────────────────────────────────────
# Schema bootstrap
# ──────────────────────────────────────────────────────────────────────────────
def _ensure_schema(con) -> None:
    """
    Гарантируем:
      - в batches есть qty_sold
      - таблица profit_fifo_ledger и индексы
      - уникальный индекс (order_code, line_index, batch_id) для идемпотентности
      - VIEW bridge_sales поверх bridge_lines (если таблицы bridge_sales нет)
    """
    cur = con.cursor()

    # qty_sold в партиях (если нет)
    cur.execute("""
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
         WHERE table_name='batches' AND column_name='qty_sold'
      ) THEN
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
        batch_date DATE,
        unit_cost DOUBLE PRECISION DEFAULT 0,
        commission_pct DOUBLE PRECISION DEFAULT 0,
        commission_amount DOUBLE PRECISION DEFAULT 0,
        cost_amount DOUBLE PRECISION DEFAULT 0,
        profit_amount DOUBLE PRECISION DEFAULT 0,
        created_at TIMESTAMPTZ DEFAULT now()
    );
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_fifo_order ON profit_fifo_ledger(order_code)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_fifo_sku   ON profit_fifo_ledger(sku)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_fifo_batch ON profit_fifo_ledger(batch_id)")

    # Уникальный индекс для UPSERT (идемпотентность)
    cur.execute("""
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM pg_indexes WHERE indexname = 'uq_fifo_code_line_batch'
      ) THEN
        EXECUTE 'CREATE UNIQUE INDEX uq_fifo_code_line_batch
                 ON profit_fifo_ledger(order_code, line_index, batch_id)';
      END IF;
    END$$;
    """)

    # Совместимость: если нет таблицы/вьюхи bridge_sales — создаём VIEW поверх bridge_lines
    cur.execute("""
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables WHERE table_name='bridge_sales'
      ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.views  WHERE table_name='bridge_sales'
      ) AND EXISTS (
        SELECT 1 FROM information_schema.tables WHERE table_name='bridge_lines'
      ) THEN
        EXECUTE $V$
          CREATE VIEW bridge_sales AS
          SELECT order_id,
                 order_code,
                 date_utc_ms,
                 state,
                 line_index,
                 sku,
                 title,
                 qty,
                 unit_price,
                 total_price
            FROM bridge_lines
        $V$;
      END IF;
    END$$;
    """)

    con.commit()

# ──────────────────────────────────────────────────────────────────────────────
# Misc helpers
# ──────────────────────────────────────────────────────────────────────────────
def _iso_to_day_ms(s: str, end: bool = False) -> int:
    """YYYY-MM-DD → миллисекунды UTC. end=True → конец дня."""
    dt = datetime.fromisoformat(s).replace(tzinfo=timezone.utc)
    if end:
        return int((dt.timestamp() + 86399.999) * 1000)
    return int(dt.timestamp() * 1000)

def _category_commission_sum(cur, sku: str) -> float:
    row = _fetchone(cur, """
        SELECT COALESCE(c.base_percent,0) + COALESCE(c.extra_percent,0) + COALESCE(c.tax_percent,0) AS pct
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
               commission_pct
          FROM batches
         WHERE sku = %s
         ORDER BY date ASC, id ASC
    """, [sku])

def _sales_from_bridge_by_codes(cur, codes: List[str]) -> List[dict]:
    """Берём продажи из bridge_sales (view/table)."""
    if not codes:
        return []
    fmt = ",".join(["%s"] * len(codes))
    # сначала пробуем view/таблицу bridge_sales
    try:
        return _fetchall(cur, f"""
            SELECT order_id, order_code, date_utc_ms, state, line_index,
                   sku, title, COALESCE(qty,1) AS qty,
                   COALESCE(unit_price,0) AS unit_price,
                   COALESCE(total_price,0) AS total_price
              FROM bridge_sales
             WHERE order_code IN ({fmt})
             ORDER BY date_utc_ms ASC, order_code ASC, line_index ASC
        """, codes)
    except Exception:
        # fallback на bridge_lines (если нет view/table)
        return _fetchall(cur, f"""
            SELECT order_id, order_code, date_utc_ms, state, line_index,
                   sku, title, COALESCE(qty,1) AS qty,
                   COALESCE(unit_price,0) AS unit_price,
                   COALESCE(total_price,0) AS total_price
              FROM bridge_lines
             WHERE order_code IN ({fmt})
             ORDER BY date_utc_ms ASC, order_code ASC, line_index ASC
        """, codes)

def _codes_from_period(cur, date_from_iso: str, date_to_iso: str) -> List[str]:
    a = _iso_to_day_ms(date_from_iso, end=False)
    b = _iso_to_day_ms(date_to_iso, end=True)
    rows = _fetchall(cur, """
        SELECT DISTINCT order_code
          FROM bridge_lines
         WHERE order_code IS NOT NULL
           AND date_utc_ms BETWEEN %s AND %s
         ORDER BY order_code
    """, [a, b])
    return [r["order_code"] for r in rows if r.get("order_code")]

def _already_allocated(cur, order_code: str, line_index: int) -> int:
    row = _fetchone(cur, """
        SELECT COALESCE(SUM(qty),0) AS q
          FROM profit_fifo_ledger
         WHERE order_code = %s AND line_index = %s
    """, [order_code, int(line_index)])
    return int(row["q"]) if row else 0

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

# ──────────────────────────────────────────────────────────────────────────────
# Core FIFO (идемпотентно с UPSERT)
# ──────────────────────────────────────────────────────────────────────────────
def _apply_fifo_for_sales(cur, sales: List[dict]) -> Dict[str, Any]:
    batches_cache: Dict[str, List[dict]] = {}
    touched_batches: Set[int] = set()
    inserted_rows = 0
    gaps: List[Dict[str, Any]] = []
    sum_cost = 0.0
    sum_comm = 0.0
    sum_profit = 0.0

    for s in sales:
        sku = (s.get("sku") or "").strip()
        if not sku:
            continue
        line_index = int(s.get("line_index") or 0)
        order_code = (s.get("order_code") or "").strip()
        if not order_code:
            continue

        qty_total = int(s.get("qty") or 1)
        if qty_total <= 0:
            continue

        already = _already_allocated(cur, order_code, line_index)
        need = qty_total - already
        if need <= 0:
            continue

        if sku not in batches_cache:
            batches_cache[sku] = _batches_for_sku(cur, sku)
        batches = batches_cache[sku]

        unit_price = float(s.get("unit_price") or 0.0)
        line_total = float(s.get("total_price") or 0.0)
        revenue_per_piece = (line_total / max(1, qty_total)) if line_total > 0 else unit_price

        local_usage: Dict[int, int] = {}
        for b in batches:
            if need <= 0:
                break
            bid = int(b["id"])
            batch_qty = int(b.get("qty") or 0)
            batch_sold = int(b.get("qty_sold") or 0)
            used_here = int(local_usage.get(bid, 0))
            free = max(0, batch_qty - batch_sold - used_here)
            if free <= 0:
                continue

            take = min(free, need)
            if take <= 0:
                continue

            commission_pct = b.get("commission_pct")
            if commission_pct is None:
                commission_pct = _category_commission_sum(cur, sku)
            commission_pct = float(commission_pct or 0.0)

            unit_cost = float(b.get("unit_cost") or 0.0)
            part_revenue = revenue_per_piece * take
            cost_amount = unit_cost * take
            commission_amount = part_revenue * (commission_pct / 100.0)
            profit_amount = part_revenue - commission_amount - cost_amount

            # UPSERT по уникальному индексу
            cur.execute("""
                INSERT INTO profit_fifo_ledger(
                    order_id, order_code, date_utc_ms, sku, line_index,
                    qty, unit_price, total_price,
                    batch_id, batch_date, unit_cost,
                    commission_pct, commission_amount, cost_amount, profit_amount
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (order_code, line_index, batch_id) DO UPDATE
                   SET qty = profit_fifo_ledger.qty + EXCLUDED.qty,
                       unit_price = EXCLUDED.unit_price,
                       total_price = profit_fifo_ledger.total_price + EXCLUDED.total_price,
                       commission_pct = EXCLUDED.commission_pct,
                       commission_amount = profit_fifo_ledger.commission_amount + EXCLUDED.commission_amount,
                       cost_amount = profit_fifo_ledger.cost_amount + EXCLUDED.cost_amount,
                       profit_amount = profit_fifo_ledger.profit_amount + EXCLUDED.profit_amount
            """, [
                s.get("order_id"), order_code, s.get("date_utc_ms"), sku, line_index,
                take, unit_price, (revenue_per_piece * take),
                bid, b.get("date"), unit_cost,
                commission_pct, commission_amount, cost_amount, profit_amount
            ])

            inserted_rows += 1
            local_usage[bid] = used_here + take
            touched_batches.add(bid)

            sum_cost += cost_amount
            sum_comm += commission_amount
            sum_profit += profit_amount
            need -= take

        if need > 0:
            gaps.append({
                "order_code": order_code,
                "line_index": line_index,
                "sku": sku,
                "not_covered_qty": need
            })

    _update_qty_sold(cur, touched_batches)

    return {
        "inserted_rows": inserted_rows,
        "sum_cost": round(sum_cost, 2),
        "sum_commission": round(sum_comm, 2),
        "sum_profit": round(sum_profit, 2),
        "gaps": gaps,
    }

def _clear_ledger_for_codes(cur, codes: List[str]) -> List[int]:
    if not codes:
        return []
    fmt = ",".join(["%s"] * len(codes))
    rows = _fetchall(cur, f"""
        SELECT DISTINCT batch_id
          FROM profit_fifo_ledger
         WHERE order_code IN ({fmt})
    """, codes)
    touched = [int(r["batch_id"]) for r in rows if r.get("batch_id") is not None]
    cur.execute(f"DELETE FROM profit_fifo_ledger WHERE order_code IN ({fmt})", codes)
    return touched

# ──────────────────────────────────────────────────────────────────────────────
# FastAPI Router
# ──────────────────────────────────────────────────────────────────────────────
def get_profit_fifo_router() -> APIRouter:
    router = APIRouter(tags=["Profit FIFO"])

    @router.post("/bridge/fifo/apply")
    def fifo_apply(
        codes: Optional[str] = Query(None, description="CSV order_code"),
        date_from: Optional[str] = Query(None, description="YYYY-MM-DD"),
        date_to: Optional[str] = Query(None, description="YYYY-MM-DD"),
    ):
        if not codes and not (date_from and date_to):
            raise HTTPException(400, "Передайте codes=... или date_from/date_to")

        with _pg() as con:
            _ensure_schema(con)
            cur = con.cursor()

            if codes:
                codes_list = [c.strip() for c in codes.split(",") if c.strip()]
            else:
                try:
                    codes_list = _codes_from_period(cur, date_from, date_to)
                except Exception:
                    raise HTTPException(400, "Неверный формат date_from/date_to, ожидается YYYY-MM-DD")

            if not codes_list:
                return {"ok": True, "processed_orders": 0, "inserted": 0, "stats": {}, "gaps": []}

            sales = _sales_from_bridge_by_codes(cur, codes_list)
            stats = _apply_fifo_for_sales(cur, sales)
            con.commit()

            return {
                "ok": True,
                "processed_orders": len(set(codes_list)),
                "seen_lines": len(sales),
                "inserted": stats.get("inserted_rows", 0),
                "stats": {k: v for k, v in stats.items() if k.startswith("sum_")},
                "gaps": stats.get("gaps", []),
            }

    @router.post("/bridge/fifo/rebuild")
    def fifo_rebuild(
        codes: Optional[str] = Query(None, description="CSV order_code; если задано — даты игнорируются"),
        date_from: Optional[str] = Query(None, description="YYYY-MM-DD"),
        date_to: Optional[str] = Query(None, description="YYYY-MM-DD"),
        dry_run: int = Query(0, description="1 = откатить транзакцию"),
    ):
        with _pg() as con:
            _ensure_schema(con)
            cur = con.cursor()

            if codes:
                codes_list = [c.strip() for c in codes.split(",") if c.strip()]
            else:
                if not (date_from and date_to):
                    raise HTTPException(400, "Передайте codes=... или date_from/date_to")
                codes_list = _codes_from_period(cur, date_from, date_to)

            touched = _clear_ledger_for_codes(cur, codes_list)
            if touched:
                _update_qty_sold(cur, touched)

            sales = _sales_from_bridge_by_codes(cur, codes_list)
            stats = _apply_fifo_for_sales(cur, sales)

            if dry_run:
                con.rollback()
            else:
                con.commit()

            ex = _fetchall(cur, "SELECT * FROM profit_fifo_ledger ORDER BY id DESC LIMIT 5")
            return {
                "ok": True,
                "processed_orders": len(set(codes_list)),
                "seen_lines": len(sales),
                "inserted": stats.get("inserted_rows", 0),
                "stats": {k: v for k, v in stats.items() if k.startswith("sum_")},
                "gaps": stats.get("gaps", []),
                "examples": ex,
                "dry_run": int(dry_run),
            }

    @router.post("/bridge/fifo/recalc-batches")
    def fifo_recalc_batches():
        with _pg() as con:
            _ensure_schema(con)
            cur = con.cursor()
            cur.execute("""
                UPDATE batches b
                   SET qty_sold = COALESCE(agg.sold,0)
                  FROM (
                    SELECT batch_id, COALESCE(SUM(qty),0) AS sold
                      FROM profit_fifo_ledger
                     WHERE batch_id IS NOT NULL
                     GROUP BY batch_id
                  ) agg
                 WHERE b.id = agg.batch_id
            """)
            con.commit()
            return {"ok": True}

    @router.get("/bridge/fifo/ledger")
    def fifo_ledger(
        codes: Optional[str] = Query(None, description="CSV order_code"),
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
    def fifo_clear(codes: Optional[str] = Query(None, description="CSV order_code")):
        lst = [c.strip() for c in (codes or "").split(",") if c.strip()]
        if not lst:
            return {"ok": True, "deleted_orders": 0}
        with _pg() as con:
            _ensure_schema(con)
            cur = con.cursor()
            touched = _clear_ledger_for_codes(cur, lst)
            con.commit()
            return {"ok": True, "deleted_orders": len(lst), "touched_batches": len(touched)}

    return router
