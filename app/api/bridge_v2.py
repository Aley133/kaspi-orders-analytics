from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status

# --------- конфиг / окружение ----------
REQ_API_KEY = (os.getenv("BRIDGE_API_KEY") or os.getenv("API_KEY") or "").strip() or None

# Подключение к БД:
# 1) PROFIT_DB_URL (желательно тот же Neon, что и products.py)
# 2) DATABASE_URL (резерв)
# 3) sqlite-файл data/kaspi-orders.sqlite3
PROFIT_DB_URL = (os.getenv("PROFIT_DB_URL") or "").strip()
DATABASE_URL = (os.getenv("DATABASE_URL") or "").strip()
DB_URL = PROFIT_DB_URL or DATABASE_URL

IS_PG = bool(DB_URL) and DB_URL.startswith(("postgresql://", "postgresql+psycopg://"))

if IS_PG:
    import psycopg
    from psycopg.rows import dict_row
else:
    import sqlite3

router = APIRouter(tags=["bridge_v2"])
PFX = ("/profit/bridge", "/bridge")  # оба работают

# --------- security ----------
def require_api_key(request: Request):
    if not REQ_API_KEY:
        return True
    provided = request.headers.get("X-API-Key") or request.query_params.get("api_key")
    if provided != REQ_API_KEY:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")
    return True

# --------- DB helpers ----------
@dataclass
class DB:
    is_pg: bool

    def connect(self):
        if self.is_pg:
            return psycopg.connect(DB_URL, autocommit=True, row_factory=dict_row)
        # sqlite
        path = os.getenv("BRIDGE_DB_PATH", "data/kaspi-orders.sqlite3")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        con = sqlite3.connect(path)
        con.row_factory = sqlite3.Row
        return con

DBH = DB(IS_PG)

def _exec(con, sql: str, params: Iterable[Any] | None = None):
    if IS_PG:
        with con.cursor() as cur:
            cur.execute(sql, tuple(params or []))
    else:
        con.execute(sql.replace("%s", "?"), tuple(params or []))

def _fetchone(con, sql: str, params: Iterable[Any] | None = None) -> Optional[dict]:
    if IS_PG:
        with con.cursor() as cur:
            cur.execute(sql, tuple(params or []))
            r = cur.fetchone()
            return dict(r) if r else None
    else:
        cur = con.execute(sql.replace("%s", "?"), tuple(params or []))
        r = cur.fetchone()
        return dict(r) if r else None

def _fetchall(con, sql: str, params: Iterable[Any] | None = None) -> List[dict]:
    if IS_PG:
        with con.cursor() as cur:
            cur.execute(sql, tuple(params or []))
            rows = cur.fetchall()
            return [dict(r) for r in rows]
    else:
        cur = con.execute(sql.replace("%s", "?"), tuple(params or []))
        rows = cur.fetchall()
        return [dict(r) for r in rows]

# --------- init schema (bridge_lines только) ----------
def _init_sql_pg(con):
    _exec(con, """
    CREATE TABLE IF NOT EXISTS bridge_lines(
      order_id     TEXT NOT NULL,
      order_code   TEXT,
      state        TEXT,
      date_utc_ms  BIGINT,
      sku          TEXT,
      title        TEXT,
      qty          INTEGER DEFAULT 1,
      unit_price   DOUBLE PRECISION DEFAULT 0,
      total_price  DOUBLE PRECISION DEFAULT 0,
      line_index   INTEGER NOT NULL,
      created_at   BIGINT DEFAULT (extract(epoch from now())*1000)::bigint,
      updated_at   BIGINT DEFAULT (extract(epoch from now())*1000)::bigint,
      PRIMARY KEY(order_id, line_index)
    )""")
    _exec(con, "CREATE INDEX IF NOT EXISTS ix_lines_date  ON bridge_lines(date_utc_ms)")
    _exec(con, "CREATE INDEX IF NOT EXISTS ix_lines_state ON bridge_lines(state)")
    _exec(con, "CREATE INDEX IF NOT EXISTS ix_lines_sku   ON bridge_lines(sku)")
    _exec(con, "CREATE INDEX IF NOT EXISTS ix_lines_code  ON bridge_lines(order_code)")

def _init_sql_sqlite(con):
    _exec(con, """
    CREATE TABLE IF NOT EXISTS bridge_lines(
      order_id     TEXT NOT NULL,
      order_code   TEXT,
      state        TEXT,
      date_utc_ms  INTEGER,
      sku          TEXT,
      title        TEXT,
      qty          INTEGER DEFAULT 1,
      unit_price   REAL    DEFAULT 0,
      total_price  REAL    DEFAULT 0,
      line_index   INTEGER NOT NULL,
      created_at   INTEGER DEFAULT (strftime('%s','now')*1000),
      updated_at   INTEGER DEFAULT (strftime('%s','now')*1000),
      PRIMARY KEY(order_id, line_index)
    )""")
    _exec(con, "CREATE INDEX IF NOT EXISTS ix_lines_date  ON bridge_lines(date_utc_ms)")
    _exec(con, "CREATE INDEX IF NOT EXISTS ix_lines_state ON bridge_lines(state)")
    _exec(con, "CREATE INDEX IF NOT EXISTS ix_lines_sku   ON bridge_lines(sku)")
    _exec(con, "CREATE INDEX IF NOT EXISTS ix_lines_code  ON bridge_lines(order_code)")

def _init_db():
    con = DBH.connect()
    try:
        if IS_PG:
            _init_sql_pg(con)
        else:
            _init_sql_sqlite(con)
    finally:
        con.close()

_init_db()

# --------- utils ----------
def _parse_csv(raw: Optional[str]) -> List[str]:
    if not raw:
        return []
    return [p.strip() for p in str(raw).split(",") if p.strip()]

def _ms_to_iso(ms: Optional[int]) -> Optional[str]:
    if ms is None:
        return None
    return time.strftime("%Y-%m-%d %H:%M", time.gmtime(int(ms) // 1000))

def _num(x) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0

# --------- cost / commission lookups ----------
# Схема из products.py:
# batches(sku TEXT, unit_cost REAL, commission_pct REAL, doc_date_utc_ms BIGINT, doc_type TEXT, ...)
# products(sku TEXT PRIMARY KEY, category TEXT, commission_pct REAL, ...)
# categories(category TEXT PRIMARY KEY, base_percent REAL, extra_percent REAL, tax_percent REAL, ...)

def _latest_batch(con, sku: str) -> Optional[dict]:
    # Берём самую свежую партию по времени документа
    return _fetchone(
        con,
        "SELECT unit_cost, commission_pct FROM batches WHERE sku=%s ORDER BY doc_date_utc_ms DESC LIMIT 1",
        [sku],
    )

def _product_commission(con, sku: str) -> Optional[float]:
    r = _fetchone(con, "SELECT commission_pct FROM products WHERE sku=%s", [sku])
    v = r and r.get("commission_pct")
    return float(v) if v is not None else None

def _category_commission(con, sku: str) -> Optional[float]:
    r = _fetchone(con,
        """SELECT c.base_percent, c.extra_percent, c.tax_percent
           FROM products p
           JOIN categories c ON c.category = p.category
           WHERE p.sku=%s""",
        [sku],
    )
    if not r:
        return None
    try:
        return float(r.get("base_percent", 0) or 0) + float(r.get("extra_percent", 0) or 0) + float(r.get("tax_percent", 0) or 0)
    except Exception:
        return None

def _cost_commission_for_sku(con, sku: str) -> Tuple[float, float]:
    # unit_cost: из batches.unit_cost
    # commission_pct: batches.commission_pct → products.commission_pct → categories.sum%
    unit_cost = 0.0
    commission_pct: Optional[float] = None

    b = _latest_batch(con, sku)
    if b:
        unit_cost = float(b.get("unit_cost") or 0.0)
        if b.get("commission_pct") is not None:
            try:
                commission_pct = float(b["commission_pct"])
            except Exception:
                commission_pct = None

    if commission_pct is None:
        commission_pct = _product_commission(con, sku)

    if commission_pct is None:
        commission_pct = _category_commission(con, sku)

    return unit_cost, float(commission_pct or 0.0)

# --------- models ----------
from pydantic import BaseModel, Field

class BridgeLineIn(BaseModel):
    id: str
    code: Optional[str] = None
    date: Optional[Any] = None
    state: Optional[str] = None
    sku: Optional[str] = None
    title: Optional[str] = None
    qty: Optional[int] = 1
    unit_price: Optional[float] = 0.0
    total_price: Optional[float] = 0.0
    amount: Optional[float] = None
    line_index: Optional[int] = None

class OrderItemOut(BaseModel):
    sku: Optional[str] = None
    title: Optional[str] = None
    qty: int = 1
    unit_price: float = 0.0
    total_price: float = 0.0
    cost: Optional[float] = None
    commission: Optional[float] = None
    profit: Optional[float] = None

class OrderOut(BaseModel):
    order_id: str
    order_code: Optional[str] = None
    state: Optional[str] = None
    date: Optional[str] = None
    items: List[OrderItemOut] = Field(default_factory=list)
    totals: Dict[str, float] = Field(default_factory=dict)

class OrdersResponse(BaseModel):
    orders: List[OrderOut] = Field(default_factory=list)
    source_used: str = "bridge_v2"
    stats: Dict[str, float] = Field(default_factory=dict)

# --------- endpoints ----------
@router.get(f"{PFX[0]}/ping")
@router.get(f"{PFX[1]}/ping")
def ping():
    con = DBH.connect()
    try:
        c = _fetchone(con, "SELECT COUNT(*) AS n FROM bridge_lines") or {"n": 0}
        # Проверим, доступна ли продуктивная схема (batches/products/categories)
        has_batches = _fetchone(con,
            "SELECT 1 AS ok FROM information_schema.tables WHERE table_name='batches'"
        ) if IS_PG else _fetchone(con,
            "SELECT 1 AS ok FROM sqlite_master WHERE type='table' AND name='batches'"
        )
        return {
            "ok": True,
            "driver": "postgres" if IS_PG else "sqlite",
            "lines": int(c["n"]),
            "has_batches": bool(has_batches),
            "ts": int(time.time() * 1000),
        }
    finally:
        con.close()

@router.post(f"{PFX[0]}/sync-by-ids")
@router.post(f"{PFX[1]}/sync-by-ids")
def sync_by_ids(items: List[BridgeLineIn], _: bool = Depends(require_api_key)):
    if not items:
        return {"inserted": 0, "updated": 0, "skipped": 0}

    con = DBH.connect()
    try:
        if not IS_PG:
            _exec(con, "PRAGMA journal_mode=WAL;")
            _exec(con, "PRAGMA synchronous=NORMAL;")

        inserted = 0
        updated = 0
        skipped = 0
        counters: Dict[str, int] = {}

        sql = """
        INSERT INTO bridge_lines
          (order_id, order_code, state, date_utc_ms, sku, title, qty, unit_price, total_price, line_index, created_at, updated_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                CAST(EXTRACT(EPOCH FROM NOW())*1000 AS BIGINT), CAST(EXTRACT(EPOCH FROM NOW())*1000 AS BIGINT))
        ON CONFLICT (order_id, line_index) DO UPDATE SET
          order_code=excluded.order_code,
          state=excluded.state,
          date_utc_ms=excluded.date_utc_ms,
          sku=excluded.sku,
          title=excluded.title,
          qty=excluded.qty,
          unit_price=excluded.unit_price,
          total_price=excluded.total_price,
          updated_at=CAST(EXTRACT(EPOCH FROM NOW())*1000 AS BIGINT)
        """
        if not IS_PG:
            sql = """
            INSERT INTO bridge_lines
              (order_id, order_code, state, date_utc_ms, sku, title, qty, unit_price, total_price, line_index, created_at, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?, strftime('%s','now')*1000, strftime('%s','now')*1000)
            ON CONFLICT(order_id,line_index) DO UPDATE SET
              order_code=excluded.order_code,
              state=excluded.state,
              date_utc_ms=excluded.date_utc_ms,
              sku=excluded.sku,
              title=excluded.title,
              qty=excluded.qty,
              unit_price=excluded.unit_price,
              total_price=excluded.total_price,
              updated_at=strftime('%s','now')*1000
            """

        cur = con.cursor() if IS_PG else con.cursor()
        for it in items:
            order_id = (it.id or "").strip()
            if not order_id:
                skipped += 1
                continue

            order_code = (it.code or "").strip() or None
            state = (it.state or "").strip() or None
            date_ms = None
            # допускаем iso/epoch/epoch_ms
            try:
                v = str(it.date).strip()
                if v.endswith("Z"):
                    import datetime as _dt
                    dt = _dt.datetime.fromisoformat(v.replace("Z","+00:00"))
                    date_ms = int(dt.timestamp()*1000)
                else:
                    n = int(v)
                    date_ms = n if n >= 10_000_000_000 else n*1000
            except Exception:
                date_ms = None

            sku = (it.sku or "").strip() or None
            title = (it.title or "").strip() or None

            try:
                qty = int(it.qty or 1)
            except Exception:
                qty = 1

            total = None
            if it.total_price is not None:
                total = float(it.total_price)
            elif it.amount is not None:
                total = float(it.amount)
            elif it.unit_price is not None:
                try:
                    total = float(it.unit_price) * qty
                except Exception:
                    total = None
            if total is None:
                total = 0.0

            unit = None
            if it.unit_price is not None:
                try:
                    unit = float(it.unit_price)
                except Exception:
                    unit = None
            if unit is None:
                unit = float(total) / max(1, qty)

            if it.line_index is not None:
                line_index = int(it.line_index)
            else:
                line_index = counters.get(order_id, 0)
                counters[order_id] = line_index + 1

            params = (order_id, order_code, state, date_ms, sku, title, qty, unit, total, line_index)
            cur.execute(sql.replace("%s", "%s") if IS_PG else sql.replace("%s", "?"), params)
            updated += 1

        if not IS_PG:
            con.commit()

        processed = len(items) - skipped
        inserted = max(0, processed - updated)
        return {"inserted": inserted, "updated": updated, "skipped": skipped}
    finally:
        con.close()

def _collect_orders(where_sql: str, params: List[Any], order_dir: str) -> OrdersResponse:
    con = DBH.connect()
    try:
        o_rows = _fetchall(con, f"""
            SELECT order_id, order_code, MIN(date_utc_ms) AS date_utc_ms, MAX(state) as state
            FROM bridge_lines
            WHERE {where_sql}
            GROUP BY order_id, order_code
            ORDER BY date_utc_ms {order_dir}
        """, params)

        out: List[OrderOut] = []
        total_lines = 0
        revenue_sum = 0.0

        for r in o_rows:
            oid, oc = r["order_id"], r["order_code"]
            items_rows = _fetchall(con, "SELECT sku,title,qty,unit_price,total_price FROM bridge_lines WHERE order_id=%s ORDER BY line_index ASC", [oid])
            items: List[OrderItemOut] = []
            revenue = 0.0
            for ir in items_rows:
                qty = int(ir["qty"] or 1)
                unit = float(ir["unit_price"] or 0.0)
                tot = float(ir["total_price"] or (unit * qty))
                revenue += tot
                total_lines += 1
                items.append(OrderItemOut(sku=ir["sku"], title=ir["title"], qty=qty, unit_price=unit, total_price=tot))
            revenue_sum += revenue
            out.append(OrderOut(
                order_id=oid, order_code=oc, state=r["state"], date=_ms_to_iso(r["date_utc_ms"]),
                items=items, totals={"revenue": round(revenue, 2)}
            ))

        stats = {"orders": len(out), "lines": total_lines, "revenue": round(revenue_sum, 2)}
        return OrdersResponse(orders=out, source_used="bridge_v2", stats=stats)
    finally:
        con.close()

@router.get(f"{PFX[0]}/by-orders")
@router.get(f"{PFX[1]}/by-orders")
def by_orders(
    date_from: Optional[str] = Query(None, description="YYYY-MM-DD"),
    date_to: Optional[str] = Query(None, description="YYYY-MM-DD"),
    state: Optional[str] = Query(None, description="CSV статусов"),
    order: str = Query("asc"),
    codes: Optional[str] = Query(None, description="CSV order_code; если задано — даты игнорируются"),
    ids: Optional[str] = Query(None, description="CSV order_id; если задано — даты игнорируются"),
    _: bool = Depends(require_api_key),
) -> OrdersResponse:
    states = _parse_csv(state)
    order_dir = "ASC" if order.lower() == "asc" else "DESC"

    codes_list = _parse_csv(codes)
    ids_list = _parse_csv(ids)

    # приоритет — точный список из «Номера заказов (для сверки)»
    if codes_list or ids_list:
        parts = []
        params: List[Any] = []
        if codes_list:
            parts.append(f"order_code IN ({','.join(['%s']*len(codes_list))})")
            params += codes_list
        if ids_list:
            parts.append(f"order_id IN ({','.join(['%s']*len(ids_list))})")
            params += ids_list
        if states:
            parts.append(f"state IN ({','.join(['%s']*len(states))})")
            params += states
        where_sql = " AND ".join(parts) if parts else "1=0"
        return _collect_orders(where_sql, params, order_dir)

    if not (date_from and date_to):
        raise HTTPException(400, "Either provide (codes/ids) or (date_from & date_to)")
    try:
        import datetime as _dt
        from datetime import timezone
        start_ms = int(_dt.datetime.fromisoformat(date_from).replace(tzinfo=timezone.utc).timestamp() * 1000)
        end_ms = int((_dt.datetime.fromisoformat(date_to).replace(tzinfo=timezone.utc).timestamp() + 86400) * 1000) - 1
    except Exception:
        raise HTTPException(400, "date_from/date_to must be YYYY-MM-DD")

    parts = ["date_utc_ms BETWEEN %s AND %s"]
    params = [start_ms, end_ms]
    if states:
        parts.append(f"state IN ({','.join(['%s']*len(states))})")
        params += states
    return _collect_orders(" AND ".join(parts), params, order_dir)

@router.post(f"{PFX[0]}/ms/sync-costs")
@router.post(f"{PFX[1]}/ms/sync-costs")
def ms_sync_costs(
    date_from: Optional[str] = Query(None, description="YYYY-MM-DD"),
    date_to: Optional[str] = Query(None),
    _: bool = Depends(require_api_key),
):
    # Ничего не тянем из МС — просто считаем, для скольких SKU в заданном периоде уже есть партии
    con = DBH.connect()
    try:
        if date_from and date_to:
            import datetime as _dt
            from datetime import timezone
            try:
                start_ms = int(_dt.datetime.fromisoformat(date_from).replace(tzinfo=timezone.utc).timestamp() * 1000)
                end_ms = int((_dt.datetime.fromisoformat(date_to).replace(tzinfo=timezone.utc).timestamp() + 86400) * 1000) - 1
            except Exception:
                raise HTTPException(400, "date_from/date_to must be YYYY-MM-DD")
            skus = _fetchall(con, "SELECT DISTINCT sku FROM bridge_lines WHERE sku IS NOT NULL AND date_utc_ms BETWEEN %s AND %s", [start_ms, end_ms])
        else:
            skus = _fetchall(con, "SELECT DISTINCT sku FROM bridge_lines WHERE sku IS NOT NULL", [])
        keys = [r["sku"] for r in skus if r.get("sku")]
        if not keys:
            return {"ok": True, "synced": 0, "examples": {}}
        q = ",".join(["%s"] * len(keys))
        rows = _fetchall(con, f"SELECT sku FROM batches WHERE sku IN ({q})", keys)
        got = sorted({r["sku"] for r in rows})
        return {"ok": True, "synced": len(got), "examples": {k: True for k in got[:5]}}
    finally:
        con.close()

@router.get(f"{PFX[0]}/by-orders-enriched")
@router.get(f"{PFX[1]}/by-orders-enriched")
def by_orders_enriched(
    date_from: Optional[str] = Query(None, description="YYYY-MM-DD"),
    date_to: Optional[str] = Query(None, description="YYYY-MM-DD"),
    state: Optional[str] = Query(None, description="CSV статусов"),
    order: str = Query("asc"),
    codes: Optional[str] = Query(None, description="CSV order_code"),
    ids: Optional[str] = Query(None, description="CSV order_id"),
    _: bool = Depends(require_api_key),
) -> OrdersResponse:
    base = by_orders(date_from=date_from, date_to=date_to, state=state, order=order, codes=codes, ids=ids, _=True)
    con = DBH.connect()
    try:
        revenue_sum = 0.0
        cost_sum = 0.0
        commission_sum = 0.0

        for o in base.orders:
            total_cost = 0.0
            total_commission = 0.0
            for it in o.items:
                sku = (it.sku or "").strip()
                unit_cost, commission_pct = _cost_commission_for_sku(con, sku) if sku else (0.0, 0.0)

                qty = int(it.qty or 1)
                sum_line = float(it.total_price or 0.0)

                cost = round(unit_cost * qty, 2)
                commission_amt = round(sum_line * (commission_pct / 100.0), 2)
                profit = round(sum_line - cost - commission_amt, 2)

                it.cost = cost
                it.commission = commission_amt
                it.profit = profit

                total_cost += cost
                total_commission += commission_amt

            rev = float(o.totals.get("revenue", 0.0))
            o.totals["cost"] = round(total_cost, 2)
            o.totals["commission"] = round(total_commission, 2)
            o.totals["profit"] = round(rev - total_cost - total_commission, 2)

            revenue_sum += rev
            cost_sum += total_cost
            commission_sum += total_commission

        base.stats = {
            "orders": base.stats.get("orders", len(base.orders)),
            "lines": base.stats.get("lines", sum(len(o.items) for o in base.orders)),
            "revenue": round(revenue_sum, 2),
            "cost": round(cost_sum, 2),
            "commission": round(commission_sum, 2),
            "profit": round(revenue_sum - cost_sum - commission_sum, 2),
        }
        return base
    finally:
        con.close()
