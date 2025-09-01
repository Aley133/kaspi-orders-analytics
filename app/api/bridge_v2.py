from __future__ import annotations

import os, sqlite3, time, math
from typing import Any, Dict, Iterable, List, Optional, Tuple
from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel, Field

router = APIRouter(tags=["bridge_v2"])

# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------
# DB with product catalog (products/batches/categories)
DB_URL = (
    os.getenv("PROFIT_DB_URL")
    or os.getenv("DATABASE_URL")
    or os.getenv("DB_URL")
    or "sqlite:///data/kaspi-orders.sqlite3"
).strip()

# Local storage for synced order lines. Keep sqlite for simplicity.
LINES_DB_PATH = os.getenv("BRIDGE_DB_PATH", "data/bridge_v2.sqlite3")
os.makedirs(os.path.dirname(LINES_DB_PATH), exist_ok=True)

REQ_API_KEY = (os.getenv("BRIDGE_API_KEY") or "").strip() or None
ONLY_STATE = (os.getenv("BRIDGE_ONLY_STATE") or "").strip() or None


# ---------------------------------------------------------------------
# Security
# ---------------------------------------------------------------------
def require_api_key(request: Request):
    if not REQ_API_KEY:
        return True
    provided = request.headers.get("X-API-Key") or request.query_params.get("api_key")
    if provided != REQ_API_KEY:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")
    return True


# ---------------------------------------------------------------------
# Connections
# ---------------------------------------------------------------------
def _connect_lines() -> sqlite3.Connection:
    c = sqlite3.connect(LINES_DB_PATH)
    c.row_factory = sqlite3.Row
    return c

def _init_lines_db():
    with _connect_lines() as con:
        con.execute(
            """
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
            )
            """
        )
        con.execute("CREATE INDEX IF NOT EXISTS ix_lines_date  ON bridge_lines(date_utc_ms)")
        con.execute("CREATE INDEX IF NOT EXISTS ix_lines_state ON bridge_lines(state)")
        con.execute("CREATE INDEX IF NOT EXISTS ix_lines_sku   ON bridge_lines(sku)")
        con.execute("CREATE INDEX IF NOT EXISTS ix_lines_code  ON bridge_lines(order_code)")
        con.commit()
_init_lines_db()


# Source DB (products/batches/categories)
def _is_sqlite_url(url: str) -> bool:
    return url.startswith("sqlite:///") or url.endswith(".sqlite3") or url.endswith(".db")

_pg_available = None
def _connect_src():
    global _pg_available
    if _is_sqlite_url(DB_URL):
        path = DB_URL.split("sqlite:///")[-1]
        c = sqlite3.connect(path)
        c.row_factory = sqlite3.Row
        return c
    # try Postgres via psycopg
    if _pg_available is None:
        try:
            import psycopg  # type: ignore
            _pg_available = True
        except Exception:
            _pg_available = False
    if not _pg_available:
        raise HTTPException(500, "PostgreSQL driver (psycopg) is not available")
    import psycopg  # type: ignore
    return psycopg.connect(DB_URL, autocommit=True)


def _src_table_exists(con, name: str) -> bool:
    try:
        if isinstance(con, sqlite3.Connection):
            cur = con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (name,))
            return cur.fetchone() is not None
        else:
            with con.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog','information_schema') AND table_name=%s LIMIT 1",
                    (name,),
                )
                return cur.fetchone() is not None
    except Exception:
        return False


# ---------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------
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


# ---------------------------------------------------------------------
# Utils
# ---------------------------------------------------------------------
def _to_ms(v: Any) -> Optional[int]:
    if v is None:
        return None
    s = str(v).strip()
    try:
        n = int(s)
        return n if n > 10_000_000_000 else n * 1000
    except Exception:
        pass
    # ISO
    try:
        from datetime import datetime, timezone
        if s.endswith("Z"):
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except Exception:
        return None

def _ms_to_iso(ms: Optional[int]) -> Optional[str]:
    if ms is None:
        return None
    from datetime import datetime, timezone
    return datetime.fromtimestamp(ms/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")


def _parse_csv(raw: Optional[str]) -> List[str]:
    if not raw:
        return []
    return [p.strip() for p in str(raw).split(",") if p.strip()]

def _first_part_candidates(sku: str) -> List[str]:
    cand = [sku]
    if "_" in sku:
        left,right = sku.split("_",1)
        if left: cand.append(left)
        if right: cand.append(right)
    return list(dict.fromkeys(cand))


# ---------------------------------------------------------------------
# Catalog lookups from source DB
# ---------------------------------------------------------------------
def _fetch_latest_batch(con, sku: str) -> Tuple[Optional[float], Optional[float]]:
    # returns (purchase_price, commission_pct) or (None,None)
    if isinstance(con, sqlite3.Connection):
        cur = con.cursor()
        for key in _first_part_candidates(sku):
            cur.execute("SELECT purchase_price, commission_pct FROM batches WHERE sku=? ORDER BY COALESCE(date,0) DESC LIMIT 1", (key,))
            row = cur.fetchone()
            if row:
                return (float(row[0]) if row[0] is not None else None, float(row[1]) if row[1] is not None else None)
        return (None, None)
    else:
        with con.cursor() as cur:
            for key in _first_part_candidates(sku):
                cur.execute(
                    "SELECT purchase_price, commission_pct FROM batches WHERE sku=%s ORDER BY COALESCE(date,0) DESC LIMIT 1",
                    (key,),
                )
                row = cur.fetchone()
                if row:
                    return (float(row[0]) if row[0] is not None else None, float(row[1]) if row[1] is not None else None)
        return (None,None)

def _fetch_category_commission(con, sku: str) -> Optional[float]:
    # categories.name is referenced by products.category
    if isinstance(con, sqlite3.Connection):
        cur = con.cursor()
        row = None
        for key in _first_part_candidates(sku):
            cur.execute("""
                SELECT c.base_percent, c.extra_percent, c.tax_percent
                FROM products p
                LEFT JOIN categories c ON c.name = p.category
                WHERE p.sku = ?
                LIMIT 1
            """, (key,))
            row = cur.fetchone()
            if row: break
        if not row: return None
        vals = [row[0] or 0.0, row[1] or 0.0, row[2] or 0.0]
        return float(sum(vals))
    else:
        with con.cursor() as cur:
            row = None
            for key in _first_part_candidates(sku):
                cur.execute("""
                    SELECT c.base_percent, c.extra_percent, c.tax_percent
                    FROM products p
                    LEFT JOIN categories c ON c.name = p.category
                    WHERE p.sku = %s
                    LIMIT 1
                """, (key,))
                row = cur.fetchone()
                if row: break
        if not row: return None
        vals = [row[0] or 0.0, row[1] or 0.0, row[2] or 0.0]
        return float(sum(vals))

def _cost_commission_for_sku(sku: str) -> Tuple[float, float]:
    # open src DB every time to avoid threading issues
    con = _connect_src()
    try:
        if not _src_table_exists(con, "batches"):
            # graceful fallback — no catalog installed
            return (0.0, 0.0)
        price, comm = _fetch_latest_batch(con, sku)
        if price is None:
            price = 0.0
        if comm is None:
            comm = _fetch_category_commission(con, sku) or 0.0
        return (float(price or 0.0), float(comm or 0.0))
    finally:
        try:
            con.close()
        except Exception:
            pass


# ---------------------------------------------------------------------
# API
# ---------------------------------------------------------------------
@router.get("/profit/bridge/ping")
def ping():
    with _connect_lines() as con:
        c = int(con.execute("SELECT COUNT(*) FROM bridge_lines").fetchone()[0])
    src_info = {"url": DB_URL, "tables": {}}
    try:
        con2 = _connect_src()
        for t in ("products","batches","categories"):
            src_info["tables"][t] = _src_table_exists(con2, t)
        try: con2.close()
        except Exception: pass
    except Exception as e:
        src_info["error"] = str(e)
    return {"ok": True, "lines": c, "src": src_info}


@router.post("/profit/bridge/sync-by-ids")
def sync_by_ids(items: List[BridgeLineIn], _: bool = Depends(require_api_key)):
    if not items:
        return {"inserted": 0, "updated": 0, "skipped": 0}
    inserted = updated = skipped = 0
    counters: Dict[str, int] = {}

    with _connect_lines() as con:
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("PRAGMA synchronous=NORMAL;")
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
        for it in items:
            oid = (it.id or "").strip()
            if not oid:
                skipped += 1; continue
            if ONLY_STATE and it.state and it.state != ONLY_STATE:
                skipped += 1; continue
            code = (it.code or "").strip() or None
            state = (it.state or "").strip() or None
            date_ms = _to_ms(it.date)
            sku = (it.sku or "").strip() or None
            title = (it.title or "").strip() or None
            try: qty = int(it.qty or 1)
            except Exception: qty = 1
            # total
            if it.total_price is not None:
                total = float(it.total_price)
            elif it.amount is not None:
                total = float(it.amount)
            elif it.unit_price is not None:
                try: total = float(it.unit_price) * qty
                except Exception: total = 0.0
            else:
                total = 0.0
            unit = float(it.unit_price) if it.unit_price is not None else (float(total)/max(1,qty))
            if it.line_index is not None:
                idx = int(it.line_index)
            else:
                idx = counters.get(oid, 0)
                counters[oid] = idx + 1
            con.execute(sql, (oid, code, state, date_ms, sku, title, qty, unit, total, idx))
            updated += 1
        con.commit()

    processed = len(items) - skipped
    inserted = max(0, processed - updated)
    return {"inserted": inserted, "updated": updated, "skipped": skipped}


def _collect_orders(where_sql: str, params: List[Any], order_dir: str) -> OrdersResponse:
    with _connect_lines() as con:
        sql_orders = f"""
            SELECT order_id, order_code, MIN(date_utc_ms) AS date_utc_ms, MAX(state) AS state
            FROM bridge_lines
            WHERE {where_sql}
            GROUP BY order_id, order_code
            ORDER BY date_utc_ms {order_dir}
        """
        rows = list(con.execute(sql_orders, params))
        sql_items = "SELECT sku,title,qty,unit_price,total_price FROM bridge_lines WHERE order_id=? ORDER BY line_index ASC"

        out: List[OrderOut] = []
        total_lines = 0
        revenue_sum = 0.0

        for r in rows:
            oid = r["order_id"]; oc = r["order_code"]
            items_rows = list(con.execute(sql_items, (oid,)))
            items: List[OrderItemOut] = []
            revenue = 0.0
            for ir in items_rows:
                qty = int(ir["qty"] or 1)
                unit = float(ir["unit_price"] or 0.0)
                tot  = float(ir["total_price"] or (unit * qty))
                revenue += tot
                total_lines += 1
                items.append(OrderItemOut(sku=ir["sku"], title=ir["title"], qty=qty, unit_price=unit, total_price=tot))
            revenue_sum += revenue
            out.append(
                OrderOut(
                    order_id=oid,
                    order_code=oc,
                    state=r["state"],
                    date=_ms_to_iso(r["date_utc_ms"]),
                    items=items,
                    totals={"revenue": round(revenue, 2)},
                )
            )

    stats = {"orders": len(out), "lines": total_lines, "revenue": round(revenue_sum, 2)}
    return OrdersResponse(orders=out, source_used="bridge_v2", stats=stats)


@router.get("/profit/bridge/by-orders")
def by_orders(
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    state: Optional[str] = Query(None, description="CSV статусов"),
    order: str = Query("asc"),
    codes: Optional[str] = Query(None, description="CSV order_code; если задано — даты игнорируются"),
    ids: Optional[str] = Query(None, description="CSV order_id; если задано — даты игнорируются"),
    _: bool = Depends(require_api_key),
) -> OrdersResponse:
    states = _parse_csv(state)
    order_dir = "ASC" if order.lower()=="asc" else "DESC"

    codes_list = _parse_csv(codes)
    ids_list = _parse_csv(ids)
    if codes_list or ids_list:
        parts = []; params: List[Any] = []
        if codes_list:
            parts.append(f"order_code IN ({','.join('?' for _ in codes_list)})"); params += codes_list
        if ids_list:
            parts.append(f"order_id IN ({','.join('?' for _ in ids_list)})"); params += ids_list
        if states:
            parts.append(f"state IN ({','.join('?' for _ in states)})"); params += states
        return _collect_orders(" AND ".join(parts) if parts else "1=0", params, order_dir)

    if not (date_from and date_to):
        raise HTTPException(400, "Either provide (codes/ids) or (date_from & date_to)")
    from datetime import datetime, timezone
    try:
        start_ms = int(datetime.fromisoformat(date_from).replace(tzinfo=timezone.utc).timestamp()*1000)
        end_ms   = int((datetime.fromisoformat(date_to).replace(tzinfo=timezone.utc).timestamp()+86400)*1000)-1
    except Exception:
        raise HTTPException(400, "date_from/date_to must be YYYY-MM-DD")

    parts = ["date_utc_ms BETWEEN ? AND ?"]; params: List[Any] = [start_ms, end_ms]
    if states:
        parts.append(f"state IN ({','.join('?' for _ in states)})"); params += states
    return _collect_orders(" AND ".join(parts), params, order_dir)


@router.get("/profit/bridge/by-orders-enriched")
def by_orders_enriched(
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    state: Optional[str] = Query(None),
    order: str = Query("asc"),
    codes: Optional[str] = Query(None),
    ids: Optional[str] = Query(None),
    _: bool = Depends(require_api_key),
) -> OrdersResponse:
    base = by_orders(date_from=date_from, date_to=date_to, state=state, order=order, codes=codes, ids=ids, _=True)

    revenue_sum = 0.0
    cost_sum = 0.0
    comm_sum = 0.0

    for o in base.orders:
        order_cost = 0.0
        order_comm = 0.0
        for it in o.items:
            sku = (it.sku or "").strip()
            unit_cost, commission_pct = _cost_commission_for_sku(sku) if sku else (0.0, 0.0)
            c = round((unit_cost or 0.0) * (it.qty or 1), 2)
            commission_val = round((it.total_price or 0.0) * (commission_pct or 0.0) / 100.0, 2)
            p = round((it.total_price or 0.0) - c - commission_val, 2)
            it.cost = c
            it.commission = commission_val
            it.profit = p
            order_cost += c
            order_comm += commission_val
        rev = float(o.totals.get("revenue", 0.0))
        o.totals["cost"] = round(order_cost, 2)
        o.totals["commission"] = round(order_comm, 2)
        o.totals["profit"] = round(rev - order_cost - order_comm, 2)
        revenue_sum += rev
        cost_sum += order_cost
        comm_sum += order_comm

    base.stats = {
        "orders": base.stats.get("orders", len(base.orders)),
        "lines": base.stats.get("lines", sum(len(x.items) for x in base.orders)),
        "revenue": round(revenue_sum, 2),
        "cost": round(cost_sum, 2),
        "commission": round(comm_sum, 2),
        "profit": round(revenue_sum - cost_sum - comm_sum, 2),
    }
    return base


@router.post("/profit/bridge/ms/sync-costs")
def ms_sync_costs(
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    _: bool = Depends(require_api_key),
):
    # For UI badge: count distinct SKUs in bridge_lines for range and how many have a batch
    from datetime import datetime, timezone
    with _connect_lines() as con:
        if date_from and date_to:
            try:
                start_ms = int(datetime.fromisoformat(date_from).replace(tzinfo=timezone.utc).timestamp()*1000)
                end_ms   = int((datetime.fromisoformat(date_to).replace(tzinfo=timezone.utc).timestamp()+86400)*1000)-1
            except Exception:
                raise HTTPException(400, "date_from/date_to must be YYYY-MM-DD")
            rows = list(con.execute(
                "SELECT DISTINCT sku FROM bridge_lines WHERE sku IS NOT NULL AND date_utc_ms BETWEEN ? AND ?",
                (start_ms, end_ms),
            ))
        else:
            rows = list(con.execute("SELECT DISTINCT sku FROM bridge_lines WHERE sku IS NOT NULL"))
    skus = [r[0] for r in rows if r[0]]

    seen = 0
    examples: Dict[str, float] = {}
    # try to find price for each (fast — stop after a few examples)
    for s in skus:
        price, _c = _cost_commission_for_sku(s)
        if price and price > 0:
            seen += 1
            if len(examples) < 5:
                examples[s] = price

    return {"ok": True, "seen": seen, "synced": seen, "examples": examples}
