from __future__ import annotations

import os
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel, Field

router = APIRouter(tags=["bridge_v2"])
PFX = ("/profit/bridge", "/bridge")

# ------------------------------ DB path как в products.py ------------------------------
def _resolve_db_path() -> str:
    """Тот же резолвер пути, что и в products.py"""
    p = (os.getenv("DB_PATH") or os.getenv("PRODUCTS_DB_PATH") or "/data/kaspi-orders.sqlite3").strip()
    if os.path.isabs(p) and os.path.exists(os.path.dirname(p)):
        return p
    # fallback рядом с приложением
    base = os.path.abspath(os.path.join(os.getcwd(), "data"))
    os.makedirs(base, exist_ok=True)
    return os.path.join(base, "kaspi-orders.sqlite3")

DB_PATH = _resolve_db_path()
REQ_API_KEY = (os.getenv("BRIDGE_API_KEY") or "").strip() or None

def _connect() -> sqlite3.Connection:
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c

# ------------------------------ Security ------------------------------
def require_api_key(request: Request):
    if not REQ_API_KEY:
        return True
    provided = request.headers.get("X-API-Key") or request.query_params.get("api_key")
    if provided != REQ_API_KEY:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")
    return True

# ------------------------------ Модели ------------------------------
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

# ------------------------------ Утилиты ------------------------------
def _to_ms(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        n = int(str(value).strip())
        return n if n >= 10_000_000_000 else n * 1000
    except Exception:
        pass
    s = str(value).strip()
    try:
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
    return datetime.fromtimestamp(ms/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")

def _parse_csv(raw: Optional[str]) -> List[str]:
    if not raw:
        return []
    return [p.strip() for p in str(raw).split(",") if p.strip()]

# ------------------------------ Инициализация таблиц моста ------------------------------
def _init_bridge():
    with _connect() as con:
        con.execute("""
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
        """)
        con.execute("CREATE INDEX IF NOT EXISTS ix_lines_date  ON bridge_lines(date_utc_ms)")
        con.execute("CREATE INDEX IF NOT EXISTS ix_lines_state ON bridge_lines(state)")
        con.execute("CREATE INDEX IF NOT EXISTS ix_lines_sku   ON bridge_lines(sku)")
        con.execute("CREATE INDEX IF NOT EXISTS ix_lines_code  ON bridge_lines(order_code)")
        con.commit()
_init_bridge()

# ------------------------------ Чтение себестоимости/комиссии из products.py БД ------------------------------
@dataclass
class BatchInfo:
    unit_cost: Optional[float]
    commission_pct: Optional[float]

def _latest_batch_for_sku(con: sqlite3.Connection, sku: str) -> Optional[BatchInfo]:
    row = con.execute(
        """
        SELECT unit_cost, commission_pct
        FROM batches
        WHERE sku = ?
        ORDER BY date DESC, id DESC
        LIMIT 1
        """,
        (sku,),
    ).fetchone()
    if not row:
        return None
    return BatchInfo(
        unit_cost=(float(row["unit_cost"]) if row["unit_cost"] is not None else None),
        commission_pct=(float(row["commission_pct"]) if row["commission_pct"] is not None else None),
    )

def _category_for_sku(con: sqlite3.Connection, sku: str) -> Optional[str]:
    r = con.execute("SELECT category FROM products WHERE sku = ?", (sku,)).fetchone()
    return (r["category"] if r and r["category"] else None)

def _category_commission_pct(con: sqlite3.Connection, cat: Optional[str]) -> Optional[float]:
    if not cat:
        return None
    r = con.execute(
        "SELECT base_percent, extra_percent, tax_percent FROM categories WHERE name = ?",
        (cat,),
    ).fetchone()
    if not r:
        return None
    base = float(r["base_percent"] or 0.0)
    extra = float(r["extra_percent"] or 0.0)
    tax = float(r["tax_percent"] or 0.0)
    return base + extra + tax

def _cost_commission_for_sku(con: sqlite3.Connection, sku: str) -> Tuple[float, float]:
    """
    Возвращает (unit_cost, commission_pct) по SKU.
    1) Берём последнюю партию из batches.
    2) Если комиссии нет — берём по категории из categories.
    """
    unit_cost = 0.0
    commission_pct = 0.0

    b = _latest_batch_for_sku(con, sku)
    if b and b.unit_cost is not None:
        unit_cost = float(b.unit_cost)

    if b and b.commission_pct is not None:
        commission_pct = float(b.commission_pct)
    else:
        cat = _category_for_sku(con, sku)
        c = _category_commission_pct(con, cat)
        if c is not None:
            commission_pct = float(c)

    return unit_cost, commission_pct

# ------------------------------ Endpoints ------------------------------
@router.get(f"{PFX[0]}/ping")
@router.get(f"{PFX[1]}/ping")
def ping():
    with _connect() as con:
        bl = int(con.execute("SELECT COUNT(*) FROM bridge_lines").fetchone()[0])
        # наличие продуктов/партий/категорий
        try:
            prod = int(con.execute("SELECT COUNT(*) FROM products").fetchone()[0])
        except Exception:
            prod = -1
        try:
            bat = int(con.execute("SELECT COUNT(*) FROM batches").fetchone()[0])
        except Exception:
            bat = -1
        try:
            cat = int(con.execute("SELECT COUNT(*) FROM categories").fetchone()[0])
        except Exception:
            cat = -1
    return {"ok": True, "db": DB_PATH, "bridge_lines": bl, "products": prod, "batches": bat, "categories": cat, "ts": int(time.time()*1000)}

@router.post(f"{PFX[0]}/sync-by-ids")
@router.post(f"{PFX[1]}/sync-by-ids")
def sync_by_ids(items: List[BridgeLineIn], _: bool = Depends(require_api_key)):
    if not items:
        return {"updated": 0, "skipped": 0}

    updated = 0
    skipped = 0
    counters: Dict[str, int] = {}

    with _connect() as con:
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
                skipped += 1
                continue

            order_code = (it.code or "").strip() or None
            state = (it.state or "").strip() or None
            date_ms = _to_ms(it.date)
            sku = (it.sku or "").strip() or None
            title = (it.title or "").strip() or None

            try:
                qty = int(it.qty or 1)
            except Exception:
                qty = 1

            # total
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

            # unit
            if it.unit_price is not None:
                try:
                    unit = float(it.unit_price)
                except Exception:
                    unit = float(total) / max(1, qty)
            else:
                unit = float(total) / max(1, qty)

            # line index
            if it.line_index is not None:
                li = int(it.line_index)
            else:
                li = counters.get(oid, 0)
                counters[oid] = li + 1

            con.execute(sql, (oid, order_code, state, date_ms, sku, title, qty, unit, total, li))
            updated += 1
        con.commit()

    return {"updated": updated, "skipped": skipped}

def _collect_orders(where_sql: str, params: List[Any], order_dir: str) -> OrdersResponse:
    with _connect() as con:
        o_rows = list(con.execute(
            f"""
            SELECT order_id, order_code, MIN(date_utc_ms) AS date_utc_ms, MAX(state) as state
            FROM bridge_lines
            WHERE {where_sql}
            GROUP BY order_id, order_code
            ORDER BY date_utc_ms {order_dir}
            """,
            params,
        ))
        sql_items = "SELECT sku,title,qty,unit_price,total_price FROM bridge_lines WHERE order_id=? ORDER BY line_index ASC"

        out: List[OrderOut] = []
        total_lines = 0
        revenue_sum = 0.0

        for r in o_rows:
            oid, oc = r["order_id"], r["order_code"]
            items_rows = list(_connect().execute(sql_items, (oid,)))
            items: List[OrderItemOut] = []
            revenue = 0.0
            for ir in items_rows:
                qty = int(ir["qty"] or 1)
                unit = float(ir["unit_price"] or 0.0)
                tot = float(ir["total_price"] or (unit * qty))
                revenue += tot
                total_lines += 1
                items.append(OrderItemOut(
                    sku=ir["sku"], title=ir["title"], qty=qty, unit_price=unit, total_price=tot
                ))
            revenue_sum += revenue
            out.append(OrderOut(
                order_id=oid, order_code=oc, state=r["state"], date=_ms_to_iso(r["date_utc_ms"]),
                items=items, totals={"revenue": round(revenue, 2)}
            ))

    stats = {"orders": len(out), "lines": total_lines, "revenue": round(revenue_sum, 2)}
    return OrdersResponse(orders=out, source_used="bridge_v2", stats=stats)

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

    if codes_list or ids_list:
        parts = []
        params: List[Any] = []
        if codes_list:
            parts.append(f"order_code IN ({','.join('?' for _ in codes_list)})")
            params += codes_list
        if ids_list:
            parts.append(f"order_id IN ({','.join('?' for _ in ids_list)})")
            params += ids_list
        if states:
            parts.append(f"state IN ({','.join('?' for _ in states)})")
            params += states
        where_sql = " AND ".join(parts) if parts else "1=0"
        return _collect_orders(where_sql, params, order_dir)

    if not (date_from and date_to):
        raise HTTPException(400, "Either provide (codes/ids) or (date_from & date_to)")
    try:
        start_ms = int(datetime.fromisoformat(date_from).replace(tzinfo=timezone.utc).timestamp() * 1000)
        end_ms = int((datetime.fromisoformat(date_to).replace(tzinfo=timezone.utc).timestamp() + 86400) * 1000) - 1
    except Exception:
        raise HTTPException(400, "date_from/date_to must be YYYY-MM-DD")

    parts = ["date_utc_ms BETWEEN ? AND ?"]
    params = [start_ms, end_ms]
    if states:
        parts.append(f"state IN ({','.join('?' for _ in states)})")
        params += states
    return _collect_orders(" AND ".join(parts), params, order_dir)

# -------- by-orders-enriched: себестоимость/комиссия/прибыль --------
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

    revenue_sum = 0.0
    cost_sum = 0.0
    comm_sum = 0.0

    with _connect() as con:
        for o in base.orders:
            total_cost = 0.0
            total_comm = 0.0
            for it in o.items:
                sku = (it.sku or "").strip()
                unit_cost, commission_pct = _cost_commission_for_sku(con, sku) if sku else (0.0, 0.0)

                c = round(unit_cost * (it.qty or 1), 2)
                comm = round((commission_pct or 0.0) * (it.total_price or 0.0) / 100.0, 2)
                p = round((it.total_price or 0.0) - c - comm, 2)

                it.cost = c
                it.commission = comm
                it.profit = p

                total_cost += c
                total_comm += comm

            rev = float(o.totals.get("revenue", 0.0))
            o.totals["cost"] = round(total_cost, 2)
            o.totals["commission"] = round(total_comm, 2)
            o.totals["profit"] = round(rev - total_cost - total_comm, 2)

            revenue_sum += rev
            cost_sum += total_cost
            comm_sum += total_comm

    base.stats = {
        "orders": base.stats.get("orders", len(base.orders)),
        "lines": base.stats.get("lines", sum(len(o.items) for o in base.orders)),
        "revenue": round(revenue_sum, 2),
        "cost": round(cost_sum, 2),
        "commission": round(comm_sum, 2),
        "profit": round(revenue_sum - cost_sum - comm_sum, 2),
    }
    return base

# -------- Кнопка «MS себестоимость»: просто считаем, сколько SKU видим в партиях/категориях --------
@router.post(f"{PFX[0]}/ms/sync-costs")
@router.post(f"{PFX[1]}/ms/sync-costs")
def ms_sync_costs(
    date_from: Optional[str] = Query(None, description="YYYY-MM-DD"),
    date_to: Optional[str] = Query(None, description="YYYY-MM-DD"),
    _: bool = Depends(require_api_key),
):
    with _connect() as con:
        if date_from and date_to:
            try:
                start_ms = int(datetime.fromisoformat(date_from).replace(tzinfo=timezone.utc).timestamp() * 1000)
                end_ms = int((datetime.fromisoformat(date_to).replace(tzinfo=timezone.utc).timestamp() + 86400) * 1000) - 1
            except Exception:
                raise HTTPException(400, "date_from/date_to must be YYYY-MM-DD")
            rows = list(con.execute(
                "SELECT DISTINCT sku FROM bridge_lines WHERE sku IS NOT NULL AND date_utc_ms BETWEEN ? AND ?",
                (start_ms, end_ms),
            ))
        else:
            rows = list(con.execute("SELECT DISTINCT sku FROM bridge_lines WHERE sku IS NOT NULL"))
    skus = [r["sku"] for r in rows if r["sku"]]
    seen = 0
    examples: Dict[str, Dict[str, float]] = {}
    with _connect() as con:
        for s in skus:
            uc, cp = _cost_commission_for_sku(con, s)
            if uc or cp:
                seen += 1
                if len(examples) < 5:
                    examples[s] = {"unit_cost": uc, "commission_pct": cp}
    return {"ok": True, "seen": seen, "synced": seen, "examples": examples}
