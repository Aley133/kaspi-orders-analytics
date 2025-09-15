# app/api/bridge_v2.py
from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel, Field

from contextlib import contextmanager
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, Connection
from sqlalchemy.orm import sessionmaker

router = APIRouter(tags=["bridge_v2"])
PFX = ("/profit/bridge", "/bridge")

# ──────────────────────────────────────────────────────────────────────────────
# ENV / конфиг
# ──────────────────────────────────────────────────────────────────────────────
REQ_API_KEY = (os.getenv("BRIDGE_API_KEY") or "").strip() or None
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
DB_PATH = os.getenv("DB_PATH", "/data/kaspi-orders.sqlite3")

def _sa_url(url: str) -> str:
    # SQLAlchemy + psycopg (pg8000/psycopg3)
    return url.replace("postgresql://", "postgresql+psycopg://", 1)

if DATABASE_URL:
    SA_URL = _sa_url(DATABASE_URL)
    _engine: Engine = create_engine(SA_URL, pool_pre_ping=True, future=True)
    DIALECT = _engine.dialect.name
else:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    _engine = create_engine(f"sqlite+pysqlite:///{DB_PATH}", future=True)
    DIALECT = "sqlite"

IS_PG = DIALECT.startswith("postgres")
SessionLocal = sessionmaker(bind=_engine, autoflush=False, autocommit=False)
NOW_MS = lambda: int(time.time() * 1000)

@contextmanager
def db() -> Iterable[Connection]:
    with _engine.begin() as con:
        yield con

# ──────────────────────────────────────────────────────────────────────────────
# Безопасность: либо Supabase-сессия, либо X-API-Key (если задан)
# ──────────────────────────────────────────────────────────────────────────────
def require_api_key(request: Request):
    """
    Пропускаем, если:
      1) у запроса есть Supabase bearer (request.state.supabase_token),
      2) BRIDGE_API_KEY не задан,
      3) либо X-API-Key/param api_key совпадает с BRIDGE_API_KEY.
    Иначе — 401.
    """
    # 1) Supabase сессия от мидлвары auth.attach_kaspi_token_middleware
    if getattr(request.state, "supabase_token", ""):
        return True
    # 2) API-ключ не настроен → считаем открытую конфигурацию
    if not REQ_API_KEY:
        return True
    # 3) Явный ключ
    provided = request.headers.get("X-API-Key") or request.query_params.get("api_key")
    if provided == REQ_API_KEY:
        return True
    raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")

# ──────────────────────────────────────────────────────────────────────────────
# Инициализация таблиц (кросс-диалектная)
# ──────────────────────────────────────────────────────────────────────────────
def _init_bridge_tables() -> None:
    with _engine.begin() as con:
        if IS_PG:
            con.execute(text("""
                CREATE TABLE IF NOT EXISTS public.batches (
                  id             bigserial PRIMARY KEY,
                  sku            text NOT NULL,
                  unit_cost      numeric NOT NULL DEFAULT 0,
                  commission_pct numeric NOT NULL DEFAULT 0,
                  date           date,
                  created_at     timestamptz NOT NULL DEFAULT now()
                )
            """))
            con.execute(text("CREATE INDEX IF NOT EXISTS idx_batches_sku ON public.batches(sku)"))

            con.execute(text("""
                CREATE TABLE IF NOT EXISTS public.bridge_lines(
                  order_id     text NOT NULL,
                  order_code   text,
                  state        text,
                  date_utc_ms  bigint,
                  sku          text,
                  title        text,
                  qty          integer DEFAULT 1,
                  unit_price   double precision DEFAULT 0,
                  total_price  double precision DEFAULT 0,
                  line_index   integer NOT NULL,
                  created_at   bigint,
                  updated_at   bigint,
                  PRIMARY KEY(order_id, line_index)
                )
            """))
            con.execute(text("CREATE INDEX IF NOT EXISTS ix_lines_date  ON public.bridge_lines(date_utc_ms)"))
            con.execute(text("CREATE INDEX IF NOT EXISTS ix_lines_state ON public.bridge_lines(state)"))
            con.execute(text("CREATE INDEX IF NOT EXISTS ix_lines_sku   ON public.bridge_lines(sku)"))
            con.execute(text("CREATE INDEX IF NOT EXISTS ix_lines_code  ON public.bridge_lines(order_code)"))
        else:
            # SQLite
            con.execute(text("""
                CREATE TABLE IF NOT EXISTS batches (
                  id             integer PRIMARY KEY AUTOINCREMENT,
                  sku            text NOT NULL,
                  unit_cost      real NOT NULL DEFAULT 0,
                  commission_pct real NOT NULL DEFAULT 0,
                  date           text,
                  created_at     integer NOT NULL DEFAULT 0
                )
            """))
            con.execute(text("CREATE INDEX IF NOT EXISTS idx_batches_sku ON batches(sku)"))

            con.execute(text("""
                CREATE TABLE IF NOT EXISTS bridge_lines(
                  order_id     text NOT NULL,
                  order_code   text,
                  state        text,
                  date_utc_ms  integer,
                  sku          text,
                  title        text,
                  qty          integer DEFAULT 1,
                  unit_price   real DEFAULT 0,
                  total_price  real DEFAULT 0,
                  line_index   integer NOT NULL,
                  created_at   integer,
                  updated_at   integer,
                  PRIMARY KEY(order_id, line_index)
                )
            """))
            con.execute(text("CREATE INDEX IF NOT EXISTS ix_lines_date  ON bridge_lines(date_utc_ms)"))
            con.execute(text("CREATE INDEX IF NOT EXISTS ix_lines_state ON bridge_lines(state)"))
            con.execute(text("CREATE INDEX IF NOT EXISTS ix_lines_sku   ON bridge_lines(sku)"))
            con.execute(text("CREATE INDEX IF NOT EXISTS ix_lines_code  ON bridge_lines(order_code)"))

_init_bridge_tables()

# ──────────────────────────────────────────────────────────────────────────────
# Модели
# ──────────────────────────────────────────────────────────────────────────────
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

# ──────────────────────────────────────────────────────────────────────────────
# Даты / утилиты
# ──────────────────────────────────────────────────────────────────────────────
def _to_ms(value: Any) -> Optional[int]:
    if value is None:
        return None
    # число (сек/мс)
    try:
        n = int(str(value).strip())
        return n if n >= 10_000_000_000 else n * 1000
    except Exception:
        pass
    # ISO
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

def _canon_sku(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    v = str(s).strip()
    return v or None

# ──────────────────────────────────────────────────────────────────────────────
# Привязка к batches / products / categories
# ──────────────────────────────────────────────────────────────────────────────
def _latest_batch(con: Connection, sku: str) -> Optional[Dict[str, Any]]:
    """
    Последняя партия по sku:
    - сперва по batches.date (DESC)
    - затем по created_at (DESC)
    """
    if IS_PG:
        row = con.execute(text("""
            SELECT unit_cost, commission_pct
            FROM batches
            WHERE sku = :sku
            ORDER BY COALESCE(date::text, '') DESC, COALESCE(CAST(created_at AS TEXT), '') DESC
            LIMIT 1
        """), {"sku": sku}).mappings().first()
    else:
        row = con.execute(text("""
            SELECT unit_cost, commission_pct
            FROM batches
            WHERE sku = :sku
            ORDER BY COALESCE(date, '') DESC, COALESCE(created_at, '') DESC
            LIMIT 1
        """), {"sku": sku}).mappings().first()
    return dict(row) if row else None

def _category_commission_pct(con: Connection, sku: str) -> Optional[float]:
    """
    base_percent + extra_percent + tax_percent по категории продукта.
    """
    row = con.execute(text("""
        SELECT c.base_percent, c.extra_percent, c.tax_percent
        FROM products p
        JOIN categories c ON c.name = p.category
        WHERE p.sku = :sku
        LIMIT 1
    """), {"sku": sku}).mappings().first()
    if not row:
        return None
    base = float(row.get("base_percent") or 0.0)
    extra = float(row.get("extra_percent") or 0.0)
    tax  = float(row.get("tax_percent") or 0.0)
    return base + extra + tax

def _cost_commission_for_sku(con: Connection, sku: str) -> Tuple[float, float]:
    """
    (unit_cost, commission_pct)
    Приоритет комиссии: batches.commission_pct → сумма процентов категории.
    """
    b = _latest_batch(con, sku)
    unit_cost = float((b or {}).get("unit_cost") or 0.0)
    commission_pct = None
    if b and b.get("commission_pct") is not None:
        try:
            commission_pct = float(b["commission_pct"])
        except Exception:
            commission_pct = None
    if commission_pct is None:
        commission_pct = _category_commission_pct(con, sku) or 0.0
    return unit_cost, float(commission_pct)

# ──────────────────────────────────────────────────────────────────────────────
# Endpoints
# ──────────────────────────────────────────────────────────────────────────────
@router.get(f"{PFX[0]}/ping")
@router.get(f"{PFX[1]}/ping")
def ping(_: bool = Depends(require_api_key)):
    with db() as con:
        c = con.execute(text("SELECT COUNT(*) AS n FROM bridge_lines")).scalar() or 0
        try:
            b = con.execute(text("SELECT COUNT(*) FROM batches")).scalar() or 0
        except Exception:
            b = 0
        try:
            cat = con.execute(text("SELECT COUNT(*) FROM categories")).scalar() or 0
        except Exception:
            cat = 0
    return {"ok": True, "dialect": DIALECT, "bridge_lines": int(c), "batches": int(b), "categories": int(cat), "ts": NOW_MS()}

@router.post(f"{PFX[0]}/sync-by-ids}")
@router.post(f"{PFX[1]}/sync-by-ids}")
def sync_by_ids(items: List[BridgeLineIn], _: bool = Depends(require_api_key)):
    if not items:
        return {"inserted": 0, "updated": 0, "skipped": 0}

    counters: Dict[str, int] = {}
    now_ms = NOW_MS()

    upsert_sql = text("""
        INSERT INTO bridge_lines
          (order_id, order_code, state, date_utc_ms, sku, title, qty, unit_price, total_price, line_index, created_at, updated_at)
        VALUES
          (:order_id, :order_code, :state, :date_utc_ms, :sku, :title, :qty, :unit_price, :total_price, :line_index, :created_at, :updated_at)
        ON CONFLICT (order_id, line_index) DO UPDATE SET
          order_code = EXCLUDED.order_code,
          state      = EXCLUDED.state,
          date_utc_ms= EXCLUDED.date_utc_ms,
          sku        = EXCLUDED.sku,
          title      = EXCLUDED.title,
          qty        = EXCLUDED.qty,
          unit_price = EXCLUDED.unit_price,
          total_price= EXCLUDED.total_price,
          updated_at = EXCLUDED.updated_at
    """) if IS_PG else text("""
        INSERT INTO bridge_lines
          (order_id, order_code, state, date_utc_ms, sku, title, qty, unit_price, total_price, line_index, created_at, updated_at)
        VALUES
          (:order_id, :order_code, :state, :date_utc_ms, :sku, :title, :qty, :unit_price, :total_price, :line_index, :created_at, :updated_at)
        ON CONFLICT(order_id, line_index) DO UPDATE SET
          order_code = excluded.order_code,
          state      = excluded.state,
          date_utc_ms= excluded.date_utc_ms,
          sku        = excluded.sku,
          title      = excluded.title,
          qty        = excluded.qty,
          unit_price = excluded.unit_price,
          total_price= excluded.total_price,
          updated_at = excluded.updated_at
    """)

    updated = 0
    skipped = 0
    with db() as con:
        for it in items:
            order_id = (it.id or "").strip()
            if not order_id:
                skipped += 1
                continue

            order_code = (it.code or "").strip() or None
            state = (it.state or "").strip() or None
            date_ms = _to_ms(it.date)
            sku = _canon_sku(it.sku)
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

            con.execute(upsert_sql, {
                "order_id": order_id, "order_code": order_code, "state": state, "date_utc_ms": date_ms,
                "sku": sku, "title": title, "qty": qty, "unit_price": unit, "total_price": total,
                "line_index": line_index, "created_at": now_ms, "updated_at": now_ms
            })
            updated += 1

    processed = len(items) - skipped
    inserted = max(0, processed - updated)
    return {"inserted": inserted, "updated": updated, "skipped": skipped}

def _collect_orders(where_sql: str, params: Dict[str, Any], order_dir: str) -> OrdersResponse:
    with db() as con:
        sql_orders = f"""
            SELECT order_id, order_code, MIN(date_utc_ms) AS date_utc_ms, MAX(state) as state
            FROM bridge_lines
            WHERE {where_sql}
            GROUP BY order_id, order_code
            ORDER BY date_utc_ms {order_dir}
        """
        o_rows = list(con.execute(text(sql_orders), params).mappings())
        sql_items = text("""
            SELECT sku,title,qty,unit_price,total_price
            FROM bridge_lines
            WHERE order_id = :oid
            ORDER BY line_index ASC
        """)

        out: List[OrderOut] = []
        total_lines = 0
        revenue_sum = 0.0

        for r in o_rows:
            oid, oc = r["order_id"], r["order_code"]
            items_rows = list(con.execute(sql_items, {"oid": oid}).mappings())
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
                order_id=oid,
                order_code=oc,
                state=r["state"],
                date=_ms_to_iso(r["date_utc_ms"]),
                items=items,
                totals={"revenue": round(revenue, 2)},
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
        params: Dict[str, Any] = {}
        if codes_list:
            parts.append(f"order_code IN ({', '.join(f':c{i}' for i in range(len(codes_list)))})")
            params.update({f"c{i}": v for i, v in enumerate(codes_list)})
        if ids_list:
            parts.append(f"order_id IN ({', '.join(f':i{i}' for i in range(len(ids_list)))})")
            params.update({f"i{i}": v for i, v in enumerate(ids_list)})
        if states:
            parts.append(f"state IN ({', '.join(f':s{i}' for i in range(len(states)))})")
            params.update({f"s{i}": v for i, v in enumerate(states)})
        where_sql = " AND ".join(parts) if parts else "1=0"
        return _collect_orders(where_sql, params, order_dir)

    if not (date_from and date_to):
        raise HTTPException(400, "Either provide (codes/ids) or (date_from & date_to)")
    try:
        start_ms = int(datetime.fromisoformat(date_from).replace(tzinfo=timezone.utc).timestamp() * 1000)
        end_ms = int((datetime.fromisoformat(date_to).replace(tzinfo=timezone.utc).timestamp() + 86400) * 1000) - 1
    except Exception:
        raise HTTPException(400, "date_from/date_to must be YYYY-MM-DD")

    parts = ["date_utc_ms BETWEEN :a AND :b"]
    params = {"a": start_ms, "b": end_ms}
    if states:
        parts.append(f"state IN ({', '.join(f':s{i}' for i in range(len(states)))})")
        params.update({f"s{i}": v for i, v in enumerate(states)})
    return _collect_orders(" AND ".join(parts), params, order_dir)

# ---------- «MS sync» совместимость ----------
@router.post(f"{PFX[0]}/ms/sync-costs")
@router.post(f"{PFX[1]}/ms/sync-costs")
def ms_sync_costs(
    date_from: Optional[str] = Query(None, description="YYYY-MM-DD"),
    date_to: Optional[str] = Query(None),
    _: bool = Depends(require_api_key),
):
    with db() as con:
        if date_from and date_to:
            try:
                start_ms = int(datetime.fromisoformat(date_from).replace(tzinfo=timezone.utc).timestamp() * 1000)
                end_ms = int((datetime.fromisoformat(date_to).replace(tzinfo=timezone.utc).timestamp() + 86400) * 1000) - 1
            except Exception:
                raise HTTPException(400, "date_from/date_to must be YYYY-MM-DD")
            sku_rows = list(con.execute(text("""
                SELECT DISTINCT sku FROM bridge_lines
                WHERE sku IS NOT NULL AND date_utc_ms BETWEEN :a AND :b
            """), {"a": start_ms, "b": end_ms}).mappings())
        else:
            sku_rows = list(con.execute(text("SELECT DISTINCT sku FROM bridge_lines WHERE sku IS NOT NULL")).mappings())

        skus = [r["sku"] for r in sku_rows if r["sku"]]
        if not skus:
            return {"ok": True, "synced": 0, "examples": {}}
        rows = list(con.execute(
            text(f"SELECT sku FROM batches WHERE sku IN ({', '.join(f':s{i}' for i in range(len(skus)))}) GROUP BY sku"),
            {f"s{i}": v for i, v in enumerate(skus)}
        ).mappings())
        present = [r["sku"] for r in rows]
        return {"ok": True, "synced": len(present), "examples": dict((s, True) for s in present[:5])}

# ---------- Обогащённая версия: cost/commission/profit ----------
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
    commission_sum = 0.0

    with db() as con:
        for o in base.orders:
            total_cost = 0.0
            total_commission = 0.0
            for it in o.items:
                sku = (it.sku or "").strip()
                unit_cost, commission_pct = _cost_commission_for_sku(con, sku) if sku else (0.0, 0.0)

                c = round(unit_cost * (it.qty or 1), 2)
                comm = round((commission_pct / 100.0) * float(it.total_price or 0.0), 2)
                p = round((it.total_price or 0.0) - c - comm, 2)

                it.cost = c
                it.commission = comm
                it.profit = p
                total_cost += c
                total_commission += comm

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
