# app/api/bridge_v2.py
from __future__ import annotations

import base64
import os
import sqlite3
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel, Field

# --------------------------------------------------------------------------------------
# Router (регистрация сразу на два префикса, чтобы не ловить 404)
# --------------------------------------------------------------------------------------
router = APIRouter(tags=["bridge_v2"])

# Все пути, которые будет вешать каждый эндпоинт
PFX = ("/profit/bridge", "/bridge")

# --------------------------------------------------------------------------------------
# Конфиг
# --------------------------------------------------------------------------------------
DB_PATH = os.getenv("BRIDGE_DB_PATH", "data/bridge_v2.sqlite3")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# API-ключ (если пусто — доступ без ключа)
REQ_API_KEY = os.getenv("BRIDGE_API_KEY", "").strip() or None

# МойСклад
MS_BASE = os.getenv("MS_BASE", "https://api.moysklad.ru/api/remap/1.2").rstrip("/")
MS_LOGIN = os.getenv("MS_LOGIN", "").strip()
MS_PASSWORD = os.getenv("MS_PASSWORD", "").strip()
MS_BASIC = os.getenv("MS_BASIC", "").strip()  # готовая строка Basic (base64(login:pass))
HTTPX_TIMEOUT = httpx.Timeout(connect=10.0, read=40.0, write=15.0, pool=40.0)
HTTPX_LIMITS = httpx.Limits(max_connections=20, max_keepalive_connections=10)

# --------------------------------------------------------------------------------------
# Security
# --------------------------------------------------------------------------------------
def require_api_key(request: Request):
    """Если указан BRIDGE_API_KEY — требуем X-API-Key или ?api_key=."""
    if not REQ_API_KEY:
        return True
    provided = request.headers.get("X-API-Key") or request.query_params.get("api_key")
    if provided != REQ_API_KEY:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")
    return True

# --------------------------------------------------------------------------------------
# DB helpers
# --------------------------------------------------------------------------------------
def _connect() -> sqlite3.Connection:
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c

def _init_db():
    with _connect() as con:
        # сырые строки из ids
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
        con.execute("CREATE INDEX IF NOT EXISTS ix_lines_date ON bridge_lines(date_utc_ms)")
        con.execute("CREATE INDEX IF NOT EXISTS ix_lines_state ON bridge_lines(state)")
        con.execute("CREATE INDEX IF NOT EXISTS ix_lines_sku   ON bridge_lines(sku)")

        # кэш себестоимостей из МС (средняя/входная цена)
        con.execute("""
            CREATE TABLE IF NOT EXISTS ms_costs(
              sku        TEXT PRIMARY KEY,
              cost       REAL DEFAULT 0,
              updated_at INTEGER
            )
        """)

        # заготовки под FIFO (на следующий шаг)
        con.execute("""
            CREATE TABLE IF NOT EXISTS batches(
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              sku TEXT NOT NULL,
              date_utc_ms INTEGER,
              qty REAL NOT NULL,
              unit_cost REAL NOT NULL,
              source TEXT,
              unique(sku, date_utc_ms, unit_cost, qty)
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS fifo_ledger(
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              order_code TEXT,
              order_id   TEXT,
              date_utc_ms INTEGER,
              sku TEXT,
              qty REAL,
              unit_price REAL,
              revenue REAL,
              cost REAL,
              commission REAL,
              profit REAL
            )
        """)
        con.commit()
_init_db()

# --------------------------------------------------------------------------------------
# Модели
# --------------------------------------------------------------------------------------
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
    line_index: int = 0

class OrderItemOut(BaseModel):
    sku: Optional[str] = None
    title: Optional[str] = None
    qty: int = 1
    unit_price: float = 0.0
    total_price: float = 0.0
    # обогащение:
    cost: Optional[float] = None
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

# --------------------------------------------------------------------------------------
# Утилиты
# --------------------------------------------------------------------------------------
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

def _parse_states(raw: Optional[str]) -> Optional[List[str]]:
    if not raw:
        return None
    vals = [p.strip() for p in str(raw).split(",") if p.strip()]
    return vals or None

def _canon_sku(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    v = str(s).strip()
    return v or None

# --------------------------------------------------------------------------------------
# МойСклад: авторизация + fetch
# --------------------------------------------------------------------------------------
def _ms_headers() -> Dict[str, str]:
    auth = ""
    if MS_BASIC:
        auth = f"Basic {MS_BASIC}"
    elif MS_LOGIN and MS_PASSWORD:
        token = base64.b64encode(f"{MS_LOGIN}:{MS_PASSWORD}".encode("utf-8")).decode("ascii")
        auth = f"Basic {token}"
    if not auth:
        raise HTTPException(400, "MS auth is not configured (set MS_BASIC or MS_LOGIN/MS_PASSWORD)")
    return {
        "Authorization": auth,
        "Accept": "application/json;charset=utf-8",
        "Content-Type": "application/json;charset=utf-8",
    }

async def _ms_fetch_cost_for_sku(cli: httpx.AsyncClient, sku: str) -> Optional[float]:
    """
    Пытаемся найти product по article==SKU (а затем по code==SKU) и взять buyPrice.value.
    Возвращаем None если не нашли.
    """
    try:
        # По article
        r = await cli.get(
            f"{MS_BASE}/entity/product",
            params={"limit": 1, "filter": f"article={sku}"},
            headers=_ms_headers(),
        )
        r.raise_for_status()
        j = r.json()
        if (j.get("meta", {}).get("size") or 0) > 0 and j.get("rows"):
            row = j["rows"][0]
            bp = (row.get("buyPrice") or {}).get("value")
            if bp is not None:
                return float(bp)
        # По code
        r = await cli.get(
            f"{MS_BASE}/entity/product",
            params={"limit": 1, "filter": f"code={sku}"},
            headers=_ms_headers(),
        )
        r.raise_for_status()
        j = r.json()
        if (j.get("meta", {}).get("size") or 0) > 0 and j.get("rows"):
            row = j["rows"][0]
            bp = (row.get("buyPrice") or {}).get("value")
            if bp is not None:
                return float(bp)
    except Exception:
        return None
    return None

async def _ms_sync_costs(skus: Iterable[str]) -> Dict[str, float]:
    uniq = sorted({s for s in (skus or []) if s})
    if not uniq:
        return {}
    out: Dict[str, float] = {}
    async with httpx.AsyncClient(timeout=HTTPX_TIMEOUT, limits=HTTPX_LIMITS) as cli:
        for s in uniq:
            c = await _ms_fetch_cost_for_sku(cli, s)
            if c is not None:
                out[s] = float(c)
    # Сохраним в БД
    now = int(time.time() * 1000)
    with _connect() as con:
        con.executemany(
            "INSERT INTO ms_costs(sku, cost, updated_at) VALUES(?,?,?) "
            "ON CONFLICT(sku) DO UPDATE SET cost=excluded.cost, updated_at=excluded.updated_at",
            [(k, v, now) for k, v in out.items()],
        )
        con.commit()
    return out

def _get_cost_map_for_skus(skus: Iterable[str]) -> Dict[str, float]:
    keys = sorted({s for s in skus if s})
    if not keys:
        return {}
    q = ",".join(["?"] * len(keys))
    with _connect() as con:
        rows = list(con.execute(f"SELECT sku,cost FROM ms_costs WHERE sku IN ({q})", keys))
    return {r["sku"]: float(r["cost"] or 0.0) for r in rows}

# --------------------------------------------------------------------------------------
# Endpoints
# --------------------------------------------------------------------------------------
@router.get(f"{PFX[0]}/ping")
@router.get(f"{PFX[1]}/ping")
def ping():
    with _connect() as con:
        c = int(con.execute("SELECT COUNT(*) FROM bridge_lines").fetchone()[0])
        m = int(con.execute("SELECT COUNT(*) FROM ms_costs").fetchone()[0])
    return {"ok": True, "db": DB_PATH, "lines": c, "ms_costs": m, "ts": int(time.time() * 1000)}

@router.post(f"{PFX[0]}/sync-by-ids")
@router.post(f"{PFX[1]}/sync-by-ids")
def sync_by_ids(items: List[BridgeLineIn], _: bool = Depends(require_api_key)):
    """
    Принимаем плоские строки (результат ids.async) и UPSERT по (order_id, line_index)
    """
    if not items:
        return {"inserted": 0, "updated": 0, "skipped": 0}

    inserted = 0
    updated = 0
    skipped = 0
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
            try:
                unit = float(it.unit_price or 0.0)
            except Exception:
                unit = 0.0
            try:
                total = float(it.total_price or (unit * qty))
            except Exception:
                total = unit * qty
            line_index = int(it.line_index or 0)

            con.execute(sql, (order_id, order_code, state, date_ms, sku, title, qty, unit, total, line_index))
            updated += 1
        con.commit()

    processed = len(items) - skipped
    if updated == 0 and processed > 0:
        inserted = processed
    else:
        inserted = max(0, processed - updated)
    return {"inserted": inserted, "updated": updated, "skipped": skipped}

@router.get(f"{PFX[0]}/list")
@router.get(f"{PFX[1]}/list")
def list_by_sku(
    sku: str = Query(...),
    date_from: str = Query(...),
    date_to: str = Query(...),
    limit: int = Query(100000, ge=1, le=200000),
    order: str = Query("asc"),
    _: bool = Depends(require_api_key),
):
    df, dt = _to_ms(date_from), _to_ms(date_to)
    if df is None or dt is None:
        # date_from/to приходит как YYYY-MM-DD -> _to_ms понимает
        try:
            df = int(datetime.fromisoformat(date_from).replace(tzinfo=timezone.utc).timestamp() * 1000)
            dt = int((datetime.fromisoformat(date_to).replace(tzinfo=timezone.utc).timestamp() + 86400) * 1000) - 1
        except Exception:
            raise HTTPException(400, "date_from/date_to must be YYYY-MM-DD")
    else:
        dt = dt + 24*3600*1000 - 1

    with _connect() as con:
        rows = list(
            con.execute(
                f"""SELECT order_id, order_code, date_utc_ms, state, sku, title, qty, unit_price, total_price
                    FROM bridge_lines
                    WHERE sku=? AND date_utc_ms BETWEEN ? AND ?
                    ORDER BY date_utc_ms {'ASC' if order.lower()=='asc' else 'DESC'}, order_id, line_index
                    LIMIT ?""",
                (sku, df, dt, int(limit)),
            )
        )
    items = []
    for r in rows:
        items.append(
            dict(
                order_id=r["order_id"],
                order_code=r["order_code"],
                date=_ms_to_iso(r["date_utc_ms"]),
                state=r["state"],
                sku=r["sku"],
                title=r["title"],
                qty=int(r["qty"] or 1),
                unit_price=float(r["unit_price"] or 0.0),
                total_price=float(r["total_price"] or 0.0),
            )
        )
    return {"sku": sku, "date_from": date_from, "date_to": date_to, "count": len(items), "items": items}

@router.get(f"{PFX[0]}/by-orders")
@router.get(f"{PFX[1]}/by-orders")
def by_orders(
    date_from: str = Query(..., description="YYYY-MM-DD"),
    date_to: str = Query(..., description="YYYY-MM-DD"),
    state: Optional[str] = Query(None, description="CSV статусов"),
    order: str = Query("asc"),
    _: bool = Depends(require_api_key),
) -> OrdersResponse:
    try:
        start_ms = int(datetime.fromisoformat(date_from).replace(tzinfo=timezone.utc).timestamp() * 1000)
        end_ms = int((datetime.fromisoformat(date_to).replace(tzinfo=timezone.utc).timestamp() + 86400) * 1000) - 1
    except Exception:
        raise HTTPException(400, "date_from/date_to must be YYYY-MM-DD")

    states = _parse_states(state)
    order_dir = "ASC" if order.lower() == "asc" else "DESC"

    with _connect() as con:
        where = ["date_utc_ms BETWEEN ? AND ?"]
        params: List[Any] = [start_ms, end_ms]
        if states:
            where.append("state IN (%s)" % ",".join("?" for _ in states))
            params.extend(states)

        sql_orders = f"""
            SELECT order_id, order_code, MIN(date_utc_ms) AS date_utc_ms, MAX(state) as state
            FROM bridge_lines
            WHERE {' AND '.join(where)}
            GROUP BY order_id, order_code
            ORDER BY date_utc_ms {order_dir}
        """
        o_rows = list(con.execute(sql_orders, params))
        sql_items = "SELECT sku,title,qty,unit_price,total_price FROM bridge_lines WHERE order_id=? ORDER BY line_index ASC"

        out: List[OrderOut] = []
        for r in o_rows:
            oid, oc = r["order_id"], r["order_code"]
            items_rows = list(con.execute(sql_items, (oid,)))
            items: List[OrderItemOut] = []
            revenue = 0.0
            for ir in items_rows:
                qty = int(ir["qty"] or 1)
                unit = float(ir["unit_price"] or 0.0)
                tot = float(ir["total_price"] or (unit * qty))
                revenue += tot
                items.append(
                    OrderItemOut(
                        sku=ir["sku"], title=ir["title"], qty=qty, unit_price=unit, total_price=tot
                    )
                )
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
    return OrdersResponse(orders=out, source_used="bridge_v2")

# ---------- Обогащённая версия: подбираем себестоимость из МойСклад (не FIFO) ----------
@router.post(f"{PFX[0]}/ms/sync-costs")
@router.post(f"{PFX[1]}/ms/sync-costs")
async def ms_sync_costs(
    date_from: Optional[str] = Query(None, description="YYYY-MM-DD; если не задано — все уникальные SKU"),
    date_to: Optional[str] = Query(None),
    _: bool = Depends(require_api_key),
):
    # соберём SKU из БД (за период, если задан)
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
    synced = await _ms_sync_costs(skus)
    return {"ok": True, "synced": len(synced), "examples": dict(list(synced.items())[:5])}

@router.get(f"{PFX[0]}/by-orders-enriched")
@router.get(f"{PFX[1]}/by-orders-enriched")
def by_orders_enriched(
    date_from: str = Query(..., description="YYYY-MM-DD"),
    date_to: str = Query(..., description="YYYY-MM-DD"),
    state: Optional[str] = Query(None, description="CSV статусов"),
    order: str = Query("asc"),
    _: bool = Depends(require_api_key),
) -> OrdersResponse:
    """
    То же, что /by-orders, но дополняем строки cost/profit, используя ms_costs.
    (Это НЕ FIFO, а средняя входная цена по SKU. FIFO подключим в этом же модуле позже.)
    """
    base = by_orders(date_from=date_from, date_to=date_to, state=state, order=order, _=True)
    # соберём все SKU
    skus: List[str] = []
    for o in base.orders:
        for it in o.items:
            if it.sku:
                skus.append(it.sku)
    cost_map = _get_cost_map_for_skus(skus)

    # проставим себестоимость и прибыль
    for o in base.orders:
        total_cost = 0.0
        for it in o.items:
            unit_cost = float(cost_map.get(it.sku or "", 0.0))
            c = round(unit_cost * (it.qty or 1), 2)
            p = round((it.total_price or 0.0) - c, 2)
            it.cost = c
            it.profit = p
            total_cost += c
        rev = float(o.totals.get("revenue", 0.0))
        o.totals["cost"] = round(total_cost, 2)
        o.totals["profit"] = round(rev - total_cost, 2)

    return base
