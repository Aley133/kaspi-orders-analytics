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

router = APIRouter(tags=["bridge_v2"])
PFX = ("/profit/bridge", "/bridge")

# ------------------------------ Config ------------------------------
DB_PATH = os.getenv("BRIDGE_DB_PATH", "data/bridge_v2.sqlite3")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
REQ_API_KEY = os.getenv("BRIDGE_API_KEY", "").strip() or None

MS_BASE = os.getenv("MS_BASE", "https://api.moysklad.ru/api/remap/1.2").rstrip("/")
MS_LOGIN = os.getenv("MS_LOGIN", "").strip()
MS_PASSWORD = os.getenv("MS_PASSWORD", "").strip()
MS_BASIC = os.getenv("MS_BASIC", "").strip()

HTTPX_TIMEOUT = httpx.Timeout(connect=10.0, read=40.0, write=15.0, pool=40.0)
HTTPX_LIMITS = httpx.Limits(max_connections=30, max_keepalive_connections=12)

# Если нужен «жёсткий» фильтр по состоянию при записи (например, только KASPI_DELIVERY):
ONLY_STATE = (os.getenv("BRIDGE_ONLY_STATE") or "").strip() or None


# ------------------------------ Security ------------------------------
def require_api_key(request: Request):
    if not REQ_API_KEY:
        return True
    provided = request.headers.get("X-API-Key") or request.query_params.get("api_key")
    if provided != REQ_API_KEY:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")
    return True


# ------------------------------ DB ------------------------------
def _connect() -> sqlite3.Connection:
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c


def _init_db():
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

        con.execute("""
            CREATE TABLE IF NOT EXISTS ms_costs(
              sku        TEXT PRIMARY KEY,
              cost       REAL DEFAULT 0,
              updated_at INTEGER
            )
        """)
        con.commit()
_init_db()


# ------------------------------ Models ------------------------------
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


# ------------------------------ Utils ------------------------------
def _to_ms(value: Any) -> Optional[int]:
    if value is None:
        return None
    # int seconds/ms
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


def _canon_sku(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    v = str(s).strip()
    return v or None


# ------------------------------ MoySklad ------------------------------
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


async def _ms_buyprice_for_product(cli: httpx.AsyncClient, entity: str, _id_or_filter: str, by_filter: bool) -> Optional[float]:
    """
    Возвращает buyPrice.value/100 для product/variant.
    entity: 'product' | 'variant'
    _id_or_filter: id (если by_filter=False) или filter=... (если True)
    """
    url = f"{MS_BASE}/entity/{entity}"
    params: Dict[str, str] = {"limit": "1"}
    if by_filter:
        params.update({"filter": _id_or_filter})
    else:
        url = f"{url}/{_id_or_filter}"

    r = await cli.get(url, params=params if by_filter else None, headers=_ms_headers())
    r.raise_for_status()
    data = r.json()
    row = None
    if by_filter:
        if (data.get("meta", {}).get("size") or 0) > 0 and data.get("rows"):
            row = data["rows"][0]
    else:
        row = data

    if not isinstance(row, dict):
        return None

    # Основной источник — buyPrice.value (в копейках/тыйынах)
    bp = (row.get("buyPrice") or {}).get("value")
    if bp is not None:
        try:
            return float(bp) / 100.0
        except Exception:
            return None

    # Иногда buyPrice пуст — попробуем из supplyPrices/minPrice/salePrices (всё тоже /100).
    # Это эвристика, можно расширить при необходимости.
    for key in ("minPrice",):
        v = (row.get(key) or {}).get("value")
        if v is not None:
            try:
                return float(v) / 100.0
            except Exception:
                pass

    # salePrices — это цены продажи, но на крайний случай
    if isinstance(row.get("salePrices"), list) and row["salePrices"]:
        v = (row["salePrices"][0] or {}).get("value")
        if v is not None:
            try:
                return float(v) / 100.0
            except Exception:
                pass
    return None


async def _ms_fetch_cost_for_sku(cli: httpx.AsyncClient, sku: str) -> Optional[float]:
    """
    Ищем по variant и product, пробуем article/code/externalCode и общий search.
    Возвращаем float в денежных единицах (value/100).
    """
    sku = str(sku).strip()
    if not sku:
        return None

    # 1) Прямые точные фильтры
    filters = [
        ("variant", f"code={sku}"),
        ("variant", f"article={sku}"),
        ("variant", f"externalCode={sku}"),
        ("product", f"code={sku}"),
        ("product", f"article={sku}"),
        ("product", f"externalCode={sku}"),
    ]
    for ent, flt in filters:
        try:
            v = await _ms_buyprice_for_product(cli, ent, flt, by_filter=True)
            if v is not None:
                return v
        except httpx.HTTPError:
            pass

    # 2) Поиск search= (возвращает список; берём первую строку)
    try:
        r = await cli.get(f"{MS_BASE}/entity/product", params={"limit": 10, "search": sku}, headers=_ms_headers())
        r.raise_for_status()
        j = r.json()
        row = (j.get("rows") or [None])[0]
        if row and row.get("id"):
            v = await _ms_buyprice_for_product(cli, "product", row["id"], by_filter=False)
            if v is not None:
                return v
    except httpx.HTTPError:
        pass

    try:
        r = await cli.get(f"{MS_BASE}/entity/variant", params={"limit": 10, "search": sku}, headers=_ms_headers())
        r.raise_for_status()
        j = r.json()
        row = (j.get("rows") or [None])[0]
        if row and row.get("id"):
            v = await _ms_buyprice_for_product(cli, "variant", row["id"], by_filter=False)
            if v is not None:
                return v
    except httpx.HTTPError:
        pass

    # 3) На крайний случай — если sku вида "123_456" — пробуем правую/левую часть.
    if "_" in sku:
        left, right = sku.split("_", 1)
        for part in (right, left):
            if part and part != sku:
                v = await _ms_fetch_cost_for_sku(cli, part)
                if v is not None:
                    return v

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


# ------------------------------ Endpoints ------------------------------
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
    Принимаем плоские строки (как делает фронт из «Номера заказов (для сверки)»)
    — и сохраняем позиции. Если total не пришёл, считаем unit*qty.
    Можно включить жёсткую фильтрацию по состоянию через ONLY_STATE.
    """
    if not items:
        return {"inserted": 0, "updated": 0, "skipped": 0}

    inserted = 0
    updated = 0
    skipped = 0

    # локальный автоиндекс строк, если line_index не передан
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
            order_id = (it.id or "").strip()
            if not order_id:
                skipped += 1
                continue
            if ONLY_STATE and it.state and it.state != ONLY_STATE:
                skipped += 1
                continue

            order_code = (it.code or "").strip() or None
            state = (it.state or "").strip() or None
            date_ms = _to_ms(it.date)
            sku = _canon_sku(it.sku)
            title = (it.title or "").strip() or None

            # qty
            try:
                qty = int(it.qty or 1)
            except Exception:
                qty = 1

            # total: (total_price | amount) || unit*qty || 0
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

            # unit: если не пришёл — из total/qty
            unit = None
            if it.unit_price is not None:
                try:
                    unit = float(it.unit_price)
                except Exception:
                    unit = None
            if unit is None:
                unit = float(total) / max(1, qty)

            # индекс строки
            if it.line_index is not None:
                line_index = int(it.line_index)
            else:
                line_index = counters.get(order_id, 0)
                counters[order_id] = line_index + 1

            con.execute(sql, (order_id, order_code, state, date_ms, sku, title, qty, unit, total, line_index))
            updated += 1
        con.commit()

    processed = len(items) - skipped
    inserted = max(0, processed - updated)
    return {"inserted": inserted, "updated": updated, "skipped": skipped}


def _collect_orders(where_sql: str, params: List[Any], order_dir: str) -> OrdersResponse:
    with _connect() as con:
        sql_orders = f"""
            SELECT order_id, order_code, MIN(date_utc_ms) AS date_utc_ms, MAX(state) as state
            FROM bridge_lines
            WHERE {where_sql}
            GROUP BY order_id, order_code
            ORDER BY date_utc_ms {order_dir}
        """
        o_rows = list(con.execute(sql_orders, params))
        sql_items = "SELECT sku,title,qty,unit_price,total_price FROM bridge_lines WHERE order_id=? ORDER BY line_index ASC"

        out: List[OrderOut] = []
        total_lines = 0
        revenue_sum = 0.0

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
                total_lines += 1
                items.append(
                    OrderItemOut(
                        sku=ir["sku"], title=ir["title"], qty=qty, unit_price=unit, total_price=tot
                    )
                )
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

    # иначе — по датам
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


# ---------- Обогащённая версия: добавляем себестоимость из МС ----------
@router.post(f"{PFX[0]}/ms/sync-costs")
@router.post(f"{PFX[1]}/ms/sync-costs")
async def ms_sync_costs(
    date_from: Optional[str] = Query(None, description="YYYY-MM-DD"),
    date_to: Optional[str] = Query(None),
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
    synced = await _ms_sync_costs(skus)
    return {"ok": True, "synced": len(synced), "examples": dict(list(synced.items())[:5])}


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
    skus: List[str] = []
    for o in base.orders:
        for it in o.items:
            if it.sku:
                skus.append(it.sku)
    cost_map = _get_cost_map_for_skus(skus)

    revenue_sum = 0.0
    cost_sum = 0.0

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
        revenue_sum += rev
        cost_sum += total_cost

    base.stats = {
        "orders": base.stats.get("orders", len(base.orders)),
        "lines": base.stats.get("lines", sum(len(o.items) for o in base.orders)),
        "revenue": round(revenue_sum, 2),
        "cost": round(cost_sum, 2),
        "profit": round(revenue_sum - cost_sum, 2),
    }
    return base
