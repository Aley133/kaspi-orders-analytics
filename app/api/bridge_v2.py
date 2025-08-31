# app/api/bridge_v2.py
from __future__ import annotations

import os
import sqlite3
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, HTTPException, Query, Body
from pydantic import BaseModel, Field, ValidationError

import math
import time
from datetime import datetime

router = APIRouter()

# ──────────────────────────────────────────────────────────────────────
# Конфигурация БД
# ──────────────────────────────────────────────────────────────────────
DB_URL_ENV = os.getenv("PROFIT_DB_URL") or os.getenv("DATABASE_URL") or "sqlite:///./profit.db"
FALLBACK_SQLITE = os.getenv("BRIDGE_FALLBACK_DB_URL", "sqlite:///./profit_bridge.db")

# Текущее фактическое подключение (может переключиться на фолбэк)
_ACTUAL_DB_URL = DB_URL_ENV
_FALLBACK_USED = False


def _sqlite_path(url: str) -> str:
    return url.split("sqlite:///")[-1]


def _driver_name() -> str:
    return "sqlite" if _ACTUAL_DB_URL.startswith("sqlite") else "pg"


def _get_conn():
    """
    Возвращаем подключение к БД.
    Если указана postgres-строка, но psycopg2 не установлен — тихо уходим в SQLite-фолбэк.
    """
    global _ACTUAL_DB_URL, _FALLBACK_USED

    # SQLite ветка
    if _ACTUAL_DB_URL.startswith("sqlite"):
        c = sqlite3.connect(_sqlite_path(_ACTUAL_DB_URL))
        c.row_factory = sqlite3.Row
        return c

    # Postgres ветка
    try:
        import psycopg2  # type: ignore
        return psycopg2.connect(_ACTUAL_DB_URL)
    except ModuleNotFoundError:
        # Нет драйвера — переключаемся на локальную SQLite
        _ACTUAL_DB_URL = FALLBACK_SQLITE
        _FALLBACK_USED = True
        c = sqlite3.connect(_sqlite_path(_ACTUAL_DB_URL))
        c.row_factory = sqlite3.Row
        return c


def _init_schema() -> None:
    with _get_conn() as c:
        cur = c.cursor()
        if _driver_name() == "sqlite":
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS bridge_sales(
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  order_id   TEXT NOT NULL,
                  line_index INTEGER NOT NULL,
                  order_code TEXT,
                  date_utc_ms BIGINT,
                  state TEXT,
                  sku TEXT NOT NULL,
                  title TEXT,
                  qty INTEGER NOT NULL,
                  unit_price REAL NOT NULL,
                  total_price REAL NOT NULL,
                  CONSTRAINT bridge_sales_uniq UNIQUE(order_id, line_index)
                )
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS ix_bridge_sales_date ON bridge_sales(date_utc_ms)")
            cur.execute("CREATE INDEX IF NOT EXISTS ix_bridge_sales_sku  ON bridge_sales(sku)")
        else:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS bridge_sales(
                  id SERIAL PRIMARY KEY,
                  order_id   TEXT NOT NULL,
                  line_index INTEGER NOT NULL,
                  order_code TEXT,
                  date_utc_ms BIGINT,
                  state TEXT,
                  sku TEXT NOT NULL,
                  title TEXT,
                  qty INTEGER NOT NULL,
                  unit_price DOUBLE PRECISION NOT NULL,
                  total_price DOUBLE PRECISION NOT NULL,
                  CONSTRAINT bridge_sales_uniq UNIQUE(order_id, line_index)
                )
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS ix_bridge_sales_date ON bridge_sales(date_utc_ms)")
            cur.execute("CREATE INDEX IF NOT EXISTS ix_bridge_sales_sku  ON bridge_sales(sku)")
        c.commit()


def _chunked(items: List[Dict[str, Any]], n: int = 500):
    for i in range(0, len(items), n):
        yield items[i : i + n]


def _upsert_rows(rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    _init_schema()
    total = 0
    with _get_conn() as c:
        cur = c.cursor()
        if _driver_name() == "sqlite":
            sql = """
              INSERT INTO bridge_sales
                (order_id,line_index,order_code,date_utc_ms,state,sku,title,qty,unit_price,total_price)
              VALUES
                (:order_id,:line_index,:order_code,:date_utc_ms,:state,:sku,:title,:qty,:unit_price,:total_price)
              ON CONFLICT(order_id,line_index) DO UPDATE SET
                order_code=excluded.order_code,
                date_utc_ms=excluded.date_utc_ms,
                state=excluded.state,
                sku=excluded.sku,
                title=excluded.title,
                qty=excluded.qty,
                unit_price=excluded.unit_price,
                total_price=excluded.total_price
            """
            for ch in _chunked(rows):
                cur.executemany(sql, ch)
                total += cur.rowcount or 0
        else:
            # Postgres
            sql = """
              INSERT INTO bridge_sales
                (order_id,line_index,order_code,date_utc_ms,state,sku,title,qty,unit_price,total_price)
              VALUES
                (%(order_id)s,%(line_index)s,%(order_code)s,%(date_utc_ms)s,%(state)s,%(sku)s,%(title)s,%(qty)s,%(unit_price)s,%(total_price)s)
              ON CONFLICT(order_id,line_index) DO UPDATE SET
                order_code=EXCLUDED.order_code,
                date_utc_ms=EXCLUDED.date_utc_ms,
                state=EXCLUDED.state,
                sku=EXCLUDED.sku,
                title=EXCLUDED.title,
                qty=EXCLUDED.qty,
                unit_price=EXCLUDED.unit_price,
                total_price=EXCLUDED.total_price
            """
            for ch in _chunked(rows):
                cur.executemany(sql, ch)
                total += cur.rowcount or 0
        c.commit()
    return int(total or 0)


def _to_ms(dt: Any) -> Optional[int]:
    if dt is None:
        return None
    if isinstance(dt, (int, float)):
        # уже epoch-ms / epoch-s
        return int(dt if dt > 10_000_000_000 else dt * 1000)
    s = str(dt).strip()
    if not s:
        return None
    # YYYY-MM-DD или ISO
    try:
        if len(s) == 10 and s[4] == "-" and s[7] == "-":
            d = datetime.fromisoformat(s)
            return int(d.timestamp() * 1000)
        return int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp() * 1000)
    except Exception:
        return None


def _norm(s: Any, maxlen: int = 512) -> str:
    v = "" if s is None else str(s)
    return v if len(v) <= maxlen else v[:maxlen]


# ──────────────────────────────────────────────────────────────────────
# Модели входа
# ──────────────────────────────────────────────────────────────────────
class BridgeItemIn(BaseModel):
    sku: str
    qty: int = 1
    unit_price: float = Field(0, description="Цена за единицу или сумма строки")
    total_price: Optional[float] = Field(None, description="Если не задана — берём qty*unit_price")
    title: Optional[str] = None


class BridgeOrderIn(BaseModel):
    id: str
    date: Any
    state: Optional[str] = None
    order_code: Optional[str] = None
    items: List[BridgeItemIn]


class BridgeSyncIn(BaseModel):
    orders: List[BridgeOrderIn]


# ──────────────────────────────────────────────────────────────────────
# Диагностика
# ──────────────────────────────────────────────────────────────────────
@router.get("/db/ping")
def db_ping():
    info: Dict[str, Any] = {"ok": True, "driver": _driver_name(), "fallback_used": _FALLBACK_USED}
    info["db_path"] = _sqlite_path(_ACTUAL_DB_URL) if _driver_name() == "sqlite" else _ACTUAL_DB_URL
    return info


# ──────────────────────────────────────────────────────────────────────
# Загрузка позиций заказов (из фронта/пайплайна)
# ──────────────────────────────────────────────────────────────────────
@router.post("/bridge/sync-by-ids")
def bridge_sync_by_ids(payload: BridgeSyncIn = Body(...)):
    """
    Пишем позиции заказов (order_id + line_index + sku + qty + суммы).
    """
    rows: List[Dict[str, Any]] = []
    for order in payload.orders:
        date_ms = _to_ms(order.date)
        for idx, it in enumerate(order.items or []):
            total = it.total_price if it.total_price is not None else float(it.qty or 0) * float(it.unit_price or 0)
            rows.append(
                dict(
                    order_id=str(order.id),
                    line_index=int(idx),
                    order_code=_norm(order.order_code, 64),
                    date_utc_ms=int(date_ms or 0),
                    state=_norm(order.state, 64),
                    sku=_norm(it.sku, 128),
                    title=_norm(it.title, 512),
                    qty=int(it.qty or 1),
                    unit_price=float(it.unit_price or 0),
                    total_price=float(total or 0),
                )
            )
    inserted = _upsert_rows(rows)
    return {"ok": True, "rows_upserted": inserted, "orders_inserted": len(payload.orders)}


# ──────────────────────────────────────────────────────────────────────
# Выгрузка «номер заказа ↔ SKU» за период
# ──────────────────────────────────────────────────────────────────────
@router.get("/bridge/list")
def bridge_list(
    sku: str = Query(..., description="Искомый SKU"),
    date_from: str = Query(..., description="YYYY-MM-DD"),
    date_to: str = Query(..., description="YYYY-MM-DD"),
    limit: int = Query(1000, ge=1, le=100000),
    order: str = Query("asc", pattern="^(asc|desc)$"),
):
    _init_schema()
    ms_from = _to_ms(date_from)
    ms_to = _to_ms(date_to)
    if ms_from is None or ms_to is None:
        raise HTTPException(400, "date_from/date_to должны быть в формате YYYY-MM-DD")
    # включительно
    ms_to = ms_to + 24 * 3600 * 1000 - 1

    with _get_conn() as c:
        cur = c.cursor()
        if _driver_name() == "sqlite":
            cur.execute(
                f"""
                SELECT order_id, order_code, date_utc_ms, state, sku, title, qty, unit_price, total_price
                FROM bridge_sales
                WHERE sku = ? AND date_utc_ms BETWEEN ? AND ?
                ORDER BY date_utc_ms {"ASC" if order == "asc" else "DESC"}
                LIMIT ?
                """,
                (sku, ms_from, ms_to, int(limit)),
            )
            rows = [dict(r) for r in cur.fetchall()]
        else:
            cur.execute(
                f"""
                SELECT order_id, order_code, date_utc_ms, state, sku, title, qty, unit_price, total_price
                FROM bridge_sales
                WHERE sku = %(sku)s AND date_utc_ms BETWEEN %(ms_from)s AND %(ms_to)s
                ORDER BY date_utc_ms {"ASC" if order == "asc" else "DESC"}
                LIMIT %(lim)s
                """,
                dict(sku=sku, ms_from=ms_from, ms_to=ms_to, lim=int(limit)),
            )
            rows = [dict(zip([d[0] for d in cur.description], r)) for r in cur.fetchall()]

    # маленькое удобство фронту: ISO-дата и «сумма строки», если unit_price фактически цена строки
    out = []
    for r in rows:
        d_iso = datetime.utcfromtimestamp((r.get("date_utc_ms") or 0) / 1000).isoformat(timespec="seconds")
        out.append(
            dict(
                order_id=r.get("order_id"),
                order_code=r.get("order_code"),
                date=d_iso,
                state=r.get("state"),
                sku=r.get("sku"),
                title=r.get("title"),
                qty=r.get("qty"),
                unit_price=r.get("unit_price"),
                total_price=r.get("total_price"),
            )
        )

    return {
        "sku": sku,
        "date_from": date_from,
        "date_to": date_to,
        "count": len(out),
        "items": out,
        "driver": _driver_name(),
        "fallback_used": _FALLBACK_USED,
    }
