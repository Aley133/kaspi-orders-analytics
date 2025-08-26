# app/api/profit_bridge.py
from __future__ import annotations

import os
from datetime import datetime
from typing import Optional, Literal, Dict, Any, List

from fastapi import APIRouter, Request, HTTPException, status

router = APIRouter(tags=["profit"])  # main.py включает его с prefix="/profit"


# ---------- helpers ----------
def _need_key() -> Optional[str]:
    # Любая из переменных может использоваться для секьюра
    return (
        os.getenv("PROFIT_API_KEY")
        or os.getenv("KASPI_API_KEY")
        or os.getenv("API_KEY")
        or None
    )

def _check_key(req: Request) -> None:
    need = _need_key()
    if not need:
        return
    got = req.headers.get("X-API-Key") or req.query_params.get("api_key")
    if not got:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing API key")
    if got != need:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid API key")


def _driver_from_env() -> str:
    dsn = os.getenv("DATABASE_URL") or os.getenv("DB_DSN") or os.getenv("SQLALCHEMY_DATABASE_URI") or ""
    if dsn.startswith("postgres"):
        return "pg"
    if "sqlite" in dsn or dsn.startswith("file:"):
        return "sqlite"
    return "pg"  # по умолчанию


# ---------- endpoints ----------

@router.get("/db/ping")
async def db_ping(req: Request) -> Dict[str, Any]:
    """Лёгкий пинг для фронта: сообщает тип драйвера и ok."""
    _check_key(req)
    dsn = os.getenv("DATABASE_URL") or os.getenv("DB_DSN") or ""
    return {"driver": _driver_from_env(), "ok": True, "db_path": dsn}


@router.get("/summary")
async def profit_summary(
    req: Request,
    date_from: str,
    date_to: str,
    group_by: Literal["day", "week", "month", "total"] = "day",
    use_bd: Literal["0", "1"] = "0",
    bd_start: str = "20:00",
) -> Dict[str, Any]:
    """
    Обобщённый ответ для KPI и графика. Пока — заглушка с нулями,
    чтобы фронт работал стабильно.
    """
    _check_key(req)

    # Валидация дат (мягкая)
    try:
        _ = datetime.strptime(date_from, "%Y-%m-%d")
        _ = datetime.strptime(date_to, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Bad date format, expected YYYY-MM-DD")

    total = {"revenue": 0, "commission": 0, "cost": 0, "profit": 0}
    rows: List[Dict[str, Any]] = []

    # Для совместимости с фронтом: при group_by="total" можно вернуть пустые rows
    if group_by != "total":
        rows = []  # здесь позже появится реальная агрегация

    return {"currency": "KZT", "total": total, "rows": rows}


@router.get("/by-sku")
async def profit_by_sku(
    req: Request,
    date_from: str,
    date_to: str,
    limit: int = 50,
) -> Dict[str, Any]:
    """
    Топ SKU по прибыли. Заглушка — пустые ряды (фронт просто покажет пустую таблицу).
    """
    _check_key(req)
    try:
        _ = datetime.strptime(date_from, "%Y-%m-%d")
        _ = datetime.strptime(date_to, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Bad date format, expected YYYY-MM-DD")

    return {"rows": []}


@router.post("/rebuild-ledger")
async def rebuild_ledger(
    req: Request,
    date_from: str,
    date_to: str,
) -> Dict[str, Any]:
    """
    Пересчёт FIFO-списаний за период. Сейчас — no-op с нулём пересчитанных записей.
    """
    _check_key(req)
    try:
        _ = datetime.strptime(date_from, "%Y-%m-%d")
        _ = datetime.strptime(date_to, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Bad date format, expected YYYY-MM-DD")

    # TODO: здесь будет реальный пересчёт ledger'а
    return {"recomputed": 0}


@router.post("/sync")
async def sync_orders_into_profit(
    req: Request,
    start: str,
    end: str,
    tz: str = "Asia/Almaty",
    date_field: str = "creationDate",
    use_bd: Literal["0", "1"] = "0",
    business_day_start: str = "20:00",
    states: Optional[str] = None,
    exclude_states: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Синхронизация заказов/позиций в БД FIFO.
    Пока возвращаем «нулевые» итоги, чтобы кнопка в UI не падала.
    """
    _check_key(req)
    # TODO: здесь будет ход в твой источник заказов и запись позиций в таблицы FIFO

    return {
        "synced_orders": 0,
        "items_inserted": 0,
        "params": {
            "start": start,
            "end": end,
            "tz": tz,
            "date_field": date_field,
            "use_bd": use_bd,
            "business_day_start": business_day_start,
            "states": states,
            "exclude_states": exclude_states,
        },
    }
