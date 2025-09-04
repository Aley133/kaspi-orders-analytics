# app/debug_catalog.py
from __future__ import annotations

from typing import Optional, Any, List, Dict
from fastapi import APIRouter, HTTPException, Query

# Пытаемся импортировать модуль products и переиспользовать его «ядро»
try:
    from app.api import products as products_mod  # type: ignore
except Exception:
    try:
        from .api import products as products_mod  # type: ignore
    except Exception:
        import products as products_mod  # type: ignore


def get_catalog_debug_router(client: Optional["KaspiClient"]) -> APIRouter:
    """
    Диагностический роутер каталога:
      GET  /debug/catalog/ping         — проверка связки и что за итератор есть у клиента
      GET  /debug/catalog/sample       — «сухой» выбор каталога (без записи в БД)
    """
    router = APIRouter(tags=["debug_catalog"])

    @router.get("/debug/catalog/ping")
    async def catalog_ping():
        has_client = client is not None and products_mod.KaspiClient is not None
        iter_name = None
        if has_client:
            iter_fn = products_mod._find_iter_fn(client)  # iter_products | iter_offers | iter_catalog
            iter_name = getattr(iter_fn, "__name__", None) if iter_fn else None
        return {
            "ok": True,
            "has_client": bool(has_client),
            "iter_fn_detected": iter_name,
            "db_driver": "pg" if products_mod._USE_PG else "sqlite",
        }

    @router.get("/debug/catalog/sample")
    async def catalog_sample(
        active_only: int = Query(1, description="1 — только активные/видимые позиции"),
        limit: int = Query(50, ge=1, le=20000),
    ):
        if client is None or products_mod.KaspiClient is None:
            raise HTTPException(501, "KaspiClient не сконфигурирован на сервере.")
        # читаем «как есть» через уже готовый сборщик каталога
        items: List[Dict[str, Any]] = products_mod._collect_products_from_kaspi(
            client, active_only=bool(active_only)
        )
        if limit > 0:
            items = items[:limit]
        # Ничего не пишем в БД: это чисто для просмотра формата и фильтрации
        return {"ok": True, "count": len(items), "items": items}

    return router

