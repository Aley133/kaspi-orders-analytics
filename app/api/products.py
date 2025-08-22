
# app/api/products.py
from __future__ import annotations

from typing import Callable, Optional, List, Dict, Any
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response

try:
    from ..kaspi_client import KaspiClient  # type: ignore
except Exception:  # pragma: no cover
    from kaspi_client import KaspiClient  # type: ignore


def _pick(attrs: Dict[str, Any], *keys: str) -> str:
    for k in keys:
        v = attrs.get(k)
        if v not in (None, ""):
            return str(v)
    return ""


def _find_iter_fn(client: Any) -> Optional[Callable]:
    for name in ("iter_products", "iter_offers", "iter_catalog"):
        if hasattr(client, name):
            return getattr(client, name)
    return None


def get_products_router(client: Optional["KaspiClient"]) -> APIRouter:
    router = APIRouter(tags=["products"])

    @router.get("/export.csv")
    async def export_products_csv(
        active: int = Query(1, description="1 — только активные, 0 — все")
    ):
        if client is None:
            raise HTTPException(status_code=500, detail="KASPI_TOKEN is not set")

        iter_fn = _find_iter_fn(client)
        if iter_fn is None:
            raise HTTPException(
                status_code=501,
                detail=(
                    "В kaspi_client отсутствует метод итерации по товарам. "
                    "Ожидается iter_products(...) или iter_offers(...)."
                ),
            )

        rows: List[List[str]] = []
        seen = set()

        def add_row(item: Dict[str, Any]):
            attrs = item.get("attributes", {}) or {}

            pid = item.get("id") or _pick(attrs, "id", "sku", "code", "offerId")
            if pid in seen:
                return
            seen.add(pid)

            code = _pick(attrs, "code", "sku", "offerId", "article")
            name = _pick(attrs, "name", "title", "productName", "offerName")
            price = _pick(attrs, "price", "basePrice", "salePrice", "currentPrice")
            qty = _pick(attrs, "quantity", "availableAmount", "stockQuantity", "qty")
            brand = _pick(attrs, "brand", "producer", "manufacturer")
            category = _pick(attrs, "category", "categoryName", "group")
            barcode = _pick(attrs, "barcode", "ean")

            active_val = _pick(attrs, "active", "isActive", "isPublished", "visible", "isVisible", "status")
            if isinstance(active_val, str) and active_val:
                av = active_val.lower()
                if av in ("true", "1", "yes", "on", "published"):
                    active_str = "1"
                elif av in ("false", "0", "no", "off", "unpublished"):
                    active_str = "0"
                else:
                    active_str = active_val
            else:
                active_str = str(active_val)

            rows.append([pid, code, name, price, qty, active_str, brand, category, barcode])

        try:
            for item in iter_fn(active_only=bool(active)):
                add_row(item)
        except TypeError:
            for item in iter_fn():
                add_row(item)

        def esc(s: str) -> str:
            if any(c in s for c in [",", '"', "\n"]):
                return '"' + s.replace('"', '""') + '"'
            return s

        header = "id,code,name,price,qty,active,brand,category,barcode\n"
        body = "".join([",".join(esc(x) for x in r) + "\n" for r in rows])
        csv = header + body

        return Response(
            content=csv,
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": 'attachment; filename="products.csv"'},
        )

    return router
