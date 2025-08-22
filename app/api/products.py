# app/api/products.py
from __future__ import annotations
from typing import Callable, Optional, List, Dict, Any, Tuple
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response, JSONResponse

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


def _normalize_active(val: Any) -> Optional[bool]:
    if val is None or val == "":
        return None
    if isinstance(val, bool):
        return val
    s = str(val).strip().lower()
    if s in ("1", "true", "yes", "on", "published", "active"):
        return True
    if s in ("0", "false", "no", "off", "unpublished", "inactive"):
        return False
    return None


def _num(x: Any) -> float:
    try:
        return float(x)
    except Exception:
        try:
            return float(str(x).replace(" ", "").replace(",", "."))
        except Exception:
            return 0.0


def _find_iter_fn(client: Any) -> Optional[Callable]:
    for name in ("iter_products", "iter_offers", "iter_catalog"):
        if hasattr(client, name):
            return getattr(client, name)
    return None


def _collect_products(client: KaspiClient, active_only: Optional[bool]) -> Tuple[List[Dict[str, Any]], int, Optional[str]]:
    iter_fn = _find_iter_fn(client)
    if iter_fn is None:
        raise HTTPException(
            status_code=501,
            detail="В kaspi_client нет метода каталога. Ожидается iter_products/iter_offers/iter_catalog."
        )

    items: List[Dict[str, Any]] = []
    seen = set()
    total = 0
    note: Optional[str] = None

    def add_row(item: Dict[str, Any]):
        nonlocal total
        attrs = item.get("attributes", {}) or {}
        pid = item.get("id") or _pick(attrs, "id", "sku", "code", "offerId")
        if not pid or pid in seen:
            return
        seen.add(pid)
        total += 1

        code = _pick(attrs, "code", "sku", "offerId", "article")
        name = _pick(attrs, "name", "title", "productName", "offerName")
        price = _num(_pick(attrs, "price", "basePrice", "salePrice", "currentPrice"))
        qty = int(_num(_pick(attrs, "quantity", "availableAmount", "stockQuantity", "qty")))
        brand = _pick(attrs, "brand", "producer", "manufacturer")
        category = _pick(attrs, "category", "categoryName", "group")
        barcode = _pick(attrs, "barcode", "ean")
        active_val = _normalize_active(_pick(attrs, "active", "isActive", "isPublished", "visible", "isVisible", "status"))

        if active_only is not None and active_val is not None and active_only and active_val is False:
            return

        items.append({
            "id": pid,
            "code": code,
            "name": name,
            "price": price,
            "qty": qty,
            "active": True if active_val else False if active_val is False else None,
            "brand": brand,
            "category": category,
            "barcode": barcode,
        })

    # пробуем вызвать с флагом активности; если сигнатура другая — без аргументов
    try:
        try:
            for it in iter_fn(active_only=bool(active_only) if active_only is not None else True):
                add_row(it)
        except TypeError:
            for it in iter_fn():
                add_row(it)
    except Exception:
        # ничего страшного: вернём пусто и подсказку
        return [], 0, ("Каталог по API недоступен. Укажите KASPI_PRODUCTS_ENDPOINTS/KASPI_CITY_ID "
                       "или используйте автодетект в kaspi_client.iter_products().")

    return items, total, note


def get_products_router(client: Optional["KaspiClient"]) -> APIRouter:
    router = APIRouter(tags=["products"])

    @router.get("/export.csv")
    async def export_products_csv(active: int = Query(1, description="1 — только активные, 0 — все")):
        if client is None:
            raise HTTPException(status_code=500, detail="KASPI_TOKEN is not set")

        items, _, _ = _collect_products(client, active_only=bool(active))

        def esc(s: str) -> str:
            s = "" if s is None else str(s)
            if any(c in s for c in [",", '"', "\n"]):
                s = '"' + s.replace('"', '""') + '"'
            return s

        header = "id,code,name,price,qty,active,brand,category,barcode\n"
        body = "".join([",".join(esc(x) for x in [
            r["id"], r["code"], r["name"], r["price"], r["qty"],
            1 if r["active"] else 0 if r["active"] is False else "",
            r["brand"], r["category"], r["barcode"]
        ]) + "\n" for r in items])
        csv = header + body
        return Response(content=csv, media_type="text/csv; charset=utf-8",
                        headers={"Content-Disposition": 'attachment; filename="products.csv"'})

    @router.get("/list")
    async def list_products(
        active: int = Query(1, description="1 — только активные, 0 — все"),
        q: Optional[str] = Query(None, description="поиск по названию/коду"),
        page: int = Query(1, ge=1),
        page_size: int = Query(500, ge=1, le=2000),
    ):
        if client is None:
            raise HTTPException(status_code=500, detail="KASPI_TOKEN is not set")

        items, total, note = _collect_products(client, active_only=bool(active))

        if q:
            ql = q.strip().lower()
            items = [r for r in items if ql in (r["name"] or "").lower() or ql in (r["code"] or "").lower()]

        start = (page - 1) * page_size
        end = start + page_size
        page_items = items[start:end]

        return JSONResponse({"items": page_items, "total": len(items), "note": note})

    return router
