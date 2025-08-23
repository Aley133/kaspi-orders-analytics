from __future__ import annotations
from typing import Callable, Optional, List, Dict, Any, Tuple
from fastapi import APIRouter, HTTPException, Query, UploadFile, File
from fastapi.responses import Response, JSONResponse

import io
from xml.etree import ElementTree as ET
try:
    import openpyxl  # optional for .xlsx/.xls
    _OPENPYXL_AVAILABLE = True
except Exception:  # pragma: no cover
    _OPENPYXL_AVAILABLE = False

try:
    from app.kaspi_client import ProductStock, normalize_row  # type: ignore
except Exception:
    try:
        from ..kaspi_client import ProductStock, normalize_row  # type: ignore
    except Exception:
        try:
            from kaspi_client import ProductStock, normalize_row  # type: ignore
        except Exception:
            ProductStock = None
            normalize_row = None  # will fallback

def _parse_xml(content: bytes):
    try:
        root = ET.fromstring(content)
    except ET.ParseError as e:
        raise HTTPException(status_code=400, detail=f"Некорректный XML: {e}")
    rows = []
    offers = root.findall(".//offer") or root.findall(".//item") or root.findall(".//product")
    for off in offers:
        row = {}
        # attributes
        for attr in ("id","available","shop-sku","sku","offerid","code"):
            if off.get(attr) is not None:
                row[attr] = off.get(attr)
        # common child tags
        for tag in ["vendorCode","barcode","price","name","title","model",
                    "quantity","qty","category","vendor","brand"]:
            el = off.find(tag)
            if el is not None and (el.text or "").strip():
                row[tag] = (el.text or "").strip()
        rows.append(row)
    return rows

def _parse_excel(file):
    if not _OPENPYXL_AVAILABLE:
        raise HTTPException(status_code=500, detail="openpyxl не установлен на сервере.")
    try:
        data = file.file.read()
        wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Не удалось открыть Excel: {e}")
    ws = wb.active
    headers = [str(c.value or '').strip() for c in ws[1]]
    rows = []
    for row in ws.iter_rows(min_row=2, values_only=True):
        item = {h: v for h, v in zip(headers, row)}
        if any(v not in (None, "", []) for v in item.values()):
            rows.append(item)
    return rows


try:
    from app.kaspi_client import KaspiClient  # type: ignore
except Exception:  # pragma: no cover
    try:
        from ..kaspi_client import KaspiClient  # type: ignore
    except Exception:
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
        pid = item.get("id") or _pick(attrs, "id", "sku", "code", "offerId") or _pick(attrs, "name")
        if not pid or pid in seen:
            return
        seen.add(pid)
        total += 1

        code = _pick(attrs, "code", "sku", "offerId", "article", "barcode")
        name = _pick(attrs, "name", "title", "productName", "offerName")
        price = _num(_pick(attrs, "price", "basePrice", "salePrice", "currentPrice", "totalPrice"))
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

    # 1) Пытаемся штатный каталог
    try:
        try:
            for it in iter_fn(active_only=bool(active_only) if active_only is not None else True):
                add_row(it)
        except TypeError:
            for it in iter_fn():
                add_row(it)
    except Exception:
        items = []
        total = 0

    # 2) Если пусто — резерв: собираем товары из заказов
    if not items:
        try:
            for it in client.iter_products_from_orders(days=60):
                add_row(it)
            note = "Каталог по API недоступен, показаны товары, собранные из последних заказов (60 дней)."
        except Exception:
            note = "Каталог по API недоступен."
            items, total = [], 0

    return items, total, note


def get_products_router(client: Optional["KaspiClient"]) -> APIRouter:
    router = APIRouter(tags=["products"])

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

    @router.get("/export.csv")
    async def export_products_csv(active: int = Query(1)):
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

    @router.get("/probe")
    async def probe_products(active: int = Query(1)):
        if client is None:
            raise HTTPException(status_code=500, detail="KASPI_TOKEN is not set")
        try:
            res = client.probe_catalog(sample_size=2, active_only=bool(active))
            return JSONResponse({"attempts": res})
        except Exception as e:
            return JSONResponse({"attempts": [], "error": str(e)})

    
    @router.post("/manual-upload")
    async def manual_upload(file: UploadFile = File(...)):
        """Принимает XML или Excel (.xlsx/.xls), нормализует и возвращает JSON для отрисовки таблицы."""
        filename = (file.filename or "").lower()
        content = await file.read()

        if filename.endswith(".xml"):
            raw_rows = _parse_xml(content)
        elif filename.endswith(".xlsx") or filename.endswith(".xls"):
            file.file = io.BytesIO(content)  # reuse buffer
            raw_rows = _parse_excel(file)
        else:
            raise HTTPException(status_code=400, detail="Поддерживаются только XML или Excel (.xlsx/.xls).")

        normalized = []
        for r in raw_rows:
            try:
                ps = normalize_row(r)
                normalized.append(ps.to_dict())
            except Exception:
                normalized.append(r)

        normalized.sort(key=lambda x: (x.get("name") or x.get("Name") or '').lower())
        return JSONResponse({"count": len(normalized), "items": normalized})
return router
