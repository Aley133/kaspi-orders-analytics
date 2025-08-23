from __future__ import annotations
from typing import Callable, Optional, List, Dict, Any, Tuple
import os
from io import BytesIO

from fastapi import APIRouter, HTTPException, Query, UploadFile, File
from fastapi.responses import Response, JSONResponse
from openpyxl import load_workbook

from app.kaspi_client import KaspiClient, parse_kaspi_catalog_xml

# Единый роутер под /api/stock
router = APIRouter(prefix="/api/stock", tags=["stock"])

# --- Ручной импорт XML/XLSX ---
EXPECTED_XLSX_HEADERS = {
    "sku", "model", "brand", "stock", "price", "storeid", "cityid"
}

def _normalize_header(h: str) -> str:
    return "".join((h or "").strip().lower().split())

@router.post("/import/xml")
async def import_xml(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".xml"):
        raise HTTPException(status_code=400, detail="Ожидается XML файл из кабинета Kaspi.")
    xml_bytes = await file.read()
    try:
        items = parse_kaspi_catalog_xml(xml_bytes)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return JSONResponse({"count": len(items), "items": items})

@router.post("/import/xlsx")
async def import_xlsx(file: UploadFile = File(...)):
    fname = file.filename.lower()
    if not (fname.endswith(".xlsx") or fname.endswith(".xlsm") or fname.endswith(".xls")):
        raise HTTPException(status_code=400, detail="Ожидается Excel (.xlsx/.xls).")
    data = await file.read()
    try:
        wb = load_workbook(filename=BytesIO(data), data_only=True)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Не удалось прочитать Excel: {e}")
    ws = wb.active
    if ws.max_row < 2 or ws.max_column < 1:
        return {"count": 0, "items": []}
    headers_raw = [str(ws.cell(row=1, column=c).value or "").strip() for c in range(1, ws.max_column + 1)]
    headers_norm = [_normalize_header(h) for h in headers_raw]
    idx = {h: i for i, h in enumerate(headers_norm)}
    def getv(row, key: str):
        i = idx.get(key)
        return (row[i] if i is not None and i < len(row) else None)
    def to_float(x):
        try:
            return float(x) if x is not None and str(x).strip() != "" else None
        except Exception:
            return None
    items: List[Dict[str, Any]] = []
    for r in range(2, ws.max_row + 1):
        row = [ws.cell(row=r, column=c).value for c in range(1, ws.max_column + 1)]
        items.append({
            "sku": (getv(row, "sku") if getv(row, "sku") is not None else None),
            "model": (str(getv(row, "model")).strip() if getv(row, "model") is not None else None),
            "brand": (str(getv(row, "brand")).strip() if getv(row, "brand") is not None else None),
            "stock": to_float(getv(row, "stock")) or 0.0,
            "price": to_float(getv(row, "price")),
            "storeId": (str(getv(row, "storeid")).strip() if getv(row, "storeid") is not None else None),
            "cityId": (str(getv(row, "cityid")).strip() if getv(row, "cityid") is not None else None),
        })
    return JSONResponse({"count": len(items), "items": items})

@router.get("/xlsx/expected-columns")
async def expected_columns():
    return {
        "required_or_recommended": ["sku", "model", "brand", "stock", "price", "storeId", "cityId"],
        "note": "Регистр не важен. Пробелы в заголовках допустимы."
    }

# --- Работа с каталогом через API/фолбэк на заказы ---

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

def _find_iter_fn(client: Any):
    for name in ("iter_products", "iter_offers", "iter_catalog"):
        if hasattr(client, name):
            return getattr(client, name)
    return None

def _collect_products(client: KaspiClient, active_only: Optional[bool]):
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
            "id": pid, "code": code, "name": name, "price": price, "qty": qty,
            "active": True if active_val else False if active_val is False else None,
            "brand": brand, "category": category, "barcode": barcode,
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

    # 2) Если пусто — резерв: соберём из заказов
    if not items:
        try:
            for it in client.iter_products_from_orders(days=60):
                add_row(it)
            note = "Каталог по API недоступен, показаны товары, собранные из последних заказов (60 дней)."
        except Exception:
            note = "Каталог по API недоступен."
            items = []

    return items, len(items), note

# Инициализация клиента (если есть токен)
_KASPI_TOKEN = os.getenv("KASPI_TOKEN")
_CLIENT: Optional[KaspiClient] = KaspiClient(_KASPI_TOKEN) if _KASPI_TOKEN else None

@router.get("/list")
async def list_products(
    active: int = Query(1, description="1 — только активные, 0 — все"),
    q: Optional[str] = Query(None, description="поиск по названию/коду"),
    page: int = Query(1, ge=1),
    page_size: int = Query(500, ge=1, le=2000),
):
    if _CLIENT is None:
        raise HTTPException(status_code=500, detail="KASPI_TOKEN is not set")
    items, _, note = _collect_products(_CLIENT, active_only=bool(active))
    if q:
        ql = q.strip().lower()
        items = [r for r in items if ql in (r["name"] or "").lower() or ql in (r["code"] or "").lower()]
    start = (page - 1) * page_size
    end = start + page_size
    page_items = items[start:end]
    return JSONResponse({"items": page_items, "total": len(items), "note": note})

@router.get("/export.csv")
async def export_products_csv(active: int = Query(1)):
    if _CLIENT is None:
        raise HTTPException(status_code=500, detail="KASPI_TOKEN is not set")
    items, _, _ = _collect_products(_CLIENT, active_only=bool(active))
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
    if _CLIENT is None:
        raise HTTPException(status_code=500, detail="KASPI_TOKEN is not set")
    try:
        res = _CLIENT.probe_catalog(sample_size=2, active_only=bool(active))
        return JSONResponse({"attempts": res})
    except Exception as e:
        return JSONResponse({"attempts": [], "error": str(e)})