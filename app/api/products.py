from __future__ import annotations

import io
import json
import os
from typing import List, Dict, Any, Optional
from fastapi import APIRouter, UploadFile, File, HTTPException, Query
from pydantic import BaseModel

# Optional: the main app will pass an instance of KaspiClient, but we keep
# this router self-sufficient for the local price-list workflow.
# We intentionally do NOT import openpyxl at module import to avoid hard deps.

# ---------------- Local store ----------------

class LocalStore:
    def __init__(self, path: str = None):
        root_dir = os.path.dirname(os.path.dirname(__file__))  # project root (where main.py is)
        default_path = os.path.join(root_dir, "data", "products_local.json")
        self.path = path or os.getenv("LOCAL_STORE_FILE", default_path)
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        self._data: Dict[str, Dict[str, Any]] = {}
        self._load()

    def _load(self) -> None:
        try:
            if os.path.exists(self.path):
                with open(self.path, "r", encoding="utf-8") as f:
                    self._data = json.load(f)
            else:
                self._data = {}
        except Exception as e:
            # Corrupted file -> start fresh
            self._data = {}

    def _save(self) -> None:
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self._data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, self.path)

    def list(self) -> List[Dict[str, Any]]:
        return list(self._data.values())

    def upsert_many(self, items: List[Dict[str, Any]]) -> int:
        count = 0
        for it in items:
            sku = str(it.get("sku") or "").strip()
            if not sku:
                continue
            prev = self._data.get(sku, {})
            # Preserve existing purchase_price if not provided in the file
            purchase_price = it.get("purchase_price", prev.get("purchase_price"))
            merged = {
                "sku": sku,
                "name": it.get("name") or prev.get("name") or "",
                "price": _num(it.get("price") if it.get("price") is not None else prev.get("price")),
                "currency": it.get("currency") or prev.get("currency") or os.getenv("CURRENCY", "KZT"),
                "barcode": it.get("barcode") or prev.get("barcode") or "",
                "purchase_price": _num(purchase_price),
            }
            merged["net"] = _calc_net(merged.get("price"), merged.get("purchase_price"))
            self._data[sku] = merged
            count += 1
        self._save()
        return count

    def update_purchase_prices(self, updates: List[Dict[str, Any]]) -> int:
        changed = 0
        for it in updates:
            sku = str(it.get("sku") or "").strip()
            if not sku or sku not in self._data:
                # create if not exists (so user can add manually)
                self._data.setdefault(sku, {"sku": sku})
            price = _num(it.get("purchase_price"))
            self._data[sku]["purchase_price"] = price
            # Recompute net
            self._data[sku]["net"] = _calc_net(self._data[sku].get("price"), price)
            changed += 1
        self._save()
        return changed


def _num(x) -> Optional[float]:
    try:
        if x is None or x == "":
            return None
        return float(str(x).replace(" ", "").replace(",", "."))
    except Exception:
        return None


def _calc_net(sale: Optional[float], purchase: Optional[float]) -> Optional[float]:
    if sale is None or purchase is None:
        return None
    return round(sale - purchase, 2)


# ---------------- Parsing helpers ----------------

# Header synonyms (lowercased)
HEADERS = {
    "sku": {"sku", "vendorcode", "merchantsku", "article", "артикул", "код", "код товара", "merchantproductcode"},
    "name": {"name", "название", "наименование", "productname", "title"},
    "price": {"price", "цена", "цена продажи", "sellingprice", "saleprice"},
    "purchase_price": {"purchase", "purchaseprice", "закуп", "закупка", "закупочная цена"},
    "currency": {"currency", "валюта"},
    "barcode": {"barcode", "штрихкод", "ean", "ean13"},
}

def _norm_header(h: str) -> str:
    return "".join(ch for ch in h.strip().lower() if ch.isalnum() or ch in ("_",))

def _pick_key(hdr_map: Dict[str, int], want: str) -> Optional[int]:
    candidates = HEADERS[want]
    for h, idx in hdr_map.items():
        if h in candidates:
            return idx
    return None

def parse_xlsx(content: bytes) -> List[Dict[str, Any]]:
    try:
        import openpyxl  # type: ignore
    except Exception:
        raise HTTPException(status_code=400, detail="Для XLSX установите пакет openpyxl")
    wb = openpyxl.load_workbook(io.BytesIO(content), read_only=True, data_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    if not rows:
        return []
    headers = rows[0]
    hdr_map: Dict[str, int] = {}
    for i, h in enumerate(headers):
        if not h:
            continue
        hdr_map[_norm_header(str(h))] = i
    idx_sku = _pick_key(hdr_map, "sku")
    if idx_sku is None:
        raise HTTPException(status_code=400, detail="В XLSX не найден столбец Артикул / SKU")
    idx_name = _pick_key(hdr_map, "name")
    idx_price = _pick_key(hdr_map, "price")
    idx_pp = _pick_key(hdr_map, "purchase_price")
    idx_cur = _pick_key(hdr_map, "currency")
    idx_bar = _pick_key(hdr_map, "barcode")
    out: List[Dict[str, Any]] = []
    for r in rows[1:]:
        if r is None:
            continue
        sku = r[idx_sku] if idx_sku is not None else None
        if sku is None or str(sku).strip() == "":
            continue
        item = {
            "sku": str(sku).strip(),
            "name": str(r[idx_name]).strip() if idx_name is not None and r[idx_name] is not None else "",
            "price": _num(r[idx_price]) if idx_price is not None else None,
            "purchase_price": _num(r[idx_pp]) if idx_pp is not None else None,
            "currency": str(r[idx_cur]).strip() if idx_cur is not None and r[idx_cur] is not None else None,
            "barcode": str(r[idx_bar]).strip() if idx_bar is not None and r[idx_bar] is not None else None,
        }
        out.append(item)
    return out

def parse_xml(content: bytes) -> List[Dict[str, Any]]:
    # YML (Kaspi/YML) style
    import xml.etree.ElementTree as ET
    try:
        root = ET.fromstring(content)
    except ET.ParseError:
        raise HTTPException(status_code=400, detail="Некорректный XML")
    offers = []
    # possible paths: yml_catalog/shop/offers/offer OR pricelist/products/product etc.
    # We'll search generically by tag name "offer" or "product".
    for offer in root.iter():
        tag = offer.tag.lower().split("}")[-1]
        if tag not in ("offer", "product"):
            continue
        # prefer vendorCode/merchantSku as SKU
        sku = None
        for key in ("vendorCode", "merchantSku", "sku", "code", "article"):
            el = offer.find(key) or offer.find(key.lower())
            if el is not None and (el.text or "").strip():
                sku = el.text.strip()
                break
        if sku is None:
            # some files keep vendorCode as attribute
            for attr in ("vendorCode", "merchantSku", "sku", "code"):
                if offer.get(attr):
                    sku = offer.get(attr)
                    break
        if not sku:
            continue
        name_el = offer.find("name") or offer.find("Name")
        price_el = offer.find("price") or offer.find("Price")
        curr_el = offer.find("currencyId") or offer.find("currency") or offer.find("Currency")
        barcode_el = offer.find("barcode") or offer.find("Barcode") or offer.find("ean")
        # some vendors put purchase price as custom param
        pp = None
        for ptag in ("purchasePrice", "purchase", "zakup", "Закупка", "закупка"):
            el = offer.find(ptag) or offer.find(ptag.lower())
            if el is not None and (el.text or "").strip():
                pp = el.text.strip()
                break
        items = {
            "sku": sku,
            "name": (name_el.text if name_el is not None else "") or "",
            "price": _num(price_el.text) if price_el is not None else None,
            "purchase_price": _num(pp) if pp is not None else None,
            "currency": (curr_el.text if curr_el is not None else None),
            "barcode": (barcode_el.text if barcode_el is not None else None),
        }
        offers.append(items)
    return offers


# ---------------- API router ----------------

class PriceUpdate(BaseModel):
    sku: str
    purchase_price: Optional[float] = None

class PriceUpdateBulk(BaseModel):
    updates: List[PriceUpdate]


def get_products_router(_client=None) -> APIRouter:
    store = LocalStore()
    router = APIRouter()

    @router.post("/products/import")
    async def import_pricelist(file: UploadFile = File(...)):
        name = (file.filename or "").lower()
        content = await file.read()
        if not content:
            raise HTTPException(status_code=400, detail="Пустой файл")
        if name.endswith(".xlsx") or name.endswith(".xls"):
            items = parse_xlsx(content)
        elif name.endswith(".xml"):
            items = parse_xml(content)
        else:
            raise HTTPException(status_code=400, detail="Поддерживаются только XLSX и XML")
        count = store.upsert_many(items)
        return {"ok": True, "count": count, "items": store.list()}

    @router.get("/products/list")
    async def list_products(
        page: int = Query(1, ge=1),
        page_size: int = Query(2000, ge=1, le=10000),
        active: int = Query(None)
    ):
        items = store.list()
        # simple paging
        start = (page - 1) * page_size
        end = start + page_size
        return {
            "total": len(items),
            "page": page,
            "page_size": page_size,
            "items": items[start:end],
        }

    @router.post("/products/purchase-price")
    async def set_purchase_prices(body: PriceUpdateBulk):
        changed = store.update_purchase_prices([u.dict() for u in body.updates])
        return {"ok": True, "changed": changed, "items": store.list()}

    @router.get("/products/debug")
    async def debug_dump():
        return {"path": store.path, "count": len(store.list())}

    return router
