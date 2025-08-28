# app/debug_sku.py
from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Iterable, Tuple, List, Dict, Optional

import pytz
import httpx
from fastapi import APIRouter, Query, HTTPException

# --- HTTPX (все 4 таймаута указаны явно) ---
HTTPX_TIMEOUT = httpx.Timeout(connect=10.0, read=45.0, write=15.0, pool=60.0)
HTTPX_LIMITS  = httpx.Limits(max_connections=20, max_keepalive_connections=10)
HTTPX_KW = dict(timeout=HTTPX_TIMEOUT, limits=HTTPX_LIMITS)

# --- ENV для Kaspi ---
KASPI_TOKEN   = os.getenv("KASPI_TOKEN", "").strip()
KASPI_BASEURL = os.getenv("KASPI_BASE_URL", "https://kaspi.kz/shop/api/v2").rstrip("/")

def _headers() -> Dict[str, str]:
    if not KASPI_TOKEN:
        raise HTTPException(status_code=500, detail="KASPI_TOKEN is not set")
    return {
        "X-Auth-Token": KASPI_TOKEN,
        "Accept": "application/vnd.api+json",
        "Content-Type": "application/vnd.api+json",
    }

# --- Утилиты времени ---
def tzinfo_of(name: str) -> pytz.BaseTzInfo:
    try:
        return pytz.timezone(name)
    except Exception:
        raise HTTPException(status_code=400, detail=f"Bad timezone: {name}")

def parse_date_local(d: str, tz: str) -> datetime:
    z = tzinfo_of(tz)
    y, m, dd = map(int, d.split("-"))
    return z.localize(datetime(y, m, dd, 0, 0, 0, 0))

def iter_chunks(start_dt: datetime, end_dt: datetime, step_days: int) -> Iterable[Tuple[datetime, datetime]]:
    cur = start_dt
    while cur <= end_dt:
        nxt = min(cur + timedelta(days=step_days) - timedelta(milliseconds=1), end_dt)
        yield cur, nxt
        cur = nxt + timedelta(milliseconds=1)

# --- Вспомогательные парсеры ---
def _safe_get(d: dict, k: str):
    return d.get(k) if isinstance(d, dict) else None

def _guess_number(attrs: dict, fallback_id: str) -> str:
    for k in ("code", "orderNumber", "number"):
        v = attrs.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return str(fallback_id)

def extract_ms(attrs: dict, field: str) -> Optional[int]:
    v = attrs.get(field)
    if v is None:
        return None
    try:
        return int(v)
    except Exception:
        try:
            return int(datetime.fromisoformat(str(v).replace("Z", "+00:00")).timestamp() * 1000)
        except Exception:
            return None

# --- candidates для SKU/Title ---
SKU_KEYS   = ("merchantProductCode", "article", "sku", "code", "productCode", "offerId", "vendorCode", "barcode", "skuId", "id")
TITLE_KEYS = ("productName", "name", "title", "itemName", "productTitle", "merchantProductName")

def sku_candidates(d: dict) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for k in SKU_KEYS:
        v = _safe_get(d, k)
        if isinstance(v, (str, int, float)) and str(v).strip():
            out[k] = str(v).strip()
    return out

def title_candidates(entry: dict) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for k in TITLE_KEYS:
        v = _safe_get(entry, k)
        if isinstance(v, str) and v.strip():
            out[k] = v.strip()
    prod = _safe_get(entry, "product")
    if isinstance(prod, dict):
        for k in TITLE_KEYS:
            v = _safe_get(prod, k)
            if isinstance(v, str) and v.strip():
                out[f"product.{k}"] = v.strip()
    return out

# --- index included & relationships helpers ---
def _index_included(included: List[dict]) -> Dict[Tuple[str, str], dict]:
    idx: Dict[Tuple[str, str], dict] = {}
    for it in included or []:
        t = it.get("type"); i = it.get("id")
        if t and i:
            idx[(str(t), str(i))] = it
    return idx

def _rel_id(entry: dict, rel_name: str) -> Tuple[Optional[str], Optional[str]]:
    rel = entry.get("relationships", {}).get(rel_name, {})
    data = rel.get("data")
    if isinstance(data, dict):
        return data.get("type"), data.get("id")
    return None, None

# --- извлечение одной позиции (универсально) ---
def _extract_entry(entry: dict, incl: Dict[Tuple[str, str], dict]) -> Optional[Dict[str, object]]:
    attrs = entry.get("attributes", {}) if "attributes" in entry else entry
    qty   = int(attrs.get("quantity") or attrs.get("qty") or attrs.get("count") or 1)
    price = float(attrs.get("unitPrice") or attrs.get("basePrice") or attrs.get("price") or 0.0)

    # 1) собственные поля
    sku = ""
    for k in SKU_KEYS:
        v = attrs.get(k)
        if isinstance(v, (str, int, float)) and str(v).strip():
            sku = str(v).strip()
            break

    rels = entry.get("relationships", {}) if isinstance(entry, dict) else {}

    # 2) product / masterProduct / merchantProduct из included
    def from_rel(rel_key: str) -> Optional[str]:
        node = (rels.get(rel_key) or {}).get("data")
        if not isinstance(node, dict):
            return None
        t, i = str(node.get("type") or ""), str(node.get("id") or "")
        ref = incl.get((t, i), {})
        a   = ref.get("attributes", {}) or {}

        # для masterproducts часто нужен сам id
        if "master" in t.lower():
            return i or a.get("id") or a.get("code") or a.get("sku") or a.get("productCode")
        # для product — code/sku/productCode/id
        return a.get("code") or a.get("sku") or a.get("productCode") or i

    if not sku:
        sku = from_rel("product") or from_rel("masterProduct") or from_rel("merchantProduct") or ""

    # 3) композит productId_offerId (если есть)
    prod_t, prod_id = _rel_id(entry, "product")
    mp_t, mp_id     = _rel_id(entry, "merchantProduct")
    offer_like = attrs.get("offerId") or attrs.get("merchantProductId") or mp_id
    if (prod_id or mp_id) and offer_like:
        composed = f"{(prod_id or mp_id)}_{offer_like}"
        if not sku or str(offer_like) not in sku:
            sku = composed

    # 4) если цены нет — оценим из total/qty
    if price <= 0:
        total = attrs.get("totalPrice") or attrs.get("price")
        try:
            total = float(total)
            if total and qty:
                price = round(total / max(1, qty), 4)
        except Exception:
            pass

    if not (sku and str(sku).strip()):
        return None
    return {"sku": str(sku), "qty": int(qty), "unit_price": float(price)}

# --- список заказов через HTTPX (без KaspiClient) ---
async def _iter_orders_httpx(start_ms: int, end_ms: int, date_field: str) -> List[dict]:
    headers = _headers()
    out: List[dict] = []
    async with httpx.AsyncClient(base_url=KASPI_BASEURL, **HTTPX_KW) as cli:
        page = 0
        while True:
            params = {
                "page[number]": str(page),
                "page[size]": "100",
                f"filter[{date_field or 'creationDate'}][ge]": str(start_ms),
                f"filter[{date_field or 'creationDate'}][le]": str(end_ms),
            }
            r = await cli.get("/orders", params=params, headers=headers)
            r.raise_for_status()
            j = r.json()
            data = j.get("data", []) or []
            if not data:
                break
            out.extend(data)
            page += 1
    return out

# --- основной «по номеру» ---
async def _fetch_by_order_id(order_id: str) -> Dict[str, object]:
    headers = _headers()
    debug: Dict[str, object] = {}
    rows: List[dict] = []

    async with httpx.AsyncClient(base_url=KASPI_BASEURL, **HTTPX_KW) as cli:
        # S1: сабресурс /orders/{id}/entries?include=product,merchantProduct,masterProduct
        try:
            params = {"page[size]": "200", "include": "product,merchantProduct,masterProduct"}
            r = await cli.get(f"/orders/{order_id}/entries", params=params, headers=headers)
            debug["entries_sub_status"] = r.status_code
            j = r.json() if r.headers.get("content-type","").startswith("application/vnd.api+json") else {}
            data = j.get("data", []) if isinstance(j, dict) else []
            incl = _index_included(j.get("included", []) if isinstance(j, dict) else [])
            for i, e in enumerate(data):
                got = _extract_entry(e, incl)
                if got:
                    ent_attrs = e.get("attributes", {}) or {}
                    titles = title_candidates(ent_attrs)
                    rows.append({
                        "index": i,
                        "title_candidates": titles,
                        "sku_candidates": {"extracted": got["sku"]},
                        "raw": e
                    })
            if rows:
                return {"source": "orders/{id}/entries", "entries": rows, "debug": debug}
        except httpx.HTTPError as e:
            debug["entries_sub_error"] = repr(e)

        # S2: /orders/{id}?include=entries.product
        try:
            params = {"include": "entries.product"}
            r = await cli.get(f"/orders/{order_id}", params=params, headers=headers)
            debug["order_inc_prod_status"] = r.status_code
            j = r.json()
            included = j.get("included", []) if isinstance(j, dict) else []
            idx = _index_included(included)
            irow = 0
            for inc in included:
                if "entry" not in str(inc.get("type","")).lower():
                    continue
                got = _extract_entry(inc, idx)
                if got:
                    ent_attrs = inc.get("attributes", {}) or {}
                    titles = title_candidates(ent_attrs)
                    rows.append({
                        "index": irow,
                        "title_candidates": titles,
                        "sku_candidates": {"extracted": got["sku"]},
                        "raw": inc
                    })
                    irow += 1
            if rows:
                return {"source": "orders?include=entries.product", "entries": rows, "debug": debug}
        except httpx.HTTPError as e:
            debug["order_inc_prod_error"] = repr(e)

        # S3: /orderentries?filter[order.id]=...
        try:
            params = {"filter[order.id]": order_id, "page[size]": "200"}
            r = await cli.get("/orderentries", params=params, headers=headers)
            debug["orderentries_status"] = r.status_code
            j = r.json()
            data = j.get("data", []) if isinstance(j, dict) else []
            irow = 0
            for e in data:
                got = _extract_entry(e, {})
                if got:
                    ent_attrs = e.get("attributes", {}) or {}
                    titles = title_candidates(ent_attrs)
                    rows.append({
                        "index": irow,
                        "title_candidates": titles,
                        "sku_candidates": {"extracted": got["sku"]},
                        "raw": e
                    })
                    irow += 1
            if rows:
                return {"source": "orderentries?filter[order.id]", "entries": rows, "debug": debug}
        except httpx.HTTPError as e:
            debug["orderentries_error"] = repr(e)

    return {"source": "none", "entries": rows, "debug": debug}

# ─────────────────────────────────────────────────────────────────────────────
# ПУБЛИЧНАЯ ТОЧКА: вернуть APIRouter
# ─────────────────────────────────────────────────────────────────────────────
def get_debug_router(client=None, default_tz: str = "Asia/Almaty", chunk_days: int = 7) -> APIRouter:
    """
    Возвращает APIRouter с отладочными ручками:
      GET /debug/order-by-number?number=...&start=YYYY-MM-DD&end=YYYY-MM-DD&tz=...&date_field=...
      GET /debug/sample?start=YYYY-MM-DD&end=YYYY-MM-DD&limit=...
    client не обязателен — запросы к Kaspi идут напрямую через HTTPX.
    """
    router = APIRouter()

    @router.get("/debug/order-by-number")
    async def order_by_number(
        number: str = Query(..., description="Номер заказа из кабинета (code)"),
        start: str = Query(..., description="YYYY-MM-DD"),
        end: str = Query(..., description="YYYY-MM-DD"),
        tz: str = Query(default_tz),
        date_field: str = Query("creationDate")
    ):
        if not number.strip():
            raise HTTPException(400, "number is empty")
        tzinfo = tzinfo_of(tz)
        start_dt = parse_date_local(start, tz)
        end_dt   = parse_date_local(end, tz) + timedelta(days=1) - timedelta(milliseconds=1)

        # ищем заказ по временному окну
        found = []
        for s, e in iter_chunks(start_dt, end_dt, chunk_days):
            s_ms = int(s.astimezone(pytz.UTC).timestamp() * 1000)
            e_ms = int(e.astimezone(pytz.UTC).timestamp() * 1000)
            orders = await _iter_orders_httpx(s_ms, e_ms, date_field)
            for od in orders:
                oid   = od.get("id")
                attrs = od.get("attributes", {}) or {}
                code  = _guess_number(attrs, oid)
                if str(code) != str(number):
                    continue

                got = await _fetch_by_order_id(oid)
                ms  = extract_ms(attrs, date_field if date_field in attrs else "creationDate")
                found.append({
                    "order_id": oid,
                    "number": code,
                    "state": attrs.get("state"),
                    "date_ms": ms,
                    "date_iso": (datetime.fromtimestamp(ms/1000.0, tz=pytz.UTC).astimezone(tzinfo).isoformat() if ms else None),
                    "top_level_sku_candidates": sku_candidates(attrs),
                    "entries_count": len(got.get("entries", [])),
                    "entries": got.get("entries", []),
                    "attrs_keys": sorted(list(attrs.keys())),
                    "attrs_raw": attrs,
                    "entries_api_debug": got.get("debug", {}),
                    "source": got.get("source"),
                })
        return {"ok": True, "items": found}

    @router.get("/debug/sample")
    async def debug_sample(
        start: str = Query(...),
        end: str = Query(...),
        tz: str = Query(default_tz),
        date_field: str = Query("creationDate"),
        limit: int = Query(10, ge=1, le=200)
    ):
        tzinfo = tzinfo_of(tz)
        start_dt = parse_date_local(start, tz)
        end_dt   = parse_date_local(end, tz) + timedelta(days=1) - timedelta(milliseconds=1)

        out: List[dict] = []
        for s, e in iter_chunks(start_dt, end_dt, chunk_days):
            s_ms = int(s.astimezone(pytz.UTC).timestamp() * 1000)
            e_ms = int(e.astimezone(pytz.UTC).timestamp() * 1000)
            orders = await _iter_orders_httpx(s_ms, e_ms, date_field)
            for od in orders:
                oid   = od.get("id")
                attrs = od.get("attributes", {}) or {}
                # попробуем вытянуть только первую позицию (быстро)
                brief = await _fetch_by_order_id(oid)
                first = (brief.get("entries") or [{}])[0] if brief.get("entries") else {}
                out.append({
                    "order_id": oid,
                    "number": _guess_number(attrs, oid),
                    "state": attrs.get("state"),
                    "title_candidates": first.get("title_candidates") or {},
                    "sku_candidates": first.get("sku_candidates") or {},
                })
                if len(out) >= limit:
                    return {"ok": True, "items": out}
        return {"ok": True, "items": out}

    return router
