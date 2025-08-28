from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Iterable, Tuple, List, Dict, Optional

import pytz
import httpx
from fastapi import APIRouter, Query, HTTPException

# --- HTTPX configuration (explicit timeouts to avoid ReadTimeout issues) ---
HTTPX_TIMEOUT = httpx.Timeout(connect=10.0, read=45.0, write=15.0, pool=60.0)
HTTPX_LIMITS  = httpx.Limits(max_connections=20, max_keepalive_connections=10)
HTTPX_KW = dict(timeout=HTTPX_TIMEOUT, limits=HTTPX_LIMITS)

# --- Kaspi API environment variables ---
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

# --- Time utilities ---
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
    """Yield (start, end) datetime ranges of length step_days (inclusive) to cover [start_dt, end_dt]."""
    cur = start_dt
    while cur <= end_dt:
        nxt = min(cur + timedelta(days=step_days) - timedelta(milliseconds=1), end_dt)
        yield cur, nxt
        cur = nxt + timedelta(milliseconds=1)

# --- Safe getters and parsers ---
def _safe_get(d: dict, k: str):
    """Safely get key k from d if d is a dict, else return None."""
    return d.get(k) if isinstance(d, dict) else None

def _guess_number(attrs: dict, fallback_id: str) -> str:
    """Guess the order number from known fields or use fallback ID."""
    for k in ("code", "orderNumber", "number"):
        v = attrs.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return str(fallback_id)

def extract_ms(attrs: dict, field: str) -> Optional[int]:
    """Extract a timestamp in milliseconds from an attribute field (could be epoch or ISO)."""
    v = attrs.get(field)
    if v is None:
        return None
    try:
        return int(v)  # already epoch milliseconds
    except Exception:
        try:
            # Convert ISO timestamp to milliseconds
            return int(datetime.fromisoformat(str(v).replace("Z", "+00:00")).timestamp() * 1000)
        except Exception:
            return None

# --- Candidate keys for SKU and Title fields ---
SKU_KEYS   = (
    "merchantProductCode", "article", "sku", "code", 
    "productCode", "offerId", "vendorCode", "barcode", 
    "skuId", "id", "merchantProductId"
)
TITLE_KEYS = (
    "productName", "name", "title", "itemName", 
    "productTitle", "merchantProductName"
)

def sku_candidates(d: dict) -> Dict[str, str]:
    """Collect all possible SKU-related fields from a dictionary."""
    out: Dict[str, str] = {}
    for k in SKU_KEYS:
        v = _safe_get(d, k)
        if isinstance(v, (str, int, float)) and str(v).strip():
            out[k] = str(v).strip()
    return out

def title_candidates(entry: dict) -> Dict[str, str]:
    """Collect all possible title/name fields from an entry's attributes (and nested product if present)."""
    out: Dict[str, str] = {}
    for k in TITLE_KEYS:
        v = _safe_get(entry, k)
        if isinstance(v, str) and v.strip():
            out[k] = v.strip()
    # If the entry has a nested 'product' object with its own fields
    prod = _safe_get(entry, "product")
    if isinstance(prod, dict):
        for k in TITLE_KEYS:
            v = _safe_get(prod, k)
            if isinstance(v, str) and v.strip():
                out[f"product.{k}"] = v.strip()
    return out

def _index_included(included: List[dict]) -> Dict[Tuple[str, str], dict]:
    """Index the included objects by (type, id) for quick lookup."""
    idx: Dict[Tuple[str, str], dict] = {}
    for it in included or []:
        t = it.get("type"); i = it.get("id")
        if t and i:
            idx[(str(t), str(i))] = it
    return idx

def _rel_id(entry: dict, rel_name: str) -> Tuple[Optional[str], Optional[str]]:
    """Get the (type, id) of a relationship if present in entry."""
    rel = entry.get("relationships", {}).get(rel_name, {})
    data = rel.get("data")
    if isinstance(data, dict):
        return data.get("type"), data.get("id")
    return None, None

def _extract_entry(entry: dict, incl_index: Dict[Tuple[str, str], dict]) -> Optional[Dict[str, object]]:
    """
    Extract SKU, quantity, and unit_price from a single order entry.
    Tries multiple fields and relationships to reliably get the SKU.
    """
    attrs = entry.get("attributes", {}) if "attributes" in entry else entry
    qty   = int(attrs.get("quantity") or attrs.get("qty") or attrs.get("count") or 1)
    price = float(attrs.get("unitPrice") or attrs.get("basePrice") or attrs.get("price") or 0.0)
    rels  = entry.get("relationships", {}) if isinstance(entry, dict) else {}

    # 1) Check entry's own attributes for any SKU field
    sku = ""
    for k in SKU_KEYS:
        v = attrs.get(k)
        if isinstance(v, (str, int, float)) and str(v).strip():
            sku = str(v).strip()
            break

    # 2) If not found, check related objects (product, masterProduct, merchantProduct) for SKU or code
    def from_rel(rel_key: str) -> Optional[str]:
        t, i = _rel_id(entry, rel_key)
        if not t or not i:
            return None
        ref = incl_index.get((str(t), str(i)), {})  # included object if available
        a   = ref.get("attributes", {}) if isinstance(ref, dict) else {}
        # If masterProduct, often the ID itself is the code/sku
        if "master" in str(t).lower():
            return i or a.get("id") or a.get("code") or a.get("sku") or a.get("productCode")
        # For product or merchantProduct, prefer explicit code/sku fields
        return a.get("code") or a.get("sku") or a.get("productCode") or i

    if not sku:
        sku = from_rel("product") or from_rel("masterProduct") or from_rel("merchantProduct") or ""

    # 3) As a last resort, if we have both a product (or merchantProduct) ID and an offer-like ID, compose them
    prod_t, prod_id = _rel_id(entry, "product")
    mp_t, mp_id     = _rel_id(entry, "merchantProduct")
    offer_like = attrs.get("offerId") or attrs.get("merchantProductId") or mp_id
    if (prod_id or mp_id) and offer_like:
        composed = f"{(prod_id or mp_id)}_{offer_like}"
        if not sku or str(offer_like) not in sku:
            sku = composed

    # 4) Calculate unit price if only total price is given (to avoid zero unitPrice)
    if price <= 0:
        total = attrs.get("totalPrice") or attrs.get("price")
        try:
            total_val = float(total)
            if total_val and qty:
                price = round(total_val / max(1, qty), 4)
        except Exception:
            pass

    if not sku or not str(sku).strip():
        # If we couldn't extract a SKU at all, skip this entry
        return None

    return {"sku": str(sku).strip(), "qty": qty, "unit_price": price}

# --- Fetch all orders in a date range (with paging) ---
async def _iter_orders_httpx(start_ms: int, end_ms: int, date_field: str) -> List[dict]:
    """Retrieve all orders from Kaspi API between start_ms and end_ms (epoch millis) using HTTPX directly."""
    headers = _headers()
    orders: List[dict] = []
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
            data = r.json().get("data", [])  # list of orders
            if not data:
                break
            orders.extend(data)
            page += 1
    return orders

# --- Fetch order entries by order ID (tries multiple strategies) ---
async def _fetch_by_order_id(order_id: str) -> Dict[str, object]:
    """
    Retrieve entries for a given order ID and extract SKU/title info for each entry.
    Tries different API endpoints for robustness.
    """
    headers = _headers()
    debug_info: Dict[str, object] = {}
    entries_out: List[dict] = []

    async with httpx.AsyncClient(base_url=KASPI_BASEURL, **HTTPX_KW) as cli:
        # Strategy 1: GET /orders/{id}/entries?include=product,merchantProduct,masterProduct
        try:
            params = {"page[size]": "200", "include": "product,merchantProduct,masterProduct"}
            r = await cli.get(f"/orders/{order_id}/entries", params=params, headers=headers)
            debug_info["entries_sub_status"] = r.status_code
            if r.headers.get("content-type", "").startswith("application/vnd.api+json"):
                j = r.json()
            else:
                j = {}
            data_list = j.get("data", []) if isinstance(j, dict) else []
            included = _index_included(j.get("included", [])) if isinstance(j, dict) else {}
            for i, entry in enumerate(data_list):
                extracted = _extract_entry(entry, included)
                if extracted:
                    ent_attrs = entry.get("attributes", {}) or {}
                    titles = title_candidates(ent_attrs)
                    # Include titles from related included objects (product, merchantProduct, masterProduct)
                    for rel_key in ("product", "merchantProduct", "masterProduct"):
                        t, rel_id = _rel_id(entry, rel_key)
                        if t and rel_id:
                            inc_obj = included.get((str(t), str(rel_id)))
                            if inc_obj:
                                inc_attrs = inc_obj.get("attributes", {}) or {}
                                for k in TITLE_KEYS:
                                    v = _safe_get(inc_attrs, k)
                                    if isinstance(v, str) and v.strip():
                                        titles[f"{rel_key}.{k}"] = v.strip()
                    entries_out.append({
                        "index": i,
                        "title_candidates": titles,
                        "sku_candidates": {"extracted": extracted["sku"]},
                        "raw": entry
                    })
            if entries_out:
                return {"source": "orders/{id}/entries", "entries": entries_out, "debug": debug_info}
        except httpx.HTTPError as e:
            debug_info["entries_sub_error"] = repr(e)

        # Strategy 2: GET /orders/{id}?include=entries.product
        try:
            params = {"include": "entries.product"}
            r = await cli.get(f"/orders/{order_id}", params=params, headers=headers)
            debug_info["order_inc_prod_status"] = r.status_code
            j = r.json()
            included = _index_included(j.get("included", [])) if isinstance(j, dict) else {}
            irow = 0
            for inc_obj in (j.get("included", []) or []):
                # We're interested in included items that are order entries
                if "entry" not in str(inc_obj.get("type", "")).lower():
                    continue
                extracted = _extract_entry(inc_obj, included)
                if extracted:
                    ent_attrs = inc_obj.get("attributes", {}) or {}
                    titles = title_candidates(ent_attrs)
                    for rel_key in ("product", "merchantProduct", "masterProduct"):
                        t, rel_id = _rel_id(inc_obj, rel_key)
                        if t and rel_id:
                            ref = included.get((str(t), str(rel_id)))
                            if ref:
                                ref_attrs = ref.get("attributes", {}) or {}
                                for k in TITLE_KEYS:
                                    v = _safe_get(ref_attrs, k)
                                    if isinstance(v, str) and v.strip():
                                        titles[f"{rel_key}.{k}"] = v.strip()
                    entries_out.append({
                        "index": irow,
                        "title_candidates": titles,
                        "sku_candidates": {"extracted": extracted["sku"]},
                        "raw": inc_obj
                    })
                    irow += 1
            if entries_out:
                return {"source": "orders?include=entries.product", "entries": entries_out, "debug": debug_info}
        except httpx.HTTPError as e:
            debug_info["order_inc_prod_error"] = repr(e)

        # Strategy 3: GET /orderentries?filter[order.id]=... (no included data)
        try:
            params = {"filter[order.id]": order_id, "page[size]": "200"}
            r = await cli.get("/orderentries", params=params, headers=headers)
            debug_info["orderentries_status"] = r.status_code
            j = r.json()
            data_list = j.get("data", []) if isinstance(j, dict) else []
            irow = 0
            for entry in data_list:
                extracted = _extract_entry(entry, {})  # no included index available in this call
                if extracted:
                    ent_attrs = entry.get("attributes", {}) or {}
                    titles = title_candidates(ent_attrs)
                    # Without included data, we rely only on direct attributes for titles
                    entries_out.append({
                        "index": irow,
                        "title_candidates": titles,
                        "sku_candidates": {"extracted": extracted["sku"]},
                        "raw": entry
                    })
                    irow += 1
            if entries_out:
                return {"source": "orderentries?filter[order.id]", "entries": entries_out, "debug": debug_info}
        except httpx.HTTPError as e:
            debug_info["orderentries_error"] = repr(e)

    # If no entries were found or extracted by any strategy, return debug info (possibly empty entries list)
    return {"source": "none", "entries": entries_out, "debug": debug_info}

# ─────────────────────────────────────────────────────────────────────────────
# Public debug router: provides endpoints to fetch order details and entries
# ─────────────────────────────────────────────────────────────────────────────
def get_debug_router(client=None, default_tz: str = "Asia/Almaty", chunk_days: int = 3) -> APIRouter:
    """
    Returns an APIRouter with debugging endpoints:
      GET /debug/order-by-number?number=...&start=YYYY-MM-DD&end=YYYY-MM-DD&tz=...&date_field=...
      GET /debug/sample?start=YYYY-MM-DD&end=YYYY-MM-DD&limit=...
    """
    router = APIRouter()

    @router.get("/debug/order-by-number")
    async def order_by_number(
        number: str = Query(..., description="Order number as shown in cabinet (code)"),
        start: str = Query(..., description="Start date (YYYY-MM-DD)"),
        end: str = Query(..., description="End date (YYYY-MM-DD)"),
        tz: str = Query(default_tz, description="Timezone of provided dates"),
        date_field: str = Query("creationDate", description="Order date field to filter on (e.g., creationDate)")
    ):
        if not number.strip():
            raise HTTPException(status_code=400, detail="number is empty")
        tzinfo = tzinfo_of(tz)
        start_dt = parse_date_local(start, tz)
        end_dt   = parse_date_local(end, tz) + timedelta(days=1) - timedelta(milliseconds=1)

        results: List[dict] = []
        # Iterate through the date range in chunks to find the order by its number/code
        for s, e in iter_chunks(start_dt, end_dt, chunk_days):
            s_ms = int(s.astimezone(pytz.UTC).timestamp() * 1000)
            e_ms = int(e.astimezone(pytz.UTC).timestamp() * 1000)
            orders = await _iter_orders_httpx(s_ms, e_ms, date_field)
            for od in orders:
                oid   = od.get("id")
                attrs = od.get("attributes", {}) or {}
                code  = _guess_number(attrs, oid)
                if str(code) != str(number):
                    continue  # not matching the target order number

                # Fetch entries for this order and extract SKU/name info
                entries_data = await _fetch_by_order_id(oid)
                ms  = extract_ms(attrs, date_field if date_field in attrs else "creationDate")
                results.append({
                    "order_id": oid,
                    "number": code,
                    "state": attrs.get("state"),
                    "date_ms": ms,
                    "date_iso": (datetime.fromtimestamp(ms/1000.0, tz=pytz.UTC).astimezone(tzinfo).isoformat() if ms else None),
                    "top_level_sku_candidates": sku_candidates(attrs),
                    "entries_count": len(entries_data.get("entries", [])),
                    "entries": entries_data.get("entries", []),
                    "attributes_keys": sorted(list(attrs.keys())),
                    "attributes_raw": attrs,
                    "entries_api_debug": entries_data.get("debug", {}),
                    "source": entries_data.get("source"),
                })
        return {"ok": True, "items": results}

    @router.get("/debug/sample")
    async def debug_sample(
        start: str = Query(..., description="Start date (YYYY-MM-DD)"),
        end: str = Query(..., description="End date (YYYY-MM-DD)"),
        tz: str = Query(default_tz, description="Timezone of provided dates"),
        date_field: str = Query("creationDate", description="Order date field to filter on"),
        limit: int = Query(10, ge=1, le=200, description="Max number of sample orders to return")
    ):
        """Fetch a sample list of orders (up to `limit`) with basic SKU/name info for the first entry of each."""
        tzinfo = tzinfo_of(tz)
        start_dt = parse_date_local(start, tz)
        end_dt   = parse_date_local(end, tz) + timedelta(days=1) - timedelta(milliseconds=1)

        sample_out: List[dict] = []
        for s, e in iter_chunks(start_dt, end_dt, chunk_days):
            s_ms = int(s.astimezone(pytz.UTC).timestamp() * 1000)
            e_ms = int(e.astimezone(pytz.UTC).timestamp() * 1000)
            orders = await _iter_orders_httpx(s_ms, e_ms, date_field)
            for od in orders:
                oid   = od.get("id")
                attrs = od.get("attributes", {}) or {}
                # Use _guess_number to get human-readable order number or fallback to ID
                order_num = _guess_number(attrs, oid)
                state = attrs.get("state")
                # Quickly fetch the first entry of the order (for performance, not retrieving all entries here)
                brief = await _fetch_by_order_id(oid)
                first_entry = (brief.get("entries") or [{}])[0] if brief.get("entries") else {}
                sample_out.append({
                    "order_id": oid,
                    "number": order_num,
                    "state": state,
                    "title_candidates": first_entry.get("title_candidates") or {},
                    "sku_candidates": first_entry.get("sku_candidates") or {},
                })
                if len(sample_out) >= limit:
                    return {"ok": True, "items": sample_out}
        return {"ok": True, "items": sample_out}

    return router
