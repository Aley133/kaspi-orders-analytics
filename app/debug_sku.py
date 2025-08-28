# app/debug_sku.py
from __future__ import annotations

import os
from datetime import datetime, timedelta, time as dt_time
from typing import Any, Iterable, Tuple, List, Dict, Optional

import pytz
import httpx
from fastapi import APIRouter, Query, HTTPException

# ─────────────────────────────────────────────────────────────────────────────
# HTTPX: таймауты и лимиты (все 4 параметра заданы, чтобы избежать ошибок)
# ─────────────────────────────────────────────────────────────────────────────
HTTPX_TIMEOUT = httpx.Timeout(connect=10.0, read=45.0, write=15.0, pool=60.0)
HTTPX_LIMITS  = httpx.Limits(max_connections=20, max_keepalive_connections=10)
HTTPX_KW = dict(timeout=HTTPX_TIMEOUT, limits=HTTPX_LIMITS)

# ─────────────────────────────────────────────────────────────────────────────
# ENV
# ─────────────────────────────────────────────────────────────────────────────
KASPI_TOKEN   = os.getenv("KASPI_TOKEN", "").strip()
KASPI_BASEURL = os.getenv("KASPI_BASE_URL", "https://kaspi.kz/shop/api/v2").rstrip("/")
# опционально можно перечислить альтернативы через запятую
KASPI_BASEURLS_EXTRA = [u.strip().rstrip("/") for u in (os.getenv("KASPI_BASE_URLS","").split(",") if os.getenv("KASPI_BASE_URLS") else []) if u.strip()]
KASPI_FALLBACKS = ["https://seller-api.kaspi.kz/shop/api/v2"]

def _headers() -> Dict[str, str]:
    if not KASPI_TOKEN:
        raise HTTPException(status_code=500, detail="KASPI_TOKEN is not set")
    return {
        "X-Auth-Token": KASPI_TOKEN,
        "Accept": "application/vnd.api+json",
        "Content-Type": "application/vnd.api+json",
    }

# ─────────────────────────────────────────────────────────────────────────────
# ВРЕМЯ / ОКНА
# ─────────────────────────────────────────────────────────────────────────────
def tzinfo_of(name: str) -> pytz.BaseTzInfo:
    try:
        return pytz.timezone(name)
    except Exception:
        raise HTTPException(status_code=400, detail=f"Bad timezone: {name}")

def parse_date_local(d: str, tz: str) -> datetime:
    z = tzinfo_of(tz)
    y, m, dd = map(int, d.split("-"))
    return z.localize(datetime(y, m, dd, 0, 0, 0, 0))

def parse_hhmm(s: Optional[str]) -> Optional[dt_time]:
    if not s:
        return None
    try:
        hh, mm = map(int, s.split(":"))
        return dt_time(hh, mm, 0, 0)
    except Exception:
        return None

def build_window_ms(
    start: str,
    end: str,
    tz: str,
    start_time: Optional[str] = None,
    end_time: Optional[str]   = None,
) -> Tuple[int, int]:
    """
    Возвращает (start_ms, end_ms) в UTC миллисекундах.
    Если переданы HH:MM — ограничиваем окно по времени суток.
    """
    z = tzinfo_of(tz)
    s0 = parse_date_local(start, tz)
    e0 = parse_date_local(end,   tz)

    st = parse_hhmm(start_time)
    et = parse_hhmm(end_time)

    s_local = s0 if st is None else z.localize(datetime.combine(s0.date(), st))
    e_local = z.localize(datetime.combine(e0.date(), dt_time(23, 59, 59, 999000))) if et is None \
              else z.localize(datetime.combine(e0.date(), et))

    s_ms = int(s_local.astimezone(pytz.UTC).timestamp() * 1000)
    e_ms = int(e_local.astimezone(pytz.UTC).timestamp() * 1000)
    return s_ms, e_ms

# ─────────────────────────────────────────────────────────────────────────────
# ПАРСИНГ ПОЛЕЙ
# ─────────────────────────────────────────────────────────────────────────────
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
        return int(v)  # epoch ms
    except Exception:
        try:
            return int(datetime.fromisoformat(str(v).replace("Z", "+00:00")).timestamp() * 1000)
        except Exception:
            return None

# кандидаты для SKU/Title
SKU_KEYS = (
    "merchantProductCode","article","sku","code",
    "productCode","offerId","vendorCode","barcode",
    "skuId","id","merchantProductId",
)
TITLE_KEYS = ("productName","name","title","itemName","productTitle","merchantProductName")

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

def _extract_entry(entry: dict, incl_index: Dict[Tuple[str, str], dict]) -> Optional[Dict[str, Any]]:
    """
    Универсальный извлекатель позиции: SKU, qty, unit_price + устойчивые хаки.
    """
    attrs = entry.get("attributes", {}) if "attributes" in entry else entry
    qty   = int(attrs.get("quantity") or attrs.get("qty") or attrs.get("count") or 1)
    price = float(attrs.get("unitPrice") or attrs.get("basePrice") or attrs.get("price") or 0.0)

    # 1) пробуем SKU напрямую из атрибутов
    sku = ""
    for k in SKU_KEYS:
        v = attrs.get(k)
        if isinstance(v, (str, int, float)) and str(v).strip():
            sku = str(v).strip()
            break

    # 2) если нет — пытаемся через включённые сущности
    def from_rel(rel_key: str) -> Optional[str]:
        t, i = _rel_id(entry, rel_key)
        if not t or not i:
            return None
        ref = incl_index.get((str(t), str(i)), {}) or {}
        a   = ref.get("attributes", {}) if isinstance(ref, dict) else {}
        if "master" in str(t).lower():
            return i or a.get("id") or a.get("code") or a.get("sku") or a.get("productCode")
        return a.get("code") or a.get("sku") or a.get("productCode") or i

    if not sku:
        sku = from_rel("product") or from_rel("masterProduct") or from_rel("merchantProduct") or ""

    # 3) композит, если есть offer-like ID
    prod_t, prod_id = _rel_id(entry, "product")
    _mp_t, mp_id    = _rel_id(entry, "merchantProduct")
    offer_like = attrs.get("offerId") or attrs.get("merchantProductId") or mp_id
    if (prod_id or mp_id) and offer_like:
        composed = f"{(prod_id or mp_id)}_{offer_like}"
        if not sku or str(offer_like) not in sku:
            sku = composed

    # 4) если unit_price не нашли — оценим из total/qty
    if price <= 0:
        total = attrs.get("totalPrice") or attrs.get("price")
        try:
            total_val = float(total)
            if total_val and qty:
                price = round(total_val / max(1, qty), 4)
        except Exception:
            pass

    if not (sku and str(sku).strip()):
        return None

    return {"sku": str(sku).strip(), "qty": qty, "unit_price": price}

# ─────────────────────────────────────────────────────────────────────────────
# /orders: гибкие фильтры по дате (несколько форм синтаксиса)
# ─────────────────────────────────────────────────────────────────────────────
FILTER_FORMS = (
    lambda f, s, e: {f"filter[orders][{f}][$ge]": str(s), f"filter[orders][{f}][$le]": str(e)},
    lambda f, s, e: {f"filter[{f}][$ge]": str(s),         f"filter[{f}][$le]": str(e)},
    lambda f, s, e: {f"filter[{f}][ge]": str(s),          f"filter[{f}][le]": str(e)},
)

async def _iter_orders_httpx(
    start_ms: int,
    end_ms: int,
    date_field: str,
    page_size: int = 50,
    max_pages: int = 50,
) -> List[dict]:
    """
    Лёгкая пагинация по /orders с гибкими фильтрами.
    Возвращает массив «сырых» orders (элементы JSON:API).
    """
    headers = _headers()
    out: List[dict] = []
    async with httpx.AsyncClient(base_url=KASPI_BASEURL, **HTTPX_KW) as cli:
        page = 0
        ok_form = None  # запомним форму, которая «сработала» на предыдущей странице
        while page < max_pages:
            last_exc: Optional[Exception] = None
            # сначала пробуем известную «успешную» форму, затем остальные
            forms = ( (ok_form,) if ok_form else FILTER_FORMS )
            for make_filter in forms:
                params = {"page[number]": str(page), "page[size]": str(page_size)}
                params.update(make_filter(date_field or "creationDate", start_ms, end_ms))
                try:
                    r = await cli.get("/orders", params=params, headers=headers)
                    r.raise_for_status()
                    j = r.json()
                    data = j.get("data", []) or []
                    if not data:
                        return out
                    out.extend(data)
                    ok_form = make_filter
                    break
                except httpx.HTTPError as e:
                    last_exc = e
                    continue
            else:
                # ни одна форма не прошла на этой странице
                raise HTTPException(status_code=502, detail=f"Kaspi /orders failed: {repr(last_exc)}")
            page += 1
    return out

# ─────────────────────────────────────────────────────────────────────────────
# ПОЛУЧЕНИЕ ПОЗИЦИЙ ЗАКАЗА (3 стратегии)
# ─────────────────────────────────────────────────────────────────────────────
async def _fetch_by_order_id(order_id: str) -> Dict[str, Any]:
    headers = _headers()
    debug_info: Dict[str, Any] = {}
    entries_out: List[dict] = []

    async with httpx.AsyncClient(base_url=KASPI_BASEURL, **HTTPX_KW) as cli:
        # S1: сабресурс с include product/merchantProduct/masterProduct
        try:
            params = {"page[size]": "200", "include": "product,merchantProduct,masterProduct"}
            r = await cli.get(f"/orders/{order_id}/entries", params=params, headers=headers)
            debug_info["entries_sub_status"] = r.status_code
            j = r.json() if r.headers.get("content-type","").startswith("application/vnd.api+json") else {}
            data_list = j.get("data", []) if isinstance(j, dict) else []
            included  = _index_included(j.get("included", [])) if isinstance(j, dict) else {}
            for i, entry in enumerate(data_list):
                ex = _extract_entry(entry, included)
                if not ex:
                    continue
                titles = title_candidates(entry.get("attributes", {}) or {})
                for rel_key in ("product", "merchantProduct", "masterProduct"):
                    t, rel_id = _rel_id(entry, rel_key)
                    if t and rel_id:
                        inc = included.get((str(t), str(rel_id))) or {}
                        inc_attrs = inc.get("attributes", {}) or {}
                        for k in TITLE_KEYS:
                            v = _safe_get(inc_attrs, k)
                            if isinstance(v, str) and v.strip():
                                titles[f"{rel_key}.{k}"] = v.strip()
                entries_out.append({
                    "index": i,
                    "title_candidates": titles,
                    "sku_candidates": {"extracted": ex["sku"]},
                    "raw": entry,
                })
            if entries_out:
                return {"source": "orders/{id}/entries", "entries": entries_out, "debug": debug_info}
        except httpx.HTTPError as e:
            debug_info["entries_sub_error"] = repr(e)

        # S2: order + include=entries.product
        try:
            params = {"include": "entries.product"}
            r = await cli.get(f"/orders/{order_id}", params=params, headers=headers)
            debug_info["order_inc_prod_status"] = r.status_code
            j = r.json()
            included = _index_included(j.get("included", [])) if isinstance(j, dict) else {}
            irow = 0
            for inc_obj in (j.get("included", []) or []):
                if "entry" not in str(inc_obj.get("type","")).lower():
                    continue
                ex = _extract_entry(inc_obj, included)
                if not ex:
                    continue
                titles = title_candidates(inc_obj.get("attributes", {}) or {})
                for rel_key in ("product", "merchantProduct", "masterProduct"):
                    t, rel_id = _rel_id(inc_obj, rel_key)
                    if t and rel_id:
                        ref = included.get((str(t), str(rel_id))) or {}
                        ref_attrs = ref.get("attributes", {}) or {}
                        for k in TITLE_KEYS:
                            v = _safe_get(ref_attrs, k)
                            if isinstance(v, str) and v.strip():
                                titles[f"{rel_key}.{k}"] = v.strip()
                entries_out.append({
                    "index": irow,
                    "title_candidates": titles,
                    "sku_candidates": {"extracted": ex["sku"]},
                    "raw": inc_obj,
                })
                irow += 1
            if entries_out:
                return {"source": "orders?include=entries.product", "entries": entries_out, "debug": debug_info}
        except httpx.HTTPError as e:
            debug_info["order_inc_prod_error"] = repr(e)

        # S3: /orderentries по order.id
        try:
            params = {"filter[order.id]": order_id, "page[size]": "200"}
            r = await cli.get("/orderentries", params=params, headers=headers)
            debug_info["orderentries_status"] = r.status_code
            j = r.json()
            data_list = j.get("data", []) if isinstance(j, dict) else []
            for i, entry in enumerate(data_list):
                ex = _extract_entry(entry, {})
                if not ex:
                    continue
                titles = title_candidates(entry.get("attributes", {}) or {})
                entries_out.append({
                    "index": i,
                    "title_candidates": titles,
                    "sku_candidates": {"extracted": ex["sku"]},
                    "raw": entry,
                })
            if entries_out:
                return {"source": "orderentries?filter[order.id]", "entries": entries_out, "debug": debug_info}
        except httpx.HTTPError as e:
            debug_info["orderentries_error"] = repr(e)

    return {"source": "none", "entries": entries_out, "debug": debug_info}

# ─────────────────────────────────────────────────────────────────────────────
# ПРОВЕРКА ПРАВ: лёгкие запросы + альтернативные хосты
# ─────────────────────────────────────────────────────────────────────────────
def _all_bases() -> List[str]:
    seen, out = set(), []
    for u in [KASPI_BASEURL, *KASPI_BASEURLS_EXTRA, *KASPI_FALLBACKS]:
        if u and u not in seen:
            seen.add(u); out.append(u)
    return out

async def _probe_on_base(
    base: str,
    s_ms: int,
    e_ms: int,
    date_field: str,
    order_id_hint: Optional[str],
) -> Dict[str, Any]:
    result: Dict[str, Any] = {"base": base, "orders": {}, "orderentries": {}, "entries_product": {}}
    tiny_timeout = httpx.Timeout(connect=5.0, read=10.0, write=5.0, pool=10.0)
    tiny_limits  = httpx.Limits(max_connections=5, max_keepalive_connections=2)
    async with httpx.AsyncClient(base_url=base, timeout=tiny_timeout, limits=tiny_limits) as cli:
        # /orders — 1 элемент, узкое окно, корректный синтаксис
        params_orders = {
            "page[number]": "0",
            "page[size]": "1",
            "sort": f"-{date_field or 'creationDate'}",
            f"filter[orders][{date_field or 'creationDate'}][$ge]": str(s_ms),
            f"filter[orders][{date_field or 'creationDate'}][$le]": str(e_ms),
        }
        order_id: Optional[str] = order_id_hint
        try:
            r = await cli.get("/orders", params=params_orders, headers=_headers())
            result["orders"]["status"] = r.status_code
            r.raise_for_status()
            j = r.json() if r.headers.get("content-type","").startswith("application/vnd.api+json") else {}
            data = j.get("data", []) if isinstance(j, dict) else []
            result["orders"]["ok"] = True
            result["orders"]["count"] = len(data)
            if (not order_id) and data:
                order_id = str(data[0].get("id"))
        except httpx.ReadTimeout:
            result["orders"] = {"ok": False, "error": "timeout", "status": None}
        except httpx.HTTPStatusError as e:
            result["orders"] = {"ok": False, "error": f"http {e.response.status_code}", "status": e.response.status_code}
        except httpx.RequestError as e:
            result["orders"] = {"ok": False, "error": f"network {type(e).__name__}"}

        if not order_id:
            return result

        # /orderentries?filter[order.id]
        try:
            r = await cli.get("/orderentries", params={"filter[order.id]": order_id, "page[size]": "1"}, headers=_headers())
            result["orderentries"]["status"] = r.status_code
            r.raise_for_status()
            j = r.json() if r.headers.get("content-type","").startswith("application/vnd.api+json") else {}
            data = j.get("data", []) if isinstance(j, dict) else []
            result["orderentries"]["ok"] = True
            result["orderentries"]["count"] = len(data)
        except httpx.ReadTimeout:
            result["orderentries"] = {"ok": False, "error": "timeout", "status": None}
        except httpx.HTTPStatusError as e:
            result["orderentries"] = {"ok": False, "error": f"http {e.response.status_code}", "status": e.response.status_code}
        except httpx.RequestError as e:
            result["orderentries"] = {"ok": False, "error": f"network {type(e).__name__}"}

        # /orders/{id}?include=entries.product
        try:
            r = await cli.get(f"/orders/{order_id}", params={"include": "entries.product"}, headers=_headers())
            result["entries_product"]["status"] = r.status_code
            r.raise_for_status()
            j = r.json() if r.headers.get("content-type","").startswith("application/vnd.api+json") else {}
            inc = j.get("included", []) if isinstance(j, dict) else []
            result["entries_product"]["ok"] = True
            result["entries_product"]["included"] = len(inc)
        except httpx.ReadTimeout:
            result["entries_product"] = {"ok": False, "error": "timeout", "status": None}
        except httpx.HTTPStatusError as e:
            result["entries_product"] = {"ok": False, "error": f"http {e.response.status_code}", "status": e.response.status_code}
        except httpx.RequestError as e:
            result["entries_product"] = {"ok": False, "error": f"network {type(e).__name__}"}

    return result

# ─────────────────────────────────────────────────────────────────────────────
# РОУТЕР
# ─────────────────────────────────────────────────────────────────────────────
def get_debug_router(default_tz: str = "Asia/Almaty", chunk_days: int = 3) -> APIRouter:
    """
    Роуты:
      GET /debug/order-by-number?number=...&start=YYYY-MM-DD&end=YYYY-MM-DD&start_time=HH:MM&end_time=HH:MM&tz=...&date_field=...
      GET /debug/sample?start=YYYY-MM-DD&end=YYYY-MM-DD&start_time=HH:MM&end_time=HH:MM&tz=...&date_field=...&limit=10
      GET /debug/perm-check?order_id=...&start=YYYY-MM-DD&end=YYYY-MM-DD&start_time=HH:MM&end_time=HH:MM&tz=...&date_field=...
    """
    router = APIRouter()

    @router.get("/debug/order-by-number")
    async def order_by_number(
        number: str = Query(..., description="Номер заказа (code)"),
        start: str = Query(..., description="YYYY-MM-DD"),
        end: str   = Query(..., description="YYYY-MM-DD"),
        tz: str = Query(default_tz),
        date_field: str = Query("creationDate"),
        start_time: Optional[str] = Query(None, description="HH:MM"),
        end_time:   Optional[str] = Query(None, description="HH:MM"),
    ):
        if not number.strip():
            raise HTTPException(status_code=400, detail="number is empty")

        tzinfo = tzinfo_of(tz)
        s_ms, e_ms = build_window_ms(start, end, tz, start_time, end_time)

        results: List[dict] = []
        # идём кусками (по chunk_days), чтобы не ловить тяжёлые запросы
        s_local = datetime.fromtimestamp(s_ms/1000.0, tz=pytz.UTC).astimezone(tzinfo)
        e_local = datetime.fromtimestamp(e_ms/1000.0, tz=pytz.UTC).astimezone(tzinfo)
        cur_s = s_local
        while cur_s <= e_local:
            cur_e = min(cur_s + timedelta(days=chunk_days) - timedelta(milliseconds=1), e_local)
            cs_ms = int(cur_s.astimezone(pytz.UTC).timestamp() * 1000)
            ce_ms = int(cur_e.astimezone(pytz.UTC).timestamp() * 1000)

            try:
                orders = await _iter_orders_httpx(cs_ms, ce_ms, date_field)
            except HTTPException as e:
                # пробрасываем понятную ошибку вверх
                raise e
            except httpx.RequestError as e:
                raise HTTPException(status_code=502, detail=f"Network: {e!r}")

            for od in orders:
                oid   = od.get("id")
                attrs = od.get("attributes", {}) or {}
                code  = _guess_number(attrs, oid)
                if str(code) != str(number):
                    continue

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

            cur_s = cur_e + timedelta(milliseconds=1)

        return {"ok": True, "items": results}

    @router.get("/debug/sample")
    async def debug_sample(
        start: str = Query(..., description="YYYY-MM-DD"),
        end: str   = Query(..., description="YYYY-MM-DD"),
        tz: str = Query(default_tz),
        date_field: str = Query("creationDate"),
        start_time: Optional[str] = Query(None, description="HH:MM"),
        end_time:   Optional[str] = Query(None, description="HH:MM"),
        limit: int = Query(10, ge=1, le=200),
    ):
        tzinfo = tzinfo_of(tz)
        s_ms, e_ms = build_window_ms(start, end, tz, start_time, end_time)

        out: List[dict] = []
        s_local = datetime.fromtimestamp(s_ms/1000.0, tz=pytz.UTC).astimezone(tzinfo)
        e_local = datetime.fromtimestamp(e_ms/1000.0, tz=pytz.UTC).astimezone(tzinfo)
        cur_s = s_local
        while cur_s <= e_local and len(out) < limit:
            cur_e = min(cur_s + timedelta(days=chunk_days) - timedelta(milliseconds=1), e_local)
            cs_ms = int(cur_s.astimezone(pytz.UTC).timestamp() * 1000)
            ce_ms = int(cur_e.astimezone(pytz.UTC).timestamp() * 1000)

            try:
                orders = await _iter_orders_httpx(cs_ms, ce_ms, date_field)
            except HTTPException as e:
                raise e
            except httpx.RequestError as e:
                raise HTTPException(status_code=502, detail=f"Network: {e!r}")

            for od in orders:
                oid   = od.get("id")
                attrs = od.get("attributes", {}) or {}
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
                    break

            cur_s = cur_e + timedelta(milliseconds=1)

        return {"ok": True, "items": out}

    @router.get("/debug/perm-check")
    async def perm_check(
        order_id: Optional[str] = Query(None, description="Опционально: известный order_id"),
        start: Optional[str] = Query(None, description="YYYY-MM-DD (если пропустить — последние 2 часа)"),
        end:   Optional[str] = Query(None, description="YYYY-MM-DD"),
        start_time: Optional[str] = Query(None, description="HH:MM"),
        end_time:   Optional[str] = Query(None, description="HH:MM"),
        tz: str = Query(default_tz),
        date_field: str = Query("creationDate"),
    ):
        # маленькое окно по умолчанию — «последние 2 часа»
        if start and end:
            s_ms, e_ms = build_window_ms(start, end, tz, start_time, end_time)
        else:
            z = tzinfo_of(tz)
            now = datetime.now(z)
            s = now - timedelta(hours=2)
            s_ms = int(s.astimezone(pytz.UTC).timestamp()*1000)
            e_ms = int(now.astimezone(pytz.UTC).timestamp()*1000)

        checks_per_host: List[Dict[str, Any]] = []
        for base in _all_bases():
            tiny_timeout = httpx.Timeout(connect=5.0, read=10.0, write=5.0, pool=10.0)
            tiny_limits  = httpx.Limits(max_connections=5, max_keepalive_connections=2)
            async with httpx.AsyncClient(base_url=base, timeout=tiny_timeout, limits=tiny_limits) as cli:
                one: Dict[str, Any] = {"base": base}
                # /orders
                try:
                    r = await cli.get("/orders", params={
                        "page[number]": "0",
                        "page[size]": "1",
                        "sort": f"-{date_field or 'creationDate'}",
                        f"filter[orders][{date_field or 'creationDate'}][$ge]": str(s_ms),
                        f"filter[orders][{date_field or 'creationDate'}][$le]": str(e_ms),
                    }, headers=_headers())
                    one["orders_status"] = r.status_code
                    r.raise_for_status()
                    j = r.json() if r.headers.get("content-type","").startswith("application/vnd.api+json") else {}
                    data = j.get("data", []) if isinstance(j, dict) else []
                    one["orders_ok"] = True
                    one["orders_count"] = len(data)
                    if (not order_id) and data:
                        order_id = str(data[0].get("id"))
                except httpx.ReadTimeout:
                    one["orders_error"] = "timeout"
                except httpx.HTTPStatusError as e:
                    one["orders_error"] = f"http {e.response.status_code}"
                except httpx.RequestError as e:
                    one["orders_error"] = f"network {type(e).__name__}"

                if order_id:
                    # /orderentries
                    try:
                        r = await cli.get("/orderentries", params={"filter[order.id]": order_id, "page[size]":"1"}, headers=_headers())
                        one["orderentries_status"] = r.status_code
                        r.raise_for_status()
                        j = r.json() if r.headers.get("content-type","").startswith("application/vnd.api+json") else {}
                        data = j.get("data", []) if isinstance(j, dict) else []
                        one["orderentries_ok"] = True
                        one["orderentries_count"] = len(data)
                    except httpx.ReadTimeout:
                        one["orderentries_error"] = "timeout"
                    except httpx.HTTPStatusError as e:
                        one["orderentries_error"] = f"http {e.response.status_code}"
                    except httpx.RequestError as e:
                        one["orderentries_error"] = f"network {type(e).__name__}"

                    # include=entries.product
                    try:
                        r = await cli.get(f"/orders/{order_id}", params={"include":"entries.product"}, headers=_headers())
                        one["entries_product_status"] = r.status_code
                        r.raise_for_status()
                        j = r.json() if r.headers.get("content-type","").startswith("application/vnd.api+json") else {}
                        inc = j.get("included", []) if isinstance(j, dict) else []
                        one["entries_product_ok"] = True
                        one["entries_product_included"] = len(inc)
                    except httpx.ReadTimeout:
                        one["entries_product_error"] = "timeout"
                    except httpx.HTTPStatusError as e:
                        one["entries_product_error"] = f"http {e.response.status_code}"
                    except httpx.RequestError as e:
                        one["entries_product_error"] = f"network {type(e).__name__}"

                checks_per_host.append(one)

        summary = {
            "window_ms": {"start": s_ms, "end": e_ms},
            "date_field": date_field,
            "hosts": checks_per_host,
        }
        return {"ok": True, "checks": summary}

    return router
