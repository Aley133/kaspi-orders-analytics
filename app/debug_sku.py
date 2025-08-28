# app/debug_sku.py
from __future__ import annotations

import os
import asyncio
from datetime import datetime, timedelta
from typing import Iterable, Tuple, List, Dict, Optional

import pytz
import httpx
from fastapi import APIRouter, Query, HTTPException

# ─────────────────────────────────────────────────────────────────────────────
# Конфиг httpx: явные таймауты + лимиты, чтобы не ловить ValueError и ReadTimeout
# ─────────────────────────────────────────────────────────────────────────────
HTTPX_TIMEOUT = httpx.Timeout(connect=10.0, read=35.0, write=15.0, pool=60.0)
HTTPX_LIMITS  = httpx.Limits(max_connections=20, max_keepalive_connections=10)
HTTPX_KW      = dict(timeout=HTTPX_TIMEOUT, limits=HTTPX_LIMITS)

# Ретраи
MAX_RETRIES   = int(os.getenv("KASPI_RETRIES", "3"))
BACKOFF_BASE  = float(os.getenv("KASPI_BACKOFF_BASE", "0.6"))  # секунды

# Размер страницы
PAGE_SIZE     = int(os.getenv("KASPI_PAGE_SIZE", "50"))  # 50 → чаще быстрее, чем 100

# Базовые URL: можно задать несколько через запятую (будут пробоваться по очереди)
_primary = os.getenv("KASPI_BASE_URL", "https://kaspi.kz/shop/api/v2").strip()
KASPI_BASEURLS = [
    u.strip().rstrip("/") for u in (
        os.getenv("KASPI_BASE_URLS", _primary).split(",")
    )
    if u.strip()
]

# Токен
KASPI_TOKEN = os.getenv("KASPI_TOKEN", "").strip()

def _headers() -> Dict[str, str]:
    if not KASPI_TOKEN:
        raise HTTPException(status_code=500, detail="KASPI_TOKEN is not set")
    return {
        "X-Auth-Token": KASPI_TOKEN,
        "Accept": "application/vnd.api+json",
        "Content-Type": "application/vnd.api+json",
        "User-Agent": "kaspi-orders-analytics/1.0",
    }

# ─────────────────────────────────────────────────────────────────────────────
# Время и чанки
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

def iter_chunks(start_dt: datetime, end_dt: datetime, step_days: int) -> Iterable[Tuple[datetime, datetime]]:
    cur = start_dt
    while cur <= end_dt:
        nxt = min(cur + timedelta(days=step_days) - timedelta(milliseconds=1), end_dt)
        yield cur, nxt
        cur = nxt + timedelta(milliseconds=1)

# ─────────────────────────────────────────────────────────────────────────────
# Утилиты парсинга
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

# Все мыслимые поля под SKU/артикулы/идентификаторы
SKU_KEYS = (
    "merchantProductCode","article","sku","code","productCode","offerId",
    "vendorCode","barcode","skuId","id","merchantProductId","productId",
    "masterProductId","externalId","kaspiSku","vendorArticle","ean","gtin",
    "upc","merchantSku","offer","offerCode","offerSKU","skuCode",
    "variantSku","variantCode","itemCode"
)
# Поля под названия
TITLE_KEYS = (
    "productName","name","title","itemName","productTitle","merchantProductName",
    "caption","model","shortName","displayName"
)

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

# ─────────────────────────────────────────────────────────────────────────────
# Низкоуровневый GET с ретраями и перебором базовых URL
# ─────────────────────────────────────────────────────────────────────────────
async def _http_get(path: str, params: dict, headers: dict) -> httpx.Response:
    last_err: Optional[Exception] = None
    for attempt in range(1, MAX_RETRIES + 1):
        for base_url in KASPI_BASEURLS:
            try:
                async with httpx.AsyncClient(base_url=base_url, **HTTPX_KW) as cli:
                    r = await cli.get(path, params=params, headers=headers)
                    r.raise_for_status()
                    return r
            except (httpx.ReadTimeout, httpx.ConnectTimeout, httpx.ConnectError, httpx.HTTPError) as e:
                last_err = e
        # экспоненциальный backoff
        await asyncio.sleep(BACKOFF_BASE * (2 ** (attempt - 1)))
    # если все попытки по всем базовым URL провалились
    raise last_err or httpx.RequestError("Unknown network error")

# ─────────────────────────────────────────────────────────────────────────────
# Извлечение позиции заказа
# ─────────────────────────────────────────────────────────────────────────────
def _extract_entry(entry: dict, incl_index: Dict[Tuple[str, str], dict]) -> Optional[Dict[str, object]]:
    attrs = entry.get("attributes", {}) if "attributes" in entry else entry
    qty   = int(attrs.get("quantity") or attrs.get("qty") or attrs.get("count") or 1)
    price = float(attrs.get("unitPrice") or attrs.get("basePrice") or attrs.get("price") or 0.0)

    # 1) SKU в самих атрибутах
    sku = ""
    for k in SKU_KEYS:
        v = attrs.get(k)
        if isinstance(v, (str, int, float)) and str(v).strip():
            sku = str(v).strip(); break

    # 2) Если не нашли — попробовать через relationships
    def from_rel(rel_key: str) -> Optional[str]:
        t, i = _rel_id(entry, rel_key)
        if not t or not i:
            return None
        ref = incl_index.get((str(t), str(i)), {})
        a   = ref.get("attributes", {}) if isinstance(ref, dict) else {}
        if "master" in str(t).lower():
            return i or a.get("id") or a.get("code") or a.get("sku") or a.get("productCode")
        return a.get("code") or a.get("sku") or a.get("productCode") or i

    if not sku:
        sku = from_rel("product") or from_rel("masterProduct") or from_rel("merchantProduct") or ""

    # 3) Композит productId_offerId, если есть
    prod_t, prod_id = _rel_id(entry, "product")
    mp_t, mp_id     = _rel_id(entry, "merchantProduct")
    offer_like = attrs.get("offerId") or attrs.get("merchantProductId") or mp_id
    if (prod_id or mp_id) and offer_like:
        composed = f"{(prod_id or mp_id)}_{offer_like}"
        if not sku or str(offer_like) not in sku:
            sku = composed

    # 4) Если unit_price = 0, попробуем рассчитать из total/qty
    if price <= 0:
        total = attrs.get("totalPrice") or attrs.get("price")
        try:
            total_val = float(total)
            if total_val and qty:
                price = round(total_val / max(1, qty), 4)
        except Exception:
            pass

    if not sku or not str(sku).strip():
        return None
    return {"sku": str(sku).strip(), "qty": int(qty), "unit_price": float(price)}

# ─────────────────────────────────────────────────────────────────────────────
# Получение заказов за период (с корректным синтаксисом фильтра, фолбэком и ретраями)
# ─────────────────────────────────────────────────────────────────────────────
async def _iter_orders_httpx(start_ms: int, end_ms: int, date_field: str) -> List[dict]:
    headers = _headers()
    out: List[dict] = []
    page = 0

    # Сначала — «правильный» синтаксис с resource scope и $ge/$le
    def _params_v2(pg: int) -> dict:
        f = date_field or "creationDate"
        return {
            "page[number]": str(pg),
            "page[size]": str(PAGE_SIZE),
            f"filter[orders][{f}][$ge]": str(start_ms),
            f"filter[orders][{f}][$le]": str(end_ms),
        }

    # Фолбэк — старый синтаксис
    def _params_legacy(pg: int) -> dict:
        f = date_field or "creationDate"
        return {
            "page[number]": str(pg),
            "page[size]": str(PAGE_SIZE),
            f"filter[{f}][ge]": str(start_ms),
            f"filter[{f}][le]": str(end_ms),
        }

    use_legacy = False
    while True:
        params = _params_legacy(page) if use_legacy else _params_v2(page)
        try:
            r = await _http_get("/orders", params=params, headers=headers)
        except httpx.HTTPStatusError as e:
            # если 400/422 — возможно, ваш кабинет ждёт другой синтаксис фильтра
            if e.response.status_code in (400, 422) and not use_legacy:
                use_legacy = True
                # повторим итерацию этой же страницы уже с legacy
                continue
            raise
        except httpx.RequestError as e:
            raise HTTPException(status_code=502, detail=f"Kaspi API (orders) error: {e}") from e

        try:
            j = r.json()
        except Exception:
            j = {}

        data = j.get("data", []) if isinstance(j, dict) else []
        if not data:
            break
        out.extend(data)
        page += 1

    return out

# ─────────────────────────────────────────────────────────────────────────────
# Получение позиций заказа (3 стратегии) + опциональный возврат «raw»
# ─────────────────────────────────────────────────────────────────────────────
async def _fetch_by_order_id(order_id: str, collect_raw: bool = False) -> Dict[str, object]:
    headers = _headers()
    debug_info: Dict[str, object] = {}
    raw_dump: Dict[str, object] = {} if collect_raw else {}
    rows: List[dict] = []

    # S1: /orders/{id}/entries?include=product,merchantProduct,masterProduct
    try:
        params = {"page[size]": "200", "include": "product,merchantProduct,masterProduct"}
        r = await _http_get(f"/orders/{order_id}/entries", params=params, headers=headers)
        debug_info["entries_sub_status"] = r.status_code
        try:
            j = r.json()
        except Exception:
            j = {}
        if collect_raw:
            raw_dump["orders_id_entries"] = j
        data = j.get("data", []) if isinstance(j, dict) else []
        included = _index_included(j.get("included", []) if isinstance(j, dict) else [])
        for i, e in enumerate(data):
            got = _extract_entry(e, included)
            if got:
                titles = title_candidates(e.get("attributes", {}) or {})
                for rel_key in ("product", "merchantProduct", "masterProduct"):
                    t, rel_id = _rel_id(e, rel_key)
                    if t and rel_id:
                        inc = included.get((str(t), str(rel_id)))
                        if inc:
                            a = inc.get("attributes", {}) or {}
                            for k in TITLE_KEYS:
                                v = _safe_get(a, k)
                                if isinstance(v, str) and v.strip():
                                    titles[f"{rel_key}.{k}"] = v.strip()
                rows.append({
                    "index": i,
                    "title_candidates": titles,
                    "sku_candidates": {"extracted": got["sku"]},
                    "raw": e
                })
        if rows:
            return {"source": "orders/{id}/entries", "entries": rows, "debug": debug_info, "raw": raw_dump}
    except httpx.RequestError as e:
        debug_info["entries_sub_error"] = repr(e)

    # S2: /orders/{id}?include=entries.product
    try:
        params = {"include": "entries.product"}
        r = await _http_get(f"/orders/{order_id}", params=params, headers=headers)
        debug_info["order_inc_prod_status"] = r.status_code
        try:
            j = r.json()
        except Exception:
            j = {}
        if collect_raw:
            raw_dump["orders_id_inc_entries_product"] = j
        included = _index_included(j.get("included", []) if isinstance(j, dict) else [])
        irow = 0
        for inc_obj in (j.get("included", []) or []):
            if "entry" not in str(inc_obj.get("type", "")).lower():
                continue
            got = _extract_entry(inc_obj, included)
            if got:
                titles = title_candidates(inc_obj.get("attributes", {}) or {})
                for rel_key in ("product", "merchantProduct", "masterProduct"):
                    t, rel_id = _rel_id(inc_obj, rel_key)
                    if t and rel_id:
                        ref = included.get((str(t), str(rel_id)))
                        if ref:
                            a = ref.get("attributes", {}) or {}
                            for k in TITLE_KEYS:
                                v = _safe_get(a, k)
                                if isinstance(v, str) and v.strip():
                                    titles[f"{rel_key}.{k}"] = v.strip()
                rows.append({
                    "index": irow,
                    "title_candidates": titles,
                    "sku_candidates": {"extracted": got["sku"]},
                    "raw": inc_obj
                })
                irow += 1
        if rows:
            return {"source": "orders?include=entries.product", "entries": rows, "debug": debug_info, "raw": raw_dump}
    except httpx.RequestError as e:
        debug_info["order_inc_prod_error"] = repr(e)

    # S3: /orderentries?filter[order.id]=...
    try:
        params = {"filter[order.id]": order_id, "page[size]": "200"}
        r = await _http_get("/orderentries", params=params, headers=headers)
        debug_info["orderentries_status"] = r.status_code
        try:
            j = r.json()
        except Exception:
            j = {}
        if collect_raw:
            raw_dump["orderentries_by_order"] = j
        data = j.get("data", []) if isinstance(j, dict) else []
        irow = 0
        for e in data:
            got = _extract_entry(e, {})
            if got:
                titles = title_candidates(e.get("attributes", {}) or {})
                rows.append({
                    "index": irow,
                    "title_candidates": titles,
                    "sku_candidates": {"extracted": got["sku"]},
                    "raw": e
                })
                irow += 1
        if rows:
            return {"source": "orderentries?filter[order.id]", "entries": rows, "debug": debug_info, "raw": raw_dump}
    except httpx.RequestError as e:
        debug_info["orderentries_error"] = repr(e)

    return {"source": "none", "entries": rows, "debug": debug_info, "raw": raw_dump}

# ─────────────────────────────────────────────────────────────────────────────
# Публичный роутер
# ─────────────────────────────────────────────────────────────────────────────
def get_debug_router(default_tz: str = "Asia/Almaty", chunk_days: int = 3) -> APIRouter:
    """
    Роуты:
      GET /debug/order-by-number?number=...&start=YYYY-MM-DD&end=YYYY-MM-DD&tz=...&date_field=...&raw=0|1
      GET /debug/sample?start=YYYY-MM-DD&end=YYYY-MM-DD&limit=...
    """
    router = APIRouter()

    @router.get("/debug/order-by-number")
    async def order_by_number(
        number: str = Query(..., description="Номер заказа (code) из кабинета"),
        start: str = Query(..., description="YYYY-MM-DD"),
        end: str   = Query(..., description="YYYY-MM-DD"),
        tz: str = Query(default_tz),
        date_field: str = Query("creationDate"),
        raw: int = Query(0, description="1 — вернуть entries_api_raw")
    ):
        if not number.strip():
            raise HTTPException(status_code=400, detail="number is empty")

        tzinfo = tzinfo_of(tz)
        start_dt = parse_date_local(start, tz)
        end_dt   = parse_date_local(end, tz) + timedelta(days=1) - timedelta(milliseconds=1)

        results: List[dict] = []

        # Попытка прямого поиска по code (если поддерживается вашим кабинетом) — САМЫЙ быстрый путь
        try:
            params = {"page[number]": "0", "page[size]": "1", "filter[code]": number}
            r = await _http_get("/orders", params=params, headers=_headers())
            j = r.json()
            data = j.get("data", []) if isinstance(j, dict) else []
            if data:
                od   = data[0]
                oid  = od.get("id")
                attrs = od.get("attributes", {}) or {}
                entries_data = await _fetch_by_order_id(oid, collect_raw=bool(raw))
                ms = extract_ms(attrs, date_field if date_field in attrs else "creationDate")
                results.append({
                    "order_id": oid,
                    "number": _guess_number(attrs, oid),
                    "state": attrs.get("state"),
                    "date_ms": ms,
                    "date_iso": (datetime.fromtimestamp(ms/1000.0, tz=pytz.UTC).astimezone(tzinfo).isoformat() if ms else None),
                    "top_level_sku_candidates": sku_candidates(attrs),
                    "entries_count": len(entries_data.get("entries", [])),
                    "entries": entries_data.get("entries", []),
                    "attributes_keys": sorted(list(attrs.keys())),
                    "attributes_raw": attrs,
                    "entries_api_debug": entries_data.get("debug", {}),
                    "entries_api_raw": entries_data.get("raw", {}) if raw else {},
                    "source": entries_data.get("source"),
                })
                return {"ok": True, "items": results}
        except httpx.RequestError:
            # если фильтр по code недоступен — тихо продолжаем обычным способом
            pass
        except Exception:
            pass

        # Поиск в диапазоне дат чанками
        for s, e in iter_chunks(start_dt, end_dt, chunk_days):
            s_ms = int(s.astimezone(pytz.UTC).timestamp() * 1000)
            e_ms = int(e.astimezone(pytz.UTC).timestamp() * 1000)
            try:
                orders = await _iter_orders_httpx(s_ms, e_ms, date_field)
            except HTTPException:
                raise
            except Exception as e:
                raise HTTPException(status_code=502, detail=f"Kaspi API (orders) error: {e}") from e

            found_here = False
            for od in orders:
                oid   = od.get("id")
                attrs = od.get("attributes", {}) or {}
                code  = _guess_number(attrs, oid)
                if str(code) != str(number):
                    continue

                try:
                    entries_data = await _fetch_by_order_id(oid, collect_raw=bool(raw))
                except HTTPException:
                    raise
                except Exception as e:
                    raise HTTPException(status_code=502, detail=f"Kaspi API (order entries) error: {e}") from e

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
                    "entries_api_raw": entries_data.get("raw", {}) if raw else {},
                    "source": entries_data.get("source"),
                })
                found_here = True
                break  # рано выходим: номер найден
            if found_here:
                break

        return {"ok": True, "items": results}

    @router.get("/debug/sample")
    async def debug_sample(
        start: str = Query(...),
        end: str   = Query(...),
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
            try:
                orders = await _iter_orders_httpx(s_ms, e_ms, date_field)
            except HTTPException:
                raise
            except Exception as e:
                raise HTTPException(status_code=502, detail=f"Kaspi API (orders) error: {e}") from e

            for od in orders:
                oid   = od.get("id")
                attrs = od.get("attributes", {}) or {}
                try:
                    brief = await _fetch_by_order_id(oid, collect_raw=False)
                except Exception:
                    brief = {"entries": []}
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
