# app/debug_catalog.py
from __future__ import annotations
import os, asyncio, random
from typing import Optional, Dict, Any, List, Tuple
from fastapi import APIRouter, HTTPException, Query, Header
import httpx

# ───────────────────────── helpers: ENV / headers ─────────────────────────

def _env(key: str, default: str = "") -> str:
    v = os.getenv(key, default) or ""
    return v.strip().strip('"').strip("'")

def _list_base_urls() -> List[str]:
    raw = _env("KASPI_BASE_URLS", "") or _env("KASPI_BASE_URL", "")
    if not raw:
        raw = "https://kaspi.kz/shop/api/v2,https://kaspi.kz/merchantcabinet/api/v2,https://kaspi.kz/mc/api/v2"
    return [u.strip().rstrip("/") for u in raw.split(",") if u.strip()]

def _list_catalog_endpoints(merchant_id: str = "") -> List[str]:
    # порядок важен: сначала явные энтрипоинты из «Управления товарами», затем более общие
    default = [
        "merchants/{mid}/product-cards",
        "merchants/{mid}/offers",
        "catalog/offers",
        "offers",
    ]
    raw = _env("KASPI_PRODUCTS_ENDPOINTS", ",".join(default))
    lst = [p.strip("/") for p in raw.split(",") if p.strip()]
    out = []
    for p in lst:
        if "{mid}" in p or "{merchant_id}" in p:
            if merchant_id:
                p = p.replace("{mid}", merchant_id).replace("{merchant_id}", merchant_id)
            else:
                continue
        out.append(p.strip("/"))
    return out

def _kaspi_headers(token: str, jsonapi: bool = True) -> Dict[str, str]:
    h = {
        "X-Auth-Token": token,
        "User-Agent": "Mozilla/5.0",
    }
    if jsonapi:
        h["Accept"] = "application/vnd.api+json"
        h["Content-Type"] = "application/vnd.api+json"
    return h

# ───────────────────────── http / retries ─────────────────────────

_LIMITS = httpx.Limits(max_connections=10, max_keepalive_connections=5)
def _timeout(scale: float = 1.0) -> httpx.Timeout:
    scale = max(1.0, float(scale))
    return httpx.Timeout(connect=10.0, read=min(80.0*scale, 240.0), write=20.0, pool=60.0)

async def _get_json(cli: httpx.AsyncClient, url: str, params: Dict[str, Any], headers: Dict[str, str], attempts: int = 5) -> Dict[str, Any]:
    back = 0.6
    for i in range(attempts):
        try:
            r = await cli.get(url, params=params, headers=headers)
            if r.status_code in (429, 500, 502, 503, 504):
                # почтим Retry-After
                ra = r.headers.get("Retry-After")
                if ra:
                    try: await asyncio.sleep(float(ra))
                    except: pass
                raise httpx.HTTPStatusError(f"retryable {r.status_code}", request=r.request, response=r)
            r.raise_for_status()
            return r.json()
        except (httpx.RequestError, httpx.HTTPStatusError):
            if i == attempts - 1:
                raise
            await asyncio.sleep(min(back, 8.0) + random.uniform(0.0, 0.3))
            back *= 1.8
    return {}

# ───────────────────────── probing endpoints ─────────────────────────

async def _probe_first_working(token: str, merchant_id: str = "", city_id: str = "") -> Tuple[str, str, Dict[str, Any]]:
    """
    Возвращает (base_url, path, first_page_json) для первого успешно отвечающего эндпоинта каталога.
    """
    bases = _list_base_urls()
    paths = _list_catalog_endpoints(merchant_id)
    params_template = [
        # разные поставщики по-разному интерпретируют фильтры; пробуем «универсальные» параметры
        {"page[size]": "1", "active": "true", "archived": "false"},  # продуктовые карты/офферы
        {"page[size]": "1", "visible": "true"},
        {"page[size]": "1"},
    ]
    last_err = None
    for base in bases:
        async with httpx.AsyncClient(base_url=base, timeout=_timeout(), limits=_LIMITS) as cli:
            for path in paths:
                for p in params_template:
                    params = dict(p)
                    if city_id:
                        params.setdefault("cityId", city_id)
                    try:
                        js = await _get_json(cli, f"/{path}", params=params, headers=_kaspi_headers(token))
                        # JSON:API обычно имеет поле "data"
                        if isinstance(js, dict) and ("data" in js or "items" in js or "content" in js):
                            return base, path, js
                    except Exception as e:
                        last_err = e
                        continue
    raise HTTPException(502, detail=f"Не найден рабочий эндпоинт каталога (последняя ошибка: {last_err})")

# ───────────────────────── normalize item ─────────────────────────

def _pick(d: Dict[str, Any], keys: List[str], default=None):
    for k in keys:
        v = d.get(k)
        if isinstance(v, (str, int, float)) and (str(v).strip() != "" or (isinstance(v, (int, float)) and v is not None)):
            return v
    return default

def _normalize_item(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Приводим разные схемы (offers/product-cards) к единому виду:
    { sku, name, price, qty, brand, category, barcode, raw_attributes }
    """
    data = raw.get("data") if "data" in raw else raw
    if isinstance(data, dict) and "attributes" in data:
        attrs = data.get("attributes") or {}
    elif isinstance(raw, dict) and "attributes" in raw:
        attrs = raw.get("attributes") or {}
    else:
        attrs = raw

    name = _pick(attrs, ["title", "name", "productName", "shortName"], "")
    sku  = str(_pick(attrs, ["code", "sku", "offerCode", "merchantCode", "article"], "") or "")
    price = float(_pick(attrs, ["price", "minPrice", "basePrice", "sellingPrice"], 0) or 0)
    qty   = int(float(_pick(attrs, ["availableAmount", "quantity", "qty", "stock", "balance"], 0) or 0))
    brand = _pick(attrs, ["brand", "brandName"], "")
    cat   = _pick(attrs, ["category", "categoryName"], "")
    brc   = _pick(attrs, ["barcode", "barCode", "ean"], "")

    return {
        "sku": sku,
        "name": name,
        "price": price,
        "qty": qty,
        "brand": brand or "",
        "category": cat or "",
        "barcode": brc or "",
        "raw_attributes": attrs,
    }

# ───────────────────────── router (self-contained) ─────────────────────────

def get_catalog_debug_router(_ignored_client=None) -> APIRouter:
    """
    Самодостаточный роутер каталога (не зависит от main.py/products.py).
    Эндпоинты:
      GET /debug/catalog/ping
      GET /debug/catalog/probe
      GET /debug/catalog/sample
    """
    router = APIRouter(tags=["debug_catalog"])

    @router.get("/debug/catalog/ping")
    async def ping(
        token_q: Optional[str] = Query(None, description="Опционально: токен через query ?token=..."),
        x_token: Optional[str] = Header(None, convert_underscores=False, alias="X-Kaspi-Token"),
        merchant_q: Optional[str] = Query(None, description="merchant id через query, если нужен"),
        x_merchant: Optional[str] = Header(None, convert_underscores=False, alias="X-Merchant-Id"),
    ):
        token = (token_q or x_token or _env("KASPI_TOKEN", "")).strip()
        mid   = (merchant_q or x_merchant or _env("MERCHANT_ID", "")).strip()
        bases = _list_base_urls()
        paths = _list_catalog_endpoints(mid)
        return {
            "ok": True,
            "has_token": bool(token),
            "merchant_id": mid,
            "base_urls": bases,
            "candidate_paths": paths,
        }

    @router.get("/debug/catalog/probe")
    async def probe(
        token_q: Optional[str] = Query(None),
        x_token: Optional[str] = Header(None, convert_underscores=False, alias="X-Kaspi-Token"),
        merchant_q: Optional[str] = Query(None),
        x_merchant: Optional[str] = Header(None, convert_underscores=False, alias="X-Merchant-Id"),
        city_id: Optional[str] = Query(None),
    ):
        token = (token_q or x_token or _env("KASPI_TOKEN", "")).strip()
        mid   = (merchant_q or x_merchant or _env("MERCHANT_ID", "")).strip()
        if not token:
            raise HTTPException(400, "Нет токена: передай ?token=... или заголовок X-Kaspi-Token, либо задай KASPI_TOKEN")
        base, path, js = await _probe_first_working(token, mid, city_id or "")
        # вернём краткую сводку и первые 3 предмета как есть
        items = js.get("data") or js.get("items") or js.get("content") or []
        preview = items[:3] if isinstance(items, list) else js
        return {"ok": True, "base_url": base, "endpoint": path, "preview": preview}

    @router.get("/debug/catalog/sample")
    async def sample(
        active_only: int = Query(1, ge=0, le=1, description="1 — только активные/видимые"),
        limit: int = Query(50, ge=1, le=5000),
        token_q: Optional[str] = Query(None),
        x_token: Optional[str] = Header(None, convert_underscores=False, alias="X-Kaspi-Token"),
        merchant_q: Optional[str] = Query(None),
        x_merchant: Optional[str] = Header(None, convert_underscores=False, alias="X-Merchant-Id"),
        city_id: Optional[str] = Query(None),
        force_base: Optional[str] = Query(None, description="Опционально: принудительно выбрать базовый URL"),
        force_endpoint: Optional[str] = Query(None, description="Опционально: указать точный путь (например, merchants/123/offers)"),
    ):
        token = (token_q or x_token or _env("KASPI_TOKEN", "")).strip()
        mid   = (merchant_q or x_merchant or _env("MERCHANT_ID", "")).strip()
        if not token:
            raise HTTPException(400, "Нет токена: передай ?token=... или заголовок X-Kaspi-Token, либо задай KASPI_TOKEN")

        if force_base and force_endpoint:
            base, path = force_base.rstrip("/"), force_endpoint.strip("/")

            async with httpx.AsyncClient(base_url=base, timeout=_timeout(), limits=_LIMITS) as cli:
                params = {"page[size]": str(min(limit, 200))}
                if active_only: params.update({"active": "true", "archived": "false", "visible": "true"})
                if city_id:     params["cityId"] = city_id
                js = await _get_json(cli, f"/{path}", params=params, headers=_kaspi_headers(token))
        else:
            base, path, js = await _probe_first_working(token, mid, city_id or "")

        # берём массив сущностей из типичных полей
        raw_items = js.get("data") or js.get("items") or js.get("content") or []
        if not isinstance(raw_items, list):
            raise HTTPException(500, f"Неожиданный формат ответа (ожидался список), ключи: {list(js.keys())}")

        # нормализация
        out = []
        for it in raw_items[:limit]:
            try:
                out.append(_normalize_item(it))
            except Exception:
                # если что-то незнакомое — положим как есть
                out.append({"sku": "", "name": "", "price": 0, "qty": 0, "brand": "", "category": "", "barcode": "", "raw_attributes": it})

        return {
            "ok": True,
            "base_url": force_base or base,
            "endpoint": force_endpoint or path,
            "count": len(out),
            "items": out,
        }

    return router
