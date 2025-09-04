# app/debug_catalog.py
from __future__ import annotations
import os, asyncio, random, json, time
from typing import Optional, Dict, Any, List, Tuple
from fastapi import APIRouter, HTTPException, Query, Header
from fastapi.responses import JSONResponse
import httpx

# ───────────────────────────── helpers: env / sanitize ─────────────────────────────

def _env(key: str, default: str = "") -> str:
    v = os.getenv(key, default) or ""
    return v.strip().strip('"').strip("'")

def _token_from(req_token: Optional[str], header_token: Optional[str]) -> str:
    return (req_token or header_token or _env("KASPI_TOKEN", "")).strip()

def _mask(s: Optional[str], keep: int = 4) -> str:
    if not s: return ""
    s = str(s)
    return "*" * max(0, len(s) - keep) + s[-keep:]

def _safe_headers(h: Dict[str, str]) -> Dict[str, str]:
    # оставим только диагностически-важные, без Set-Cookie
    allow = {"content-type","date","server","vary","x-request-id","x-rate-limit-remaining","x-rate-limit-reset"}
    out = {}
    for k, v in h.items():
        kl = str(k).lower()
        if kl in allow:
            out[kl] = v
    return out

def _list_base_urls() -> List[str]:
    raw = _env("KASPI_BASE_URLS", "") or _env("KASPI_BASE_URL", "")
    if not raw:
        raw = "https://kaspi.kz/shop/api/v2,https://kaspi.kz/mc/api/v2"
    return [u.strip().rstrip("/") for u in raw.split(",") if u.strip()]

def _kaspi_headers(token: str, jsonapi: bool = True) -> Dict[str, str]:
    h = {
        "X-Auth-Token": token,
        "User-Agent": "Mozilla/5.0 (debug-catalog/1.0)",
    }
    if jsonapi:
        h["Accept"] = "application/vnd.api+json"
        h["Content-Type"] = "application/vnd.api+json"
    return h

def _timeout(connect=8.0, read=20.0, write=15.0, pool=40.0) -> httpx.Timeout:
    return httpx.Timeout(connect=connect, read=read, write=write, pool=pool)

_LIMITS = httpx.Limits(max_connections=12, max_keepalive_connections=6)

def _shorten(s: str, n: int = 2000) -> str:
    s = s or ""
    return s if len(s) <= n else s[:n] + "…"

# ───────────────────────────── core http helper ─────────────────────────────

async def _fetch_json_or_text(
    base: str, path: str, token: str,
    params: Optional[Dict[str, Any]] = None,
    *, attempts: int = 1, read_timeout: float = 20.0,
) -> Dict[str, Any]:
    """
    Возвращает диагностический словарь:
    { ok, status, url, elapsed_ms, headers, json?, text? }
    НИКОГДА не кидает 500 наружу — всё завернём в JSON.
    """
    params = params or {}
    url_path = path if path.startswith("/") else f"/{path}"
    diag: Dict[str, Any] = {
        "ok": False,
        "base_url": base,
        "path": path,
        "url": f"{base}{url_path}",
        "params": params,
        "attempts": attempts,
        "headers_out": {"X-Auth-Token": _mask(token)},
    }

    back = 0.6
    async with httpx.AsyncClient(base_url=base, timeout=_timeout(read=read_timeout), limits=_LIMITS) as cli:
        for i in range(attempts):
            t0 = time.perf_counter()
            try:
                r = await cli.get(url_path, params=params, headers=_kaspi_headers(token))
                elapsed = int((time.perf_counter() - t0) * 1000)
                diag.update({"status": r.status_code, "elapsed_ms": elapsed, "headers_in": _safe_headers(r.headers)})
                # пробуем JSON, иначе текст
                txt = await r.aread()
                if r.headers.get("content-type","").startswith("application/json"):
                    try:
                        js = json.loads(txt.decode("utf-8", errors="ignore"))
                        diag["json"] = js
                        diag["ok"] = r.is_success
                        return diag
                    except Exception:
                        # тело заявлено как json, но парсится плохо — вернем текст
                        diag["text"] = _shorten(txt.decode("utf-8", errors="ignore"))
                        return diag
                else:
                    diag["text"] = _shorten(txt.decode("utf-8", errors="ignore"))
                    diag["ok"] = r.is_success
                    return diag
            except httpx.HTTPStatusError as e:
                elapsed = int((time.perf_counter() - t0) * 1000)
                resp = e.response
                diag.update({"status": resp.status_code if resp else None, "elapsed_ms": elapsed})
                if resp is not None:
                    diag["headers_in"] = _safe_headers(resp.headers)
                    try:
                        diag["text"] = _shorten(resp.text)
                    except Exception:
                        pass
                if i == attempts - 1:
                    return diag
            except httpx.RequestError as e:
                elapsed = int((time.perf_counter() - t0) * 1000)
                diag.update({"network_error": str(e), "elapsed_ms": elapsed})
                if i == attempts - 1:
                    return diag
            await asyncio.sleep(min(back, 6.0) + random.uniform(0.0, 0.3))
            back *= 1.8
    return diag

# ───────────────────────────── normalization ─────────────────────────────

def _pick(d: Dict[str, Any], keys: List[str], default=None):
    for k in keys:
        if not isinstance(d, dict): continue
        v = d.get(k)
        if isinstance(v, (str, int, float)) and (str(v).strip() != "" or isinstance(v, (int, float))):
            return v
    return default

def _normalize_item_any(raw: Any) -> Dict[str, Any]:
    """
    Пробуем несколько типичных схем (JSON:API data[i].attributes / items[i] / content[i])
    """
    attrs = None
    if isinstance(raw, dict):
        if "attributes" in raw and isinstance(raw["attributes"], dict):
            attrs = raw["attributes"]
        elif "data" in raw and isinstance(raw["data"], dict) and "attributes" in raw["data"]:
            attrs = raw["data"]["attributes"]
        else:
            attrs = raw
    else:
        attrs = {}

    name = _pick(attrs, ["title", "name", "productName", "shortName"], "")
    sku  = str(_pick(attrs, ["code", "sku", "offerCode", "merchantCode", "article"], "") or "")
    price = float(_pick(attrs, ["price", "minPrice", "basePrice", "sellingPrice"], 0) or 0)
    qty   = int(float(_pick(attrs, ["availableAmount", "quantity", "qty", "stock", "balance"], 0) or 0))
    brand = _pick(attrs, ["brand", "brandName"], "") or ""
    cat   = _pick(attrs, ["category", "categoryName"], "") or ""
    brc   = _pick(attrs, ["barcode", "barCode", "ean"], "") or ""
    return {
        "sku": sku, "name": name, "price": price, "qty": qty,
        "brand": brand, "category": cat, "barcode": brc,
        "raw_attributes": attrs,
    }

def _extract_items_from_payload(js: Dict[str, Any]) -> List[Any]:
    for key in ("data", "items", "content", "results"):
        v = js.get(key)
        if isinstance(v, list):
            return v
    return []

# ───────────────────────────── probe candidates ─────────────────────────────

def _candidate_endpoints_for_shop(mid: str) -> List[str]:
    """
    Эндпоинты для /shop/api/v2. Фильтры — разные диалекты.
    """
    base = [
        f"offers?merchantId={mid}",
        f"offers?filter[merchantId]={mid}",
        f"offers?filter[merchant.id]={mid}",
        f"catalog/offers?merchantId={mid}",
        f"catalog/offers?filter[merchantId]={mid}",
        f"catalog/offers?filter[merchant.id]={mid}",
    ]
    extras = ["", "&visible=true", "&active=true&archived=false"]
    out = []
    for p in base:
        for ex in extras:
            out.append(p + ex)
    return out

# ───────────────────────────── router ─────────────────────────────

def get_catalog_debug_router(_ignored_client=None) -> APIRouter:
    """
    Самодостаточный отладочный роутер каталога.
    Эндпоинты:
      GET /debug/catalog/help          — подсказка
      GET /debug/catalog/ping          — проверка ENV/кандидатов
      GET /debug/catalog/probe         — перебор типовых эндпоинтов /shop/api/v2
      GET /debug/catalog/try           — прямой запрос (force_base + endpoint)
      GET /debug/catalog/sample        — сократитель: успешный запрос → нормализация
      GET /debug/catalog/offer         — карточка по offerId / offer.code
    """
    router = APIRouter(tags=["debug_catalog"])

    @router.get("/debug/catalog/help")
    async def help():
        return {
            "endpoints": {
                "/debug/catalog/ping": "Показывает, виден ли токен/merchantId и какие base-url будут пробоваться.",
                "/debug/catalog/probe": "Быстрый перебор /shop/api/v2 + offers/catalog/offers с разными фильтрами.",
                "/debug/catalog/try": "Прямой вызов к апстриму. Параметры: force_base, endpoint (можно с query), page_size.",
                "/debug/catalog/sample": "Упрощённый вариант: успешный ответ → items нормализованы (sku, name, price, qty, ...).",
                "/debug/catalog/offer": "Проверка доступа к офферу: offerId=... или offer_code=AAA_BBB (берём вторую часть как id).",
            },
            "usage": {
                "probe": "/debug/catalog/probe?merchant_q=30295031 (заголовок X-Kaspi-Token можно передать)",
                "try": "/debug/catalog/try?force_base=https://kaspi.kz/shop/api/v2&endpoint=offers?merchantId=30295031",
                "offer": "/debug/catalog/offer?offerId=719024874  ИЛИ  ?offer_code=130974342_719024874",
            }
        }

    @router.get("/debug/catalog/ping")
    async def ping(
        token_q: Optional[str] = Query(None),
        x_token: Optional[str] = Header(None, convert_underscores=False, alias="X-Kaspi-Token"),
        merchant_q: Optional[str] = Query(None),
        x_merchant: Optional[str] = Header(None, convert_underscores=False, alias="X-Merchant-Id"),
    ):
        token = _token_from(token_q, x_token)
        mid   = (merchant_q or x_merchant or _env("MERCHANT_ID", "")).strip()
        bases = _list_base_urls()
        return {
            "ok": True,
            "has_token": bool(token),
            "token_tail": _mask(token),
            "merchant_id": mid,
            "base_urls": bases,
            "shop_candidates": _candidate_endpoints_for_shop(mid) if mid else [],
        }

    @router.get("/debug/catalog/probe")
    async def probe(
        token_q: Optional[str] = Query(None),
        x_token: Optional[str] = Header(None, convert_underscores=False, alias="X-Kaspi-Token"),
        merchant_q: Optional[str] = Query(None),
        x_merchant: Optional[str] = Header(None, convert_underscores=False, alias="X-Merchant-Id"),
        limit: int = Query(1, ge=1, le=50, description="сколько позиций просить на страницу"),
        attempts: int = Query(1, ge=1, le=3),
        read_timeout: float = Query(10.0, ge=1.0, le=60.0),
    ):
        """
        Быстрый перебор /shop/api/v2 + offers/catalog/offers с разными фильтрами.
        Никогда не возвращает 500; всегда JSON с попытками и первой удачной.
        """
        token = _token_from(token_q, x_token)
        mid   = (merchant_q or x_merchant or _env("MERCHANT_ID", "")).strip()
        if not token:
            return JSONResponse(status_code=400, content={"ok": False, "error": "no_token"})

        bases = [b for b in _list_base_urls() if "shop/api/v2" in b] or ["https://kaspi.kz/shop/api/v2"]
        eps = _candidate_endpoints_for_shop(mid) if mid else []
        attempts_log: List[Dict[str, Any]] = []
        first_ok: Optional[Dict[str, Any]] = None

        for base in bases:
            for ep in eps:
                # добавим page[size]
                joiner = "&" if "?" in ep else "?"
                ep_ps = f"{ep}{joiner}page[size]={min(limit, 50)}"
                res = await _fetch_json_or_text(base, ep_ps, token, attempts=attempts, read_timeout=read_timeout)
                item = {
                    "base": base, "endpoint": ep_ps,
                    "status": res.get("status"), "elapsed_ms": res.get("elapsed_ms"),
                    "ok": bool(res.get("ok")),
                }
                if "json" in res:
                    # сократим лог, но ключи покажем
                    item["json_keys"] = list(res["json"].keys())
                    items = _extract_items_from_payload(res["json"])
                    item["items_len"] = len(items)
                    if not first_ok and res.get("ok") and items:
                        first_ok = {"base": base, "endpoint": ep_ps, "payload": res["json"]}
                else:
                    item["text"] = _shorten(res.get("text",""))
                attempts_log.append(item)
                if first_ok:
                    break
            if first_ok:
                break

        out: Dict[str, Any] = {"ok": bool(first_ok), "attempts": attempts_log}
        if first_ok:
            raw_items = _extract_items_from_payload(first_ok["payload"])
            out.update({
                "base_url": first_ok["base"],
                "endpoint": first_ok["endpoint"],
                "count": len(raw_items),
                "items": [_normalize_item_any(x) for x in raw_items],
            })
        return JSONResponse(status_code=200 if first_ok else 207, content=out)

    @router.get("/debug/catalog/try")
    async def try_direct(
        force_base: str = Query(..., description="например https://kaspi.kz/shop/api/v2"),
        endpoint: str = Query(..., description="например offers?merchantId=30295031"),
        token_q: Optional[str] = Query(None),
        x_token: Optional[str] = Header(None, convert_underscores=False, alias="X-Kaspi-Token"),
        page_size: int = Query(10, ge=1, le=200),
        read_timeout: float = Query(20.0, ge=1.0, le=120.0),
        attempts: int = Query(1, ge=1, le=5),
    ):
        token = _token_from(token_q, x_token)
        if not token:
            return JSONResponse(status_code=400, content={"ok": False, "error": "no_token"})
        # аккуратно добавим page[size]
        ep = endpoint
        ep = ep + ("&" if "?" in ep else "?") + f"page[size]={min(page_size, 200)}"
        res = await _fetch_json_or_text(force_base.rstrip("/"), ep.strip("/"), token, attempts=attempts, read_timeout=read_timeout)

        # Нормализация, если json и есть список
        norm = None
        if res.get("json"):
            items = _extract_items_from_payload(res["json"])
            norm = [_normalize_item_any(x) for x in items]

        return {
            "probe": {
                "ok": res.get("ok"), "status": res.get("status"),
                "url": res.get("url"), "elapsed_ms": res.get("elapsed_ms"),
                "headers_in": res.get("headers_in"), "headers_out": res.get("headers_out"),
            },
            "raw_json_keys": list(res["json"].keys()) if res.get("json") else None,
            "raw_text_snippet": res.get("text"),
            "normalized_items": norm[:50] if isinstance(norm, list) else None,
            "normalized_count": len(norm) if isinstance(norm, list) else 0,
        }

    @router.get("/debug/catalog/sample")
    async def sample(
        token_q: Optional[str] = Query(None),
        x_token: Optional[str] = Header(None, convert_underscores=False, alias="X-Kaspi-Token"),
        merchant_q: Optional[str] = Query(None),
        x_merchant: Optional[str] = Header(None, convert_underscores=False, alias="X-Merchant-Id"),
        limit: int = Query(50, ge=1, le=500),
    ):
        """
        Упрощённый сценарий: пробуем /shop/api/v2 c типовыми фильтрами,
        как только находим работающую — нормализуем и возвращаем items.
        """
        token = _token_from(token_q, x_token)
        mid   = (merchant_q or x_merchant or _env("MERCHANT_ID", "")).strip()
        if not token:
            return JSONResponse(status_code=400, content={"ok": False, "error": "no_token"})
        if not mid:
            return JSONResponse(status_code=400, content={"ok": False, "error": "no_merchant_id"})

        base = "https://kaspi.kz/shop/api/v2"
        eps  = _candidate_endpoints_for_shop(mid)
        first_ok: Optional[Tuple[str, str, Dict[str, Any]]] = None
        attempts_log: List[Dict[str, Any]] = []

        for ep in eps:
            ep_ps = ep + ("&" if "?" in ep else "?") + f"page[size]={min(limit, 200)}"
            res = await _fetch_json_or_text(base, ep_ps, token, attempts=1, read_timeout=10.0)
            attempts_log.append({"ep": ep_ps, "status": res.get("status"), "ok": res.get("ok"), "elapsed_ms": res.get("elapsed_ms")})
            if res.get("ok") and res.get("json"):
                items = _extract_items_from_payload(res["json"])
                if items:
                    first_ok = (base, ep_ps, res["json"])
                    break

        if not first_ok:
            return JSONResponse(status_code=207, content={
                "ok": False,
                "message": "Не нашли рабочий эндпоинт в /shop/api/v2. См. попытки ниже. Если всюду 401/403 — у токена нет прав на каталог.",
                "attempts": attempts_log
            })

        raw_items = _extract_items_from_payload(first_ok[2])[:limit]
        norm = [_normalize_item_any(x) for x in raw_items]
        return {
            "ok": True,
            "base_url": first_ok[0],
            "endpoint": first_ok[1],
            "count": len(norm),
            "items": norm,
            "attempts_used": attempts_log,
        }

    @router.get("/debug/catalog/offer")
    async def offer_lookup(
        offerId: Optional[str] = Query(None, description="например 719024874"),
        offer_code: Optional[str] = Query(None, description="например 130974342_719024874 (берём часть после _)"),
        token_q: Optional[str] = Query(None),
        x_token: Optional[str] = Header(None, convert_underscores=False, alias="X-Kaspi-Token"),
    ):
        """
        Проверка прав через оффер: /shop/api/v2/offers/{id}
        Это работает у тех токенов, которые уже видят /orders.
        """
        token = _token_from(token_q, x_token)
        if not token:
            return JSONResponse(status_code=400, content={"ok": False, "error": "no_token"})

        oid = (offerId or "")
        if not oid and offer_code and "_" in offer_code:
            oid = offer_code.split("_")[-1]
        if not oid:
            return JSONResponse(status_code=400, content={"ok": False, "error": "no_offerId_or_code"})

        base = "https://kaspi.kz/shop/api/v2"
        res = await _fetch_json_or_text(base, f"offers/{oid}", token, attempts=1, read_timeout=10.0)
        norm = None
        if res.get("json"):
            # некоторые ответы завернуты data: { attributes: {...} }
            payload = res["json"].get("data") or res["json"]
            if isinstance(payload, dict):
                norm = _normalize_item_any(payload)
        return {
            "probe": {
                "ok": res.get("ok"), "status": res.get("status"),
                "url": res.get("url"), "elapsed_ms": res.get("elapsed_ms"),
                "headers_in": res.get("headers_in"),
            },
            "raw_json_keys": list(res["json"].keys()) if res.get("json") else None,
            "raw_text_snippet": res.get("text"),
            "normalized": norm,
        }

    return router
