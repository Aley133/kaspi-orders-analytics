# app/debug_sku.py
from __future__ import annotations

import os
import math
import asyncio
from datetime import datetime, timedelta
from typing import Iterable, Tuple, Dict, List

import httpx
import pytz
from fastapi import APIRouter, Query, HTTPException
from httpx import HTTPStatusError


def get_debug_router(client, default_tz: str = "Asia/Almaty", chunk_days: int = 7) -> APIRouter:
    """
    Возвращает APIRouter с эндпоинтами:
      - GET /debug/order-by-number
      - GET /debug/sample
    Подключи в main.py:  app.include_router(get_debug_router(client))
    """
    router = APIRouter()

    # ---- ENV / настройки сети ----
    KASPI_TOKEN = os.getenv("KASPI_TOKEN", "").strip()
    KASPI_BASE_URL = os.getenv("KASPI_BASE_URL", "https://kaspi.kz/shop/api/v2").rstrip("/")

    KASPI_HTTP_TIMEOUT = float(os.getenv("KASPI_HTTP_TIMEOUT", "90"))  # сек
    KASPI_RETRIES = int(os.getenv("KASPI_RETRIES", "2"))               # доп. попытки
    KASPI_PAGE_SIZE = int(os.getenv("KASPI_PAGE_SIZE", "50"))

    # ---------------- базовые утилиты ----------------
    def tzinfo_of(name: str) -> pytz.BaseTzInfo:
        try:
            return pytz.timezone(name)
        except Exception:
            raise HTTPException(status_code=400, detail=f"Bad timezone: {name}")

    def parse_date_local(d: str, tz: str) -> datetime:
        tzinfo = tzinfo_of(tz)
        y, m, dd = map(int, d.split("-"))
        return tzinfo.localize(datetime(y, m, dd, 0, 0, 0, 0))

    def iter_chunks(start_dt: datetime, end_dt: datetime, step_days: int) -> Iterable[Tuple[datetime, datetime]]:
        cur = start_dt
        while cur <= end_dt:
            nxt = min(cur + timedelta(days=step_days) - timedelta(milliseconds=1), end_dt)
            yield cur, nxt
            cur = nxt + timedelta(milliseconds=1)

    def _guess_number(attrs: dict, fallback_id: str) -> str:
        for k in ("number", "code", "orderNumber"):
            v = attrs.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
        return str(fallback_id)

    def extract_ms(attrs: dict, field: str) -> int | None:
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

    def _safe_get(d: dict, key: str):
        return d.get(key) if isinstance(d, dict) else None

    def _find_entries(attrs: dict) -> list[dict]:
        # Возможные ключи массива позиций на разных версиях
        for k in ("entries", "items", "positions", "orderItems", "products", "lines", "orderLines"):
            v = _safe_get(attrs, k)
            if isinstance(v, list) and v and isinstance(v[0], dict):
                return v
        # fallback: первый list[dict] на 1 уровне
        for v in attrs.values():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                return v
        return []

    SKU_KEYS = (
        "merchantProductCode", "article", "sku", "code", "productCode",
        "offerId", "vendorCode", "barcode", "skuId", "id"
    )
    TITLE_KEYS = (
        "productName", "name", "title", "itemName", "productTitle", "merchantProductName"
    )

    def sku_candidates(d: dict) -> dict[str, str]:
        out: dict[str, str] = {}
        for k in SKU_KEYS:
            v = _safe_get(d, k)
            if isinstance(v, (str, int, float)) and str(v).strip():
                out[k] = str(v).strip()
        return out

    def title_candidates(entry: dict) -> dict[str, str]:
        out: dict[str, str] = {}
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

    # ---------- работа с included ----------
    def build_included_index(included: list[dict]) -> dict[tuple[str, str], dict]:
        idx: dict[tuple[str, str], dict] = {}
        for it in included or []:
            t = it.get("type"); i = it.get("id")
            if t and i:
                idx[(str(t), str(i))] = it
        return idx

    def get_rel_id(obj: dict, rel_name: str) -> tuple[str | None, str | None]:
        rel = obj.get("relationships", {}).get(rel_name, {})
        data = rel.get("data")
        if isinstance(data, dict):
            return (data.get("type"), data.get("id"))
        return None, None

    # ---------- вызов /orders/{id}/entries с ретраями и фолбэком ----------
    async def api_fetch_entries(order_id: str, want_include: bool = True) -> dict:
        """
        1) Пытается получить /entries с include=product,merchantProduct (page[size]=KASPI_PAGE_SIZE)
        2) При таймауте/5xx делает повтор.
        3) При окончательной неудаче — фолбэк: без include, уменьшенный page[size].
        Всегда возвращает dict; при провале кладёт {"_error": "..."}.
        """
        if not KASPI_TOKEN:
            return {"_error": "KASPI_TOKEN is not set"}

        url = f"{KASPI_BASE_URL}/orders/{order_id}/entries"
        headers = {
            "X-Auth-Token": KASPI_TOKEN,
            "Accept": "application/vnd.api+json",
            "Content-Type": "application/vnd.api+json",
        }

        async def _do(include: bool, page_size: int) -> dict:
            params = {"page[size]": page_size}
            if include:
                params["include"] = "product,merchantProduct,masterProduct"
            async with httpx.AsyncClient(timeout=KASPI_HTTP_TIMEOUT) as s:
                r = await s.get(url, params=params, headers=headers)
                r.raise_for_status()
                return r.json()

        tries = max(1, KASPI_RETRIES)
        for i in range(tries):
            try:
                return await _do(want_include, KASPI_PAGE_SIZE)
            except (httpx.ReadTimeout, httpx.ConnectTimeout):
                await asyncio.sleep(min(2 * (i + 1), 6))
                continue
            except httpx.HTTPStatusError as e:
                if 500 <= e.response.status_code < 600 and i < tries - 1:
                    await asyncio.sleep(min(2 * (i + 1), 6))
                    continue
                break
            except Exception:
                break

        # fallback: без include и поменьше страница
        try:
            return await _do(False, max(10, math.ceil(KASPI_PAGE_SIZE / 2)))
        except Exception as e:
            return {"_error": f"entries fetch failed: {type(e).__name__}: {e}"}

    # ================================================== эндпоинты ==================================================

    @router.get("/debug/order-by-number")
    async def order_by_number(
        number: str = Query(..., description="Номер заказа из кабинета"),
        start: str = Query(..., description="YYYY-MM-DD"),
        end: str = Query(..., description="YYYY-MM-DD"),
        tz: str = Query(default_tz),
        date_field: str = Query("creationDate")
    ):
        if client is None:
            raise HTTPException(status_code=500, detail="KASPI_TOKEN is not set")

        tzinfo = tzinfo_of(tz)
        start_dt = parse_date_local(start, tz)
        end_dt = parse_date_local(end, tz) + timedelta(days=1) - timedelta(milliseconds=1)

        out: list[dict] = []

        for s, e in iter_chunks(start_dt, end_dt, chunk_days):
            try_field = date_field
            while True:
                try:
                    for order in client.iter_orders(start=s, end=e, filter_field=try_field):
                        attrs = order.get("attributes", {}) or {}
                        num = _guess_number(attrs, order.get("id"))
                        if str(num) != str(number):
                            continue

                        # (1) entries в attributes
                        entries = _find_entries(attrs)
                        rows: list[dict] = []

                        # (2) если нет — тянем /entries
                        api_payload = None
                        included_idx: Dict[tuple[str, str], dict] = {}
                        if not entries:
                            api_payload = await api_fetch_entries(order.get("id"))
                            if isinstance(api_payload, dict) and not api_payload.get("_error"):
                                raw_entries = api_payload.get("data") or []
                                entries = [
                                    (it.get("attributes", {}) | {"relationships": it.get("relationships", {})})
                                    for it in raw_entries
                                ]
                                included_idx = build_included_index(api_payload.get("included") or [])
                            else:
                                # даже при ошибке вернём пустые позиции, но покажем api_error ниже
                                entries = []

                        # соберём masterproduct ids из included (если есть вообще)
                        master_types = {"masterproducts", "masterproduct", "masterProducts", "masterProduct"}
                        included_master_ids = []
                        if included_idx:
                            for (t, i), obj in included_idx.items():
                                if t in master_types:
                                    included_master_ids.append(str(i))

                        for i, ent in enumerate(entries):
                            titles = title_candidates(ent)
                            skus = sku_candidates(ent)

                            # relationships-based: product.id / merchantProduct.id
                            prod_t, prod_id = get_rel_id(ent, "product")
                            mp_t, mp_id = get_rel_id(ent, "merchantProduct")

                            # название из included product
                            prod_attrs = (included_idx.get((prod_t, prod_id)) or {}).get("attributes", {}) if prod_id else {}
                            if prod_attrs:
                                nm = prod_attrs.get("name") or prod_attrs.get("title")
                                if isinstance(nm, str) and nm.strip():
                                    titles.setdefault("included.product.name", nm.strip())

                            # составной SKU productId_offerId/merchantProductId — совпадает с видом 115247815_269796431
                            offer_like = ent.get("offerId") or ent.get("merchantProductId") or mp_id
                            if prod_id and offer_like:
                                skus.setdefault("composed(productId_offerId)", f"{prod_id}_{offer_like}")

                            # masterproduct id: из product.relationships.masterProduct или из included
                            # 1) попробуем взять из included продукта
                            mp_rel_t, mp_rel_id = None, None
                            prod_included = included_idx.get((prod_t, prod_id)) if prod_id else None
                            if isinstance(prod_included, dict):
                                mp_rel_t, mp_rel_id = get_rel_id(prod_included, "masterProduct")
                            # 2) если не нашли — возьмём любой masterproduct из included как кандидат
                            kaspi_master = mp_rel_id or (included_master_ids[0] if included_master_ids else None)
                            if kaspi_master:
                                skus.setdefault("kaspi.masterproduct.id", str(kaspi_master))

                            rows.append({
                                "index": i,
                                "title_candidates": titles,
                                "sku_candidates": skus,
                                "all_keys": sorted(list(ent.keys())),
                                "raw": ent,
                            })

                        ms = extract_ms(attrs, date_field if date_field in attrs else try_field)
                        out.append({
                            "order_id": order.get("id"),
                            "number": num,
                            "state": attrs.get("state"),
                            "date_ms": ms,
                            "date_iso": datetime.fromtimestamp(ms/1000.0, tz=pytz.UTC).astimezone(tzinfo).isoformat() if ms else None,
                            "top_level_sku_candidates": sku_candidates(attrs),
                            "entries_count": len(rows),
                            "entries": rows,
                            "attrs_keys": sorted(list(attrs.keys())),
                            "attrs_raw": attrs,
                            "entries_api_raw": api_payload if isinstance(api_payload, dict) and not api_payload.get("_error") else None,
                            "api_error": (api_payload or {}).get("_error") if isinstance(api_payload, dict) else None,
                        })
                    break
                except HTTPStatusError as ee:
                    if ee.response.status_code in (400, 422) and try_field != "creationDate":
                        try_field = "creationDate"
                        continue
                    raise

        return {"ok": True, "items": out}

    @router.get("/debug/sample")
    async def debug_sample(
        start: str = Query(...), end: str = Query(...),
        tz: str = Query(default_tz), date_field: str = Query("creationDate"),
        limit: int = Query(10)
    ):
        if client is None:
            raise HTTPException(status_code=500, detail="KASPI_TOKEN is not set")

        start_dt = parse_date_local(start, tz)
        end_dt = parse_date_local(end, tz) + timedelta(days=1) - timedelta(milliseconds=1)

        res: list[dict] = []
        for s, e in iter_chunks(start_dt, end_dt, chunk_days):
            try_field = date_field
            while True:
                try:
                    for order in client.iter_orders(start=s, end=e, filter_field=try_field):
                        attrs = order.get("attributes", {}) or {}
                        entries = _find_entries(attrs)
                        first = entries[0] if entries else {}

                        api_payload = None
                        if not first:
                            api_payload = await api_fetch_entries(order.get("id"))
                            if isinstance(api_payload, dict) and not api_payload.get("_error"):
                                raw_entries = api_payload.get("data") or []
                                if raw_entries:
                                    first = (raw_entries[0].get("attributes") or {})
                                    # relationships → составной SKU
                                    relationships = raw_entries[0].get("relationships") or {}
                                    prod = (relationships.get("product") or {}).get("data") or {}
                                    mp   = (relationships.get("merchantProduct") or {}).get("data") or {}
                                    prod_id = prod.get("id")
                                    offer_like = first.get("offerId") or mp.get("id")
                                    composed = f"{prod_id}_{offer_like}" if prod_id and offer_like else None
                                    sc = sku_candidates(first)
                                    if composed:
                                        sc.setdefault("composed(productId_offerId)", composed)
                                    # masterproduct из included
                                    included_idx = build_included_index(api_payload.get("included") or [])
                                    prod_included = included_idx.get((prod.get("type"), prod_id)) if prod_id else None
                                    if isinstance(prod_included, dict):
                                        mp_rel_t, mp_rel_id = get_rel_id(prod_included, "masterProduct")
                                        if mp_rel_id:
                                            sc.setdefault("kaspi.masterproduct.id", str(mp_rel_id))
                                    res.append({
                                        "order_id": order.get("id"),
                                        "number": _guess_number(attrs, order.get("id")),
                                        "state": attrs.get("state"),
                                        "title_candidates": title_candidates(first),
                                        "sku_candidates": sc,
                                    })
                                    if len(res) >= limit:
                                        return {"ok": True, "items": res}
                                else:
                                    res.append({
                                        "order_id": order.get("id"),
                                        "number": _guess_number(attrs, order.get("id")),
                                        "state": attrs.get("state"),
                                        "title_candidates": {},
                                        "sku_candidates": {},
                                    })
                            else:
                                res.append({
                                    "order_id": order.get("id"),
                                    "number": _guess_number(attrs, order.get("id")),
                                    "state": attrs.get("state"),
                                    "title_candidates": {},
                                    "sku_candidates": {},
                                })
                        else:
                            res.append({
                                "order_id": order.get("id"),
                                "number": _guess_number(attrs, order.get("id")),
                                "state": attrs.get("state"),
                                "title_candidates": title_candidates(first),
                                "sku_candidates": sku_candidates(first),
                            })

                        if len(res) >= limit:
                            return {"ok": True, "items": res}
                    break
                except HTTPStatusError as ee:
                    if ee.response.status_code in (400, 422) and try_field != "creationDate":
                        try_field = "creationDate"
                        continue
                    raise
        return {"ok": True, "items": res}

    return router
