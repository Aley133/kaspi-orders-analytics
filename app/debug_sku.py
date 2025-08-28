# app/debug_sku.py
from __future__ import annotations

from fastapi import APIRouter, Query, HTTPException
from httpx import HTTPStatusError
from datetime import datetime, timedelta
from typing import Iterable, Tuple, List, Dict
import pytz

def get_debug_router(client, default_tz: str = "Asia/Almaty", chunk_days: int = 7) -> APIRouter:
    """
    Возвращает APIRouter с эндпоинтами:
      - GET /debug/order-by-number
      - GET /debug/sample
    Работает на том же домене, что и main.py.
    """
    if client is None:
        # пусть ошибка будет в рантайме при первом вызове эндпоинта,
        # но лучше явно оставить проверку.
        pass

    router = APIRouter()

    # --- локальные утилиты (копии, чтобы не тянуть main.py и избежать циклического импорта) ---
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
        # возможные имена массива позиций
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

    # --- эндпоинты ---
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

        found: list[dict] = []

        for s, e in iter_chunks(start_dt, end_dt, chunk_days):
            try_field = date_field
            while True:
                try:
                    for order in client.iter_orders(start=s, end=e, filter_field=try_field):
                        attrs = order.get("attributes", {}) or {}
                        num = _guess_number(attrs, order.get("id"))
                        if str(num) != str(number):
                            continue

                        entries = _find_entries(attrs)
                        rows = []
                        for i, ent in enumerate(entries):
                            rows.append({
                                "index": i,
                                "title_candidates": title_candidates(ent),
                                "sku_candidates": sku_candidates(ent),
                                "all_keys": sorted(list(ent.keys())),
                                "raw": ent,
                            })

                        ms = extract_ms(attrs, date_field if date_field in attrs else try_field)
                        found.append({
                            "order_id": order.get("id"),
                            "number": num,
                            "state": attrs.get("state"),
                            "date_ms": ms,
                            "date_iso": datetime.fromtimestamp(ms/1000.0, tz=pytz.UTC).astimezone(tzinfo).isoformat() if ms else None,
                            "top_level_sku_candidates": sku_candidates(attrs),
                            "entries_count": len(entries),
                            "entries": rows,
                            "attrs_keys": sorted(list(attrs.keys())),
                            "attrs_raw": attrs,
                        })
                    break
                except HTTPStatusError as ee:
                    # fallback на creationDate если фильтр не поддерживается
                    if ee.response.status_code in (400, 422) and try_field != "creationDate":
                        try_field = "creationDate"
                        continue
                    raise

        return {"ok": True, "items": found}

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

        out: list[dict] = []
        for s, e in iter_chunks(start_dt, end_dt, chunk_days):
            try_field = date_field
            while True:
                try:
                    for order in client.iter_orders(start=s, end=e, filter_field=try_field):
                        attrs = order.get("attributes", {}) or {}
                        entries = _find_entries(attrs)
                        first = entries[0] if entries else {}
                        out.append({
                            "order_id": order.get("id"),
                            "number": _guess_number(attrs, order.get("id")),
                            "state": attrs.get("state"),
                            "title_candidates": title_candidates(first) if first else {},
                            "sku_candidates": sku_candidates(first) if first else {},
                        })
                        if len(out) >= limit:
                            return {"ok": True, "items": out}
                    break
                except HTTPStatusError as ee:
                    if ee.response.status_code in (400, 422) and try_field != "creationDate":
                        try_field = "creationDate"
                        continue
                    raise
        return {"ok": True, "items": out}

    return router
