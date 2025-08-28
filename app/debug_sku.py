# debug_sku.py — минимальный сервис для "пробоя" SKU/названий по номеру заказа
from __future__ import annotations

import os
import json
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Iterable, Tuple

import pytz
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from dotenv import load_dotenv

# берём существующий клиент из проекта (робастный импорт, как у тебя в main.py)
try:
    from app.kaspi_client import KaspiClient  # type: ignore
except Exception:
    try:
        from .kaspi_client import KaspiClient  # type: ignore
    except Exception:
        from kaspi_client import KaspiClient  # type: ignore

load_dotenv()

KASPI_TOKEN = os.getenv("KASPI_TOKEN", "").strip()
KASPI_BASE_URL = os.getenv("KASPI_BASE_URL", "https://kaspi.kz/shop/api/v2")
DEFAULT_TZ = os.getenv("TZ", "Asia/Almaty")

client = KaspiClient(token=KASPI_TOKEN, base_url=KASPI_BASE_URL) if KASPI_TOKEN else None

app = FastAPI(title="Kaspi Debug: SKU/Title probe")

# ---------- утилиты ----------
def tzinfo_of(name: str) -> pytz.BaseTzInfo:
    try:
        return pytz.timezone(name)
    except Exception:
        raise HTTPException(status_code=400, detail=f"Bad timezone: {name}")

def parse_date_local(d: str, tz: str) -> datetime:
    tzinfo = tzinfo_of(tz)
    y, m, dd = map(int, d.split("-"))
    return tzinfo.localize(datetime(y, m, dd, 0, 0, 0, 0))

def iter_chunks(start_dt: datetime, end_dt: datetime, step_days: int = 7) -> Iterable[Tuple[datetime, datetime]]:
    cur = start_dt
    while cur <= end_dt:
        nxt = min(cur + timedelta(days=step_days) - timedelta(milliseconds=1), end_dt)
        yield cur, nxt
        cur = nxt + timedelta(milliseconds=1)

def safe_get(d: dict, key: str):
    return d.get(key) if isinstance(d, dict) else None

def find_entries(attrs: dict) -> List[dict]:
    """
    Находим массив позиций заказа в разных возможных полях.
    """
    for k in ("entries", "items", "positions", "orderItems", "products"):
        v = safe_get(attrs, k)
        if isinstance(v, list) and v and isinstance(v[0], dict):
            return v
    # fallback: ищем первый list[dict] на 1 уровне
    for v in attrs.values():
        if isinstance(v, list) and v and isinstance(v[0], dict):
            return v
    return []

SKU_KEYS = (
    "merchantProductCode", "article", "sku", "code", "productCode",
    "offerId", "vendorCode", "barcode", "skuId", "id"
)
TITLE_KEYS = (
    "productName", "name", "title", "itemName",
    "productTitle", "merchantProductName"
)

def sku_candidates(entry_or_attrs: dict) -> Dict[str, str]:
    out = {}
    for k in SKU_KEYS:
        v = safe_get(entry_or_attrs, k)
        if isinstance(v, str) and v.strip():
            out[k] = v.strip()
        elif isinstance(v, (int, float)):
            out[k] = str(v)
    return out

def title_candidates(entry: dict) -> Dict[str, str]:
    out = {}
    for k in TITLE_KEYS:
        v = safe_get(entry, k)
        if isinstance(v, str) and v.strip():
            out[k] = v.strip()
    # иногда название находится в подполяx типа product.{title/name}
    prod = safe_get(entry, "product")
    if isinstance(prod, dict):
        for k in TITLE_KEYS:
            v = safe_get(prod, k)
            if isinstance(v, str) and v.strip():
                out[f"product.{k}"] = v.strip()
    return out

def guess_order_number(attrs: dict, fallback: str) -> str:
    for k in ("number", "code", "orderNumber"):
        v = safe_get(attrs, k)
        if isinstance(v, str) and v.strip():
            return v.strip()
        if isinstance(v, (int, float)):
            return str(v)
    return str(fallback)

# ---------- endpoints ----------
@app.get("/", response_class=HTMLResponse)
async def root():
    return HTMLResponse("""
<!doctype html><html><head><meta charset="utf-8">
<title>Kaspi Debug: SKU/Title</title></head>
<body>
  <h1>Kaspi Debug: SKU/Title</h1>
  <p>Открой <a href="/debug.html">debug.html</a> чтобы искать по номеру заказа.</p>
</body></html>
""")

@app.get("/debug.html", response_class=HTMLResponse)
async def debug_page():
    # эта страница дублирует debug_sku.html на случай, если проще запустить одним файлом
    with open(os.path.join(os.path.dirname(__file__), "debug_sku.html"), "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())

@app.get("/debug/order-by-number")
async def order_by_number(
    number: str = Query(..., description="Номер заказа из кабинета, напр. 623903299"),
    start: str = Query(..., description="YYYY-MM-DD"),
    end: str = Query(..., description="YYYY-MM-DD"),
    tz: str = Query(DEFAULT_TZ),
    date_field: str = Query("creationDate")
):
    if client is None:
        raise HTTPException(status_code=500, detail="KASPI_TOKEN is not set")

    tzinfo = tzinfo_of(tz)
    start_dt = parse_date_local(start, tz)
    end_dt = parse_date_local(end, tz) + timedelta(days=1) - timedelta(milliseconds=1)

    found = []
    for s, e in iter_chunks(start_dt, end_dt, 7):
        for order in client.iter_orders(start=s, end=e, filter_field=date_field):
            attrs = order.get("attributes", {}) or {}
            num = guess_order_number(attrs, order.get("id"))
            if str(num) != str(number):
                continue

            # соберём полную картину по позициям
            entries = find_entries(attrs)
            entry_rows = []
            for idx, ent in enumerate(entries):
                entry_rows.append({
                    "index": idx,
                    "title_candidates": title_candidates(ent),
                    "sku_candidates": sku_candidates(ent),
                    "all_keys": sorted(list(ent.keys())),
                    "raw": ent,  # специально отдаём целиком для точной диагностики
                })

            found.append({
                "order_id": order.get("id"),
                "number": num,
                "state": attrs.get("state"),
                "date_ms": attrs.get(date_field),
                "date_iso": datetime.fromtimestamp(int(attrs.get(date_field))/1000.0, tz=pytz.UTC).astimezone(tzinfo).isoformat()
                            if attrs.get(date_field) else None,
                "top_level_sku_candidates": sku_candidates(attrs),
                "entries_count": len(entries),
                "entries": entry_rows,
                "attrs_keys": sorted(list(attrs.keys())),
                "attrs_raw": attrs,  # тоже целиком
            })

    if not found:
        return {"ok": True, "message": "не нашли заказ с таким номером в периоде", "items": []}

    return {"ok": True, "items": found}

@app.get("/debug/sample")
async def sample_orders(
    start: str = Query(...), end: str = Query(...),
    tz: str = Query(DEFAULT_TZ), date_field: str = Query("creationDate"),
    limit: int = Query(10)
):
    if client is None:
        raise HTTPException(status_code=500, detail="KASPI_TOKEN is not set")

    tzinfo = tzinfo_of(tz)
    start_dt = parse_date_local(start, tz)
    end_dt = parse_date_local(end, tz) + timedelta(days=1) - timedelta(milliseconds=1)

    out = []
    for s, e in iter_chunks(start_dt, end_dt, 7):
        for order in client.iter_orders(start=s, end=e, filter_field=date_field):
            attrs = order.get("attributes", {}) or {}
            entries = find_entries(attrs)
            first = entries[0] if entries else {}
            out.append({
                "order_id": order.get("id"),
                "number": guess_order_number(attrs, order.get("id")),
                "state": attrs.get("state"),
                "title_candidates": title_candidates(first) if first else {},
                "sku_candidates": sku_candidates(first) if first else {},
            })
            if len(out) >= limit:
                return {"ok": True, "items": out}
    return {"ok": True, "items": out}

