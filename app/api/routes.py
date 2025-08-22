from __future__ import annotations
from fastapi import APIRouter
from fastapi.responses import PlainTextResponse, JSONResponse
from typing import Optional
import io, csv, time, httpx

from ..core.config import settings
from ..core.kaspi import build_headers
from ..services.analytics import analytics_payload, list_numbers

api_router = APIRouter()

@api_router.get("/meta")
def meta():
    return {
        "timezone": settings.TZ,
        "tz": settings.TZ,
        "currency": settings.CURRENCY,
        "day_cutoff": settings.DAY_CUTOFF,
        "pack_lookback_days": settings.PACK_LOOKBACK_DAYS,
    }

@api_router.get("/analytics")
async def analytics(
    start: str, end: str,
    tz: str = settings.TZ,
    date_field: str = "creationDate",
    states: Optional[str] = None,
    exclude_canceled: bool = True,
    end_time: Optional[str] = None,
    cutoff_mode: bool = False,
    cutoff: str = settings.DAY_CUTOFF,
    lookback_days: int = settings.PACK_LOOKBACK_DAYS,
    with_prev: bool = False,
):
    return await analytics_payload(start, end, tz, date_field, states, exclude_canceled, end_time, cutoff_mode, cutoff, lookback_days, with_prev)

@api_router.get("/orders/ids")
async def orders_ids(
    start: str, end: str,
    tz: str = settings.TZ,
    date_field: str = "creationDate",
    states: Optional[str] = None,
    exclude_canceled: bool = True,
    end_time: Optional[str] = None,
    cutoff_mode: bool = False,
    cutoff: str = settings.DAY_CUTOFF,
    lookback_days: int = settings.PACK_LOOKBACK_DAYS,
    limit: int = 20000
):
    return await list_numbers(start, end, tz, date_field, states, exclude_canceled, end_time, cutoff_mode, cutoff, lookback_days, limit)

@api_router.get("/orders/ids.csv")
async def orders_ids_csv(
    start: str, end: str,
    tz: str = settings.TZ,
    date_field: str = "creationDate",
    states: Optional[str] = None,
    exclude_canceled: bool = True,
    end_time: Optional[str] = None,
    cutoff_mode: bool = False,
    cutoff: str = settings.DAY_CUTOFF,
    lookback_days: int = settings.PACK_LOOKBACK_DAYS,
    limit: int = 20000
):
    data = await list_numbers(start, end, tz, date_field, states, exclude_canceled, end_time, cutoff_mode, cutoff, lookback_days, limit)
    out = io.StringIO()
    w = csv.writer(out, lineterminator="\n")
    w.writerow(["number","state","date","amount","city","id"])
    for it in data["items"]:
        w.writerow([it["number"], it["state"], it["date"], it["amount"], it["city"], it["id"]])
    return PlainTextResponse(out.getvalue(), media_type="text/csv; charset=utf-8")

@api_router.get("/diagnostics/ping-kaspi")
async def ping_kaspi():
    t0 = time.time()
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(10.0, connect=5.0)) as c:
            r = await c.get("https://kaspi.kz/shop/api/v2/orders", headers=build_headers(), params={"page[size]": 1})
        body = r.json() if r.status_code < 400 else {"text": r.text[:300]}
        return JSONResponse({"ok": r.status_code<400, "status": r.status_code, "elapsed": round(time.time()-t0,3), "keys": list(body.keys()) if isinstance(body, dict) else str(type(body))})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e), "elapsed": round(time.time()-t0,3)})
