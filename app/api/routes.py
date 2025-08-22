from __future__ import annotations
from fastapi import APIRouter, Query
from fastapi.responses import PlainTextResponse
from typing import Optional
import csv, io

from ..core.config import settings
from ..services.analytics import analytics_payload, list_numbers

router = APIRouter()

@router.get("/meta")
def meta():
    return {
        "timezone": settings.TZ,
        "currency": settings.CURRENCY,
        "day_cutoff": settings.DAY_CUTOFF,
        "pack_lookback_days": settings.PACK_LOOKBACK_DAYS,
        "amount_fields": settings.AMOUNT_FIELDS,
    }

@router.get("/analytics")
def analytics(start: str = Query(...), end: str = Query(...),
              tz: str = Query(settings.TZ),
              date_field: str = Query("creationDate"),
              states: Optional[str] = Query(None),
              exclude_canceled: bool = Query(True),
              end_time: Optional[str] = Query(None),
              cutoff_mode: bool = Query(False),
              cutoff: str = Query(settings.DAY_CUTOFF),
              lookback_days: int = Query(settings.PACK_LOOKBACK_DAYS),
              with_prev: bool = Query(True)):
    return analytics_payload(start=start, end=end, tz=tz, date_field=date_field,
                             states=states, exclude_canceled=exclude_canceled,
                             end_time=end_time, cutoff_mode=cutoff_mode,
                             cutoff=cutoff, lookback_days=lookback_days,
                             with_prev=with_prev)

@router.get("/orders/ids")
def orders_ids(start: str, end: str, tz: str = settings.TZ,
               date_field: str = "creationDate", states: Optional[str] = None,
               exclude_canceled: bool = True, end_time: Optional[str] = None,
               cutoff_mode: bool = False, cutoff: str = settings.DAY_CUTOFF,
               lookback_days: int = settings.PACK_LOOKBACK_DAYS):
    items = list_numbers(start=start, end=end, tz=tz, date_field=date_field,
                         states=states, exclude_canceled=exclude_canceled,
                         end_time=end_time, cutoff_mode=cutoff_mode,
                         cutoff=cutoff, lookback_days=lookback_days)
    return {"count": len(items), "items": items}

@router.get("/orders/ids.csv")
def orders_ids_csv(start: str, end: str, tz: str = settings.TZ,
                   date_field: str = "creationDate", states: Optional[str] = None,
                   exclude_canceled: bool = True, end_time: Optional[str] = None,
                   cutoff_mode: bool = False, cutoff: str = settings.DAY_CUTOFF,
                   lookback_days: int = settings.PACK_LOOKBACK_DAYS):
    data = orders_ids(start, end, tz, date_field, states, exclude_canceled, end_time, cutoff_mode, cutoff, lookback_days)  # type: ignore
    output = io.StringIO()
    w = csv.writer(output, lineterminator="\n")
    w.writerow(["number","state","date","amount","city","id"])
    for it in data["items"]:
        w.writerow([it["number"], it["state"], it["date"], it["amount"], it["city"], it["id"]])
    return PlainTextResponse(content=output.getvalue(), media_type="text/csv; charset=utf-8")
