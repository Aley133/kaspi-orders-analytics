from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Body, Query
from fastapi.responses import PlainTextResponse

from ..core.config import settings
from ..services.analytics import analytics_payload, list_numbers
from ..services.profit_store import (
    get_config as profit_get_config,
    set_config as profit_set_config,
    set_cost as profit_set_cost,
    compute_profit_for_range,
)
from ..services.inventory import (
    add_batch, Batch, get_stock, set_threshold, reset_sales_cache, apply_sales_agg
)
from ..services.catalog import sync_products, list_catalog, overview

router = APIRouter()


# -------------------- PROFIT --------------------
@router.get("/profit/config")
def profit_config_get():
    return profit_get_config()

@router.post("/profit/config")
def profit_config_set(payload: dict = Body(...)):
    profit_set_config({
        "commission_percent": float(payload.get("commission_percent", 0)),
        "acquiring_percent": float(payload.get("acquiring_percent", 0)),
        "delivery_fixed": float(payload.get("delivery_fixed", 0)),
        "other_fixed": float(payload.get("other_fixed", 0)),
    })
    return {"ok": True}

@router.post("/profit/cost")
def profit_set_order_cost(payload: dict = Body(...)):
    profit_set_cost(str(payload["number"]), float(payload.get("cost", 0)), payload.get("note"))
    return {"ok": True}

@router.get("/profit/orders")
def profit_orders(start: str, end: str, tz: str = settings.TZ,
                  date_field: str = "creationDate", states: Optional[str] = None,
                  exclude_canceled: bool = True, end_time: Optional[str] = None,
                  cutoff_mode: bool = False, cutoff: str = settings.DAY_CUTOFF,
                  lookback_days: int = settings.PACK_LOOKBACK_DAYS):
    return compute_profit_for_range(
        start=start, end=end, tz=tz, date_field=date_field, states=states,
        exclude_canceled=exclude_canceled, end_time=end_time,
        cutoff_mode=cutoff_mode, cutoff=cutoff, lookback_days=lookback_days
    )


# -------------------- INVENTORY --------------------
@router.post("/inventory/batch")
def inventory_add_batch(payload: dict = Body(...)):
    b = Batch(
        product_code=payload["product_code"],
        product_name=payload.get("product_name") or "",
        received_at=payload["received_at"],
        unit_cost=float(payload["unit_cost"]),
        qty_in=int(payload["qty_in"]),
        note=payload.get("note"),
    )
    bid = add_batch(b)
    return {"ok": True, "batch_id": bid}

@router.get("/inventory/stock")
def inventory_stock():
    return get_stock()

@router.post("/inventory/threshold")
def inventory_threshold(payload: dict = Body(...)):
    set_threshold(payload["product_code"], int(payload.get("threshold") or 0), payload.get("preferred_name"))
    return {"ok": True}

@router.post("/inventory/recalc")
def inventory_recalc(lookback_days: int = Query(35, ge=1, le=365)):
    # Временная заглушка пересчёта продаж (FIFO). Позже подставим реальный агрегат.
    reset_sales_cache()
    apply_sales_agg({})
    return {"ok": True}


# -------------------- CATALOG --------------------
@router.post("/catalog/sync")
def catalog_sync():
    return sync_products()

@router.get("/catalog/list")
def catalog_list():
    return list_catalog()

@router.get("/catalog/overview")
def catalog_overview():
    return overview()


# -------------------- BASE --------------------
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
