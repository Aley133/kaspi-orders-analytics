from fastapi import APIRouter, Query
from fastapi.responses import StreamingResponse
from typing import Optional
import io, csv
from ..core.config import settings
from ..services.analytics import (
    get_meta,
    fetch_analytics,
    fetch_order_ids
)

api_router = APIRouter()

@api_router.get("/meta")
async def meta():
    return get_meta()

@api_router.get("/analytics")
async def analytics(
    start: str = Query(..., description="YYYY-MM-DD"),
    end: str = Query(..., description="YYYY-MM-DD"),
    tz: str = Query(default=settings.TZ),
    date_field: str = Query(default=settings.DATE_FIELD_DEFAULT),
    states: Optional[str] = Query(default=None, description="CSV of states to include"),
    exclude_states: Optional[str] = Query(default=None, description="CSV of states to exclude"),
    exclude_canceled: bool = Query(default=True),
    start_time: Optional[str] = Query(default=None, description="HH:MM"),
    end_time: Optional[str] = Query(default=None, description="HH:MM"),
    with_prev: bool = Query(default=False),
    use_cutoff_window: bool = Query(default=False),
    lte_cutoff_only: bool = Query(default=False),
    lookback_days: int = Query(default=settings.PACK_LOOKBACK_DAYS),
):
    return await fetch_analytics(
        start=start,
        end=end,
        tz=tz,
        date_field=date_field,
        states=states,
        exclude_states=exclude_states,
        exclude_canceled=exclude_canceled,
        start_time=start_time,
        end_time=end_time,
        with_prev=with_prev,
        use_cutoff_window=use_cutoff_window,
        lte_cutoff_only=lte_cutoff_only,
        lookback_days=lookback_days,
    )

@api_router.get("/orders/ids")
async def order_ids(
    start: str = Query(..., description="YYYY-MM-DD"),
    end: str = Query(..., description="YYYY-MM-DD"),
    tz: str = Query(default=settings.TZ),
    date_field: str = Query(default=settings.DATE_FIELD_DEFAULT),
    states: Optional[str] = Query(default=None),
    exclude_states: Optional[str] = Query(default=None),
    exclude_canceled: bool = Query(default=True),
    use_cutoff_window: bool = Query(default=False),
    lte_cutoff_only: bool = Query(default=False),
    lookback_days: int = Query(default=settings.PACK_LOOKBACK_DAYS),
    limit: int = Query(default=10000),
):
    items = await fetch_order_ids(
        start=start,
        end=end,
        tz=tz,
        date_field=date_field,
        states=states,
        exclude_states=exclude_states,
        exclude_canceled=exclude_canceled,
        use_cutoff_window=use_cutoff_window,
        lte_cutoff_only=lte_cutoff_only,
        lookback_days=lookback_days,
        limit=limit,
    )
    return {"count": len(items), "items": items}

@api_router.get("/orders/ids.csv")
async def order_ids_csv(
    start: str = Query(..., description="YYYY-MM-DD"),
    end: str = Query(..., description="YYYY-MM-DD"),
    tz: str = Query(default=settings.TZ),
    date_field: str = Query(default=settings.DATE_FIELD_DEFAULT),
    states: Optional[str] = Query(default=None),
    exclude_states: Optional[str] = Query(default=None),
    exclude_canceled: bool = Query(default=True),
    use_cutoff_window: bool = Query(default=False),
    lte_cutoff_only: bool = Query(default=False),
    lookback_days: int = Query(default=settings.PACK_LOOKBACK_DAYS),
    limit: int = Query(default=10000),
):
    items = await fetch_order_ids(
        start=start,
        end=end,
        tz=tz,
        date_field=date_field,
        states=states,
        exclude_states=exclude_states,
        exclude_canceled=exclude_canceled,
        use_cutoff_window=use_cutoff_window,
        lte_cutoff_only=lte_cutoff_only,
        lookback_days=lookback_days,
        limit=limit,
    )

    # Build CSV
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=["id", "number", "state", "date", "amount", "city"])
    writer.writeheader()
    for it in items:
        writer.writerow(it)
    data = buf.getvalue().encode("utf-8-sig")

    return StreamingResponse(io.BytesIO(data), media_type="text/csv", headers={
        "Content-Disposition": "attachment; filename=order_ids.csv"
    })
