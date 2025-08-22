
from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime
from typing import List, Dict, Any, Optional

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.utils.business_day import business_bucket_date, business_window_to_db_range
from app.models.store_settings import StoreSettings

# TODO: replace with your Order model import
# from app.models.order import Order
class Order:  # placeholder model for example only
    created_at: datetime

# TODO: Wire up your real DB session dependency
def get_db():
    raise RuntimeError("Replace get_db() with your project's DB session dependency")

router = APIRouter(prefix="/api/orders", tags=["orders"])

class DayCount(BaseModel):
    day: date
    count: int

class SummaryOut(BaseModel):
    business_day_start: str
    timezone: str
    rows: List[DayCount]


@router.get("/summary", response_model=SummaryOut)
def summary(
    start: date = Query(..., description="Business-day start date, inclusive"),
    end: date = Query(..., description="Business-day end date, inclusive"),
    business_day_start: Optional[str] = Query(None, description="HH:MM override; if omitted, uses persisted setting"),
    tz: Optional[str] = Query(None, description="Timezone override; if omitted, uses persisted setting"),
    db: Session = Depends(get_db),
):
    # 1) load persisted store settings (or defaults)
    settings = db.query(StoreSettings).order_by(StoreSettings.id.asc()).first()
    default_bds = settings.business_day_start if settings else "20:00"
    default_tz = settings.timezone if settings else "Asia/Almaty"

    bds = business_day_start or default_bds
    timezone = tz or default_tz

    # 2) convert business-day window to UTC fetch window
    start_utc, end_utc = business_window_to_db_range(start, end, bds, timezone)

    # 3) fetch orders that fall into the DB window
    # orders: List[Order] = (
    #     db.query(Order)
    #       .filter(Order.created_at >= start_utc, Order.created_at < end_utc)
    #       .all()
    # )

    # Placeholder: replace with actual fetch
    orders: List[Order] = []

    # 4) bucket by business-day (shift by offset then .date())
    bucket: Dict[date, int] = defaultdict(int)
    for o in orders:
        d = business_bucket_date(o.created_at, bds, timezone)
        bucket[d] += 1

    # 5) fill gaps (optional) + to rows
    rows = [DayCount(day=d, count=bucket.get(d, 0)) for d in daterange(start, end)]

    return SummaryOut(business_day_start=bds, timezone=timezone, rows=rows)


def daterange(d1: date, d2: date):
    cur = d1
    while cur <= d2:
        yield cur
        cur = date.fromordinal(cur.toordinal() + 1)
