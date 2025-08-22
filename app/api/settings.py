
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.schemas.settings import StoreHoursIn, StoreHoursOut
from app.models.store_settings import StoreSettings

# TODO: Wire up your real DB session dependency
def get_db():
    raise RuntimeError("Replace get_db() with your project's DB session dependency")

router = APIRouter(prefix="/api/settings", tags=["settings"])

@router.get("/store-hours", response_model=StoreHoursOut)
def get_store_hours(db: Session = Depends(get_db)):
    settings = db.query(StoreSettings).order_by(StoreSettings.id.asc()).first()
    if not settings:
        # provide in-memory default if nothing persisted yet
        return StoreHoursOut(id=1, business_day_start="20:00", timezone="Asia/Almaty")
    return StoreHoursOut(id=settings.id, business_day_start=settings.business_day_start, timezone=settings.timezone)

@router.post("/store-hours", response_model=StoreHoursOut)
def set_store_hours(payload: StoreHoursIn, db: Session = Depends(get_db)):
    settings = db.query(StoreSettings).order_by(StoreSettings.id.asc()).first()
    if not settings:
        settings = StoreSettings(business_day_start=payload.business_day_start, timezone=payload.timezone)
        db.add(settings)
    else:
        settings.business_day_start = payload.business_day_start
        settings.timezone = payload.timezone
    db.commit()
    db.refresh(settings)
    return StoreHoursOut(id=settings.id, business_day_start=settings.business_day_start, timezone=settings.timezone)
