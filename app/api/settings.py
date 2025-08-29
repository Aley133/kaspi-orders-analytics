# app/api/settings.py
from fastapi import APIRouter
import os
import re

router = APIRouter(prefix="/api/settings", tags=["settings"])

def _hhmm(v: str) -> str:
    v = (v or "").strip()
    return v if re.match(r"^\d{2}:\d{2}$", v) else "20:00"

@router.get("/store-hours")
def get_store_hours():
    return {
        "business_day_start": _hhmm(os.getenv("BUSINESS_DAY_START", "20:00")),
        "timezone": os.getenv("TZ", "Asia/Almaty"),
    }
