
from __future__ import annotations
from pydantic import BaseModel, Field, validator

class StoreHoursIn(BaseModel):
    business_day_start: str = Field("20:00", description="HH:MM (local) when business day starts")
    timezone: str = Field("Asia/Almaty", description="IANA timezone, e.g. Asia/Almaty")

    @validator("business_day_start")
    def _validate_bds(cls, v: str) -> str:
        v = (v or "").strip()
        parts = v.split(":")
        if len(parts) != 2:
            raise ValueError("Format must be HH:MM")
        h, m = int(parts[0]), int(parts[1])
        if not (0 <= h <= 23 and 0 <= m <= 59):
            raise ValueError("Invalid HH:MM bounds")
        return f"{h:02d}:{m:02d}"


class StoreHoursOut(StoreHoursIn):
    id: int = 1
