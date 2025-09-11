# app/api/settings.py
from __future__ import annotations

from typing import Optional
from typing_extensions import Annotated

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field, field_validator

from app.deps.auth import get_current_tenant_id
from app.deps.tenant import get_settings, upsert_settings

router = APIRouter()
HHMM = Annotated[str, Field(pattern=r"^\d{2}:\d{2}$")]

class SettingsIn(BaseModel):
    shop_name: str = "LeoXpress"
    partner_id: Optional[str] = ""
    kaspi_token: str
    amount_fields: str = "totalPrice"
    amount_divisor: float = 1.0
    date_field_default: str = "creationDate"
    business_day_start: HHMM = "20:00"
    use_business_day: bool = True
    store_accept_until: HHMM = "17:00"
    city_keys: str = "city,deliveryAddress.city"
    allowed_origins: Optional[str] = ""

    @field_validator("amount_divisor")
    @classmethod
    def _divisor_positive(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("amount_divisor must be > 0")
        return v

@router.get("/me")
def me(req: Request):
    tenant_id = get_current_tenant_id(req)
    if not tenant_id:
        raise HTTPException(status_code=401, detail="unauthorized")
    row = get_settings(tenant_id)
    if row is None:
        raise HTTPException(status_code=404, detail="settings not found")
    return row

@router.post("/save")
def save(payload: SettingsIn, req: Request):
    tenant_id = get_current_tenant_id(req)
    if not tenant_id:
        raise HTTPException(status_code=401, detail="unauthorized")
    try:
        upsert_settings(tenant_id, payload.model_dump())
        return {"ok": True}
    except Exception as e:
        # покажем реальную причину — тебе сейчас важно это видеть
        raise HTTPException(status_code=500, detail=f"save failed: {e}")

@router.get("/check")
def check(req: Request):
    """
    Лёгкая проверка: если мидлвара пустила (есть bearer) — отвечаем OK.
    Токен Kaspi реально проверяется уже при вызове заказов.
    """
    if not get_current_tenant_id(req):
        raise HTTPException(status_code=401, detail="unauthorized")
    return {"ok": True}
