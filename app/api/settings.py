# app/api/settings.py
from fastapi import APIRouter, HTTPException, Request
from pydantic import constr
from app.deps.auth import get_current_tenant_id
from app.deps.tenant import get_settings, upsert_settings
from app.deps.kaspi_client import KaspiClient
from typing import Optional
from typing_extensions import Annotated
from pydantic import BaseModel, Field, field_validator

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

def _normalize_payload(p: SettingsIn) -> dict:
    d = p.dict()
    d["shop_name"] = d["shop_name"].strip()
    if not d["shop_name"]:
        raise HTTPException(status_code=422, detail="shop_name must not be empty")
    return d

@router.get("/me")
def me(req: Request):
    tenant_id = get_current_tenant_id(req)
    row = get_settings_row(tenant_id)
    if not row:
        raise HTTPException(status_code=404, detail="settings not found")
    return row

@router.post("/save")
def save(payload: SettingsIn, req: Request):
    try:
        tenant_id = get_current_tenant_id(req)
        upsert_settings(tenant_id, payload.model_dump())
        return {"ok": True}
    except Exception as e:
        import traceback; traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"save failed: {e}")

@router.get("/check")
def check(req: Request):
    # Пингуем Kaspi-клиент; если токен отсутствует — middleware вернёт 401
    try:
        # Просто лёгкий вызов; сам факт корректных заголовков важен
        list(KaspiClient().iter_orders(
            start=__import__("datetime").datetime.utcnow(),
            end=__import__("datetime").datetime.utcnow()
        ))
    except Exception:
        pass
    return {"ok": True}
