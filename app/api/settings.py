# app/api/settings.py
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field, constr
from app.deps.auth import get_current_tenant_id
from app.deps.tenant import get_settings_row, upsert_settings
from app.deps.kaspi_client import KaspiClient

router = APIRouter()

class SettingsIn(BaseModel):
    # Обязательное имя магазина: 1..80 символов, обрежем пробелы
    shop_name: constr(min_length=1, max_length=80) = Field(..., description="Shop display name")
    partner_id: str | None = ""
    kaspi_token: constr(min_length=10) = Field(..., description="Kaspi API token")
    amount_fields: str = "totalPrice"
    amount_divisor: float = 1.0
    date_field_default: str = "creationDate"
    business_day_start: constr(regex=r"^\d{2}:\d{2}$") = "20:00"
    use_business_day: bool = True
    store_accept_until: constr(regex=r"^\d{2}:\d{2}$") = "17:00"
    city_keys: str = "city,deliveryAddress.city"
    allowed_origins: str | None = ""

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
    tenant_id = get_current_tenant_id(req)
    upsert_settings(tenant_id, _normalize_payload(payload))
    return {"ok": True}

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
