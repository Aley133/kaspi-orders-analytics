# app/api/settings.py
from __future__ import annotations
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, field_validator
from typing import Optional
import os
from datetime import time as dtime

from app.deps.tenant import require_tenant
from app import db

# ── (опц.) шифрование токена
_FERNET = None
try:
    from cryptography.fernet import Fernet
    if os.getenv("SETTINGS_CRYPT_KEY"):
        _FERNET = Fernet(os.getenv("SETTINGS_CRYPT_KEY"))
except Exception:
    _FERNET = None

def _enc(s: Optional[str]) -> Optional[str]:
    if not s: return s
    return _FERNET.encrypt(s.encode()).decode() if _FERNET else s

def _dec(s: Optional[str]) -> Optional[str]:
    if not s: return s
    return _FERNET.decrypt(s.encode()).decode() if _FERNET else s

router = APIRouter(prefix="/settings", tags=["settings"])

# ──────────────────────────────────────────────────────────────────────────────
# helpers: jsonb {"v": ...}
def _get_kv(tenant_id: str, key: str) -> Optional[str]:
    row = db.fetchrow("select value from tenant_settings where tenant_id=%s and key=%s",
                      [tenant_id, key])
    if not row: return None
    val = row["value"]
    # val — это dict (row_factory=dict_row), ожидаем {"v": "..."}
    return val.get("v") if isinstance(val, dict) else None

def _set_kv(tenant_id: str, key: str, value: Optional[str]) -> None:
    if value is None:
        return
    db.execute(
        "insert into tenant_settings(tenant_id,key,value) values (%s,%s, jsonb_build_object('v', %s)) "
        "on conflict (tenant_id,key) do update set value = excluded.value, updated_at = now()",
        [tenant_id, key, value]
    )

# ──────────────────────────────────────────────────────────────────────────────
# Schemas
class SettingsIn(BaseModel):
    partner_id: Optional[int] = None
    shop_name: Optional[str] = None
    kaspi_token: Optional[str] = None
    city_id: Optional[int] = None
    business_day_start: Optional[str] = None
    timezone: Optional[str] = None
    min_margin_pct: Optional[float] = None
    auto_reprice: Optional[bool] = None

    @field_validator("business_day_start")
    @classmethod
    def _validate_bds(cls, v: Optional[str]) -> Optional[str]:
        if not v: return v
        try:
            hh, mm = v.split(":")
            _ = dtime(hour=int(hh), minute=int(mm))
            return v
        except Exception:
            raise ValueError("business_day_start must be 'HH:MM'")

class SettingsOut(BaseModel):
    partner_id: Optional[int] = None
    shop_name: Optional[str] = None
    kaspi_token_masked: Optional[str] = None
    city_id: Optional[int] = None
    business_day_start: Optional[str] = None
    timezone: Optional[str] = None
    min_margin_pct: Optional[float] = None
    auto_reprice: Optional[bool] = None

def _mask(token: Optional[str]) -> Optional[str]:
    if not token: return None
    tail = token[-4:] if len(token) >= 4 else token
    return "••••" + tail

# ──────────────────────────────────────────────────────────────────────────────
@router.get("/me", response_model=SettingsOut)
def get_my_settings(tenant_id: str = Depends(require_tenant)):
    out = SettingsOut(
        partner_id = int(_get_kv(tenant_id, "kaspi.partner_id") or 0) or None,
        shop_name  = _get_kv(tenant_id, "shop.name"),
        kaspi_token_masked = _mask(_dec(_get_kv(tenant_id, "kaspi.token"))),
        city_id = int(_get_kv(tenant_id, "kaspi.city_id") or 0) or None,
        business_day_start = _get_kv(tenant_id, "bizday.start"),
        timezone = _get_kv(tenant_id, "tz.name") or "Asia/Almaty",
        min_margin_pct = (lambda v: float(v) if v is not None else None)(_get_kv(tenant_id, "price.min_margin")),
        auto_reprice = (lambda v: (v.lower()=='true') if isinstance(v, str) else None)(_get_kv(tenant_id, "price.auto_reprice")),
    )
    return out

@router.post("/me", response_model=SettingsOut)
def upsert_my_settings(payload: SettingsIn, tenant_id: str = Depends(require_tenant)):
    if payload.partner_id is not None:
        _set_kv(tenant_id, "kaspi.partner_id", str(payload.partner_id))
    if payload.shop_name is not None:
        _set_kv(tenant_id, "shop.name", payload.shop_name)
    if payload.kaspi_token:
        _set_kv(tenant_id, "kaspi.token", _enc(payload.kaspi_token))
    if payload.city_id is not None:
        _set_kv(tenant_id, "kaspi.city_id", str(payload.city_id))
    if payload.business_day_start is not None:
        _set_kv(tenant_id, "bizday.start", payload.business_day_start)
    if payload.timezone is not None:
        _set_kv(tenant_id, "tz.name", payload.timezone)
    if payload.min_margin_pct is not None:
        _set_kv(tenant_id, "price.min_margin", str(payload.min_margin_pct))
    if payload.auto_reprice is not None:
        _set_kv(tenant_id, "price.auto_reprice", "true" if payload.auto_reprice else "false")

    return get_my_settings(tenant_id)  # отдать актуальные значения
