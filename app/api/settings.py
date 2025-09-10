from __future__ import annotations
import os, re, json
from typing import Optional, Dict, Any
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
import psycopg, psycopg.rows
import httpx
from app.deps.auth import get_current_user

router = APIRouter(prefix="/settings", tags=["settings"])

def _normalize_pg_url(url: str) -> str:
    u = (url or "").strip().strip('"').strip("'")
    u = re.sub(r"^postgresql\+[^:]+://", "postgresql://", u, flags=re.IGNORECASE)
    if u and "sslmode=" not in u:
        u += ("&" if "?" in u else "?") + "sslmode=require"
    return u

PG_URL = _normalize_pg_url(os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL") or os.getenv("PROFIT_DB_URL") or "")
if not PG_URL:
    raise RuntimeError("No Postgres URL for settings")

def _conn():
    return psycopg.connect(PG_URL, autocommit=True)

# ключ в KV
SETTINGS_KEY = "settings"

# дефолты
DEFAULTS = {
    "shop_name": "LeoXpress",
    "partner_id": "",
    "kaspi_token": "",
    "amount_fields": "totalPrice",
    "amount_divisor": 1.0,
    "date_field_default": "creationDate",
    "business_day_start": "20:00",
    "use_business_day": True,
    "store_accept_until": "17:00",
    "city_keys": "city,deliveryAddress.city",
    "allowed_origins": "",
}

class SettingsIn(BaseModel):
    shop_name: str = Field(default=DEFAULTS["shop_name"])
    partner_id: str = Field(default=DEFAULTS["partner_id"])
    kaspi_token: str = Field(default=DEFAULTS["kaspi_token"])
    amount_fields: str = Field(default=DEFAULTS["amount_fields"])
    amount_divisor: float = Field(default=DEFAULTS["amount_divisor"])
    date_field_default: str = Field(default=DEFAULTS["date_field_default"])
    business_day_start: str = Field(default=DEFAULTS["business_day_start"])
    use_business_day: bool = Field(default=DEFAULTS["use_business_day"])
    store_accept_until: str = Field(default=DEFAULTS["store_accept_until"])
    city_keys: str = Field(default=DEFAULTS["city_keys"])
    allowed_origins: str = Field(default=DEFAULTS["allowed_origins"])

class SettingsOut(SettingsIn):
    tenant_id: str
    user_id: str
    updated_at: str

def _merge_defaults(v: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(DEFAULTS)
    out.update({k: v[k] for k in v.keys() if k in DEFAULTS})
    return out

@router.get("/me", response_model=SettingsOut)
def get_me(user = Depends(get_current_user)):
    with _conn() as con, con.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute("""
          select value, user_id from public.tenant_settings
          where tenant_id = %s and key = %s
          limit 1
        """, (user["tenant_id"], SETTINGS_KEY))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="settings not initialized")
        val = row["value"] if isinstance(row["value"], dict) else json.loads(row["value"])
        merged = _merge_defaults(val or {})
        return SettingsOut(tenant_id=user["tenant_id"], user_id=str(row["user_id"] or user["user_id"]),
                           updated_at=datetime.utcnow().isoformat()+"Z", **merged)

@router.post("/save")
def save_settings(payload: SettingsIn, user = Depends(get_current_user)):
    data = payload.dict()
    with _conn() as con, con.cursor() as cur:
        cur.execute("""
          insert into public.tenant_settings(tenant_id, user_id, key, value)
          values (%s, %s, %s, %s::jsonb)
          on conflict (tenant_id, key) do update set
            value = EXCLUDED.value
        """, (user["tenant_id"], user["user_id"], SETTINGS_KEY, json.dumps(data)))
    return {"ok": True, "updated_at": datetime.utcnow().isoformat() + "Z"}

@router.get("/check")
async def check_settings(user = Depends(get_current_user)):
    with _conn() as con, con.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute("""
          select (value->>'kaspi_token') as kaspi_token
          from public.tenant_settings
          where tenant_id = %s and key = %s
          limit 1
        """, (user["tenant_id"], SETTINGS_KEY))
        row = cur.fetchone()
        if not row or not row["kaspi_token"]:
            raise HTTPException(status_code=404, detail="no kaspi_token")
        token = row["kaspi_token"]

    base = os.getenv("KASPI_BASE_URL", "https://kaspi.kz/shop/api/v2").rstrip("/")
    headers = {
        "X-Auth-Token": token,
        "Accept": "application/vnd.api+json",
        "Content-Type": "application/vnd.api+json",
        "User-Agent": "Mozilla/5.0",
    }
    params = {"page[number]":"0","page[size]":"1"}
    async with httpx.AsyncClient(timeout=httpx.Timeout(connect=10.0, read=20.0)) as cli:
        r = await cli.get(f"{base}/orders", headers=headers, params=params)
        if r.status_code in (200, 204): return {"ok": True}
        if r.status_code in (401, 403): return {"ok": False, "status": r.status_code}
        r.raise_for_status()
        return {"ok": True}
