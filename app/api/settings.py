from __future__ import annotations
import os
from typing import Optional, Dict, Any
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
import psycopg
from app.deps.auth import get_current_user
from app.kaspi_client import KaspiClient

router = APIRouter(prefix="/settings", tags=["settings"])

DB_URL = os.getenv("SUPABASE_DB_URL")
if not DB_URL:
    raise RuntimeError("SUPABASE_DB_URL is not set")

DDL = """
create table if not exists public.tenant_settings (
  tenant_id text primary key,
  user_id uuid not null references auth.users(id) on delete cascade,
  shop_name text not null default 'LeoXpress',
  partner_id text not null default '',
  kaspi_token text not null default '',
  amount_fields text not null default 'totalPrice',
  amount_divisor double precision not null default 1,
  date_field_default text not null default 'creationDate',
  business_day_start text not null default '20:00',
  use_business_day boolean not null default true,
  store_accept_until text not null default '17:00',
  city_keys text not null default 'city,deliveryAddress.city',
  allowed_origins text not null default '',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);
"""

def _conn():
    return psycopg.connect(DB_URL, autocommit=True)

with _conn() as con:
    con.execute(DDL)

class SettingsIn(BaseModel):
    shop_name: str = Field(default="LeoXpress")
    partner_id: str = Field(default="")
    kaspi_token: str = Field(default="")
    amount_fields: str = Field(default="totalPrice")
    amount_divisor: float = Field(default=1.0)
    date_field_default: str = Field(default="creationDate")
    business_day_start: str = Field(default="20:00")
    use_business_day: bool = Field(default=True)
    store_accept_until: str = Field(default="17:00")
    city_keys: str = Field(default="city,deliveryAddress.city")
    allowed_origins: str = Field(default="")

class SettingsOut(SettingsIn):
    tenant_id: str
    user_id: str
    updated_at: str

def _row_to_out(r) -> SettingsOut:
    return SettingsOut(
        tenant_id=r["tenant_id"],
        user_id=str(r["user_id"]),
        shop_name=r["shop_name"],
        partner_id=r["partner_id"],
        kaspi_token=r["kaspi_token"],
        amount_fields=r["amount_fields"],
        amount_divisor=float(r["amount_divisor"]),
        date_field_default=r["date_field_default"],
        business_day_start=r["business_day_start"],
        use_business_day=bool(r["use_business_day"]),
        store_accept_until=r["store_accept_until"],
        city_keys=r["city_keys"],
        allowed_origins=r["allowed_origins"],
        updated_at=r["updated_at"].isoformat() if hasattr(r["updated_at"], 'isoformat') else str(r["updated_at"]),
    )

@router.get("/me", response_model=SettingsOut)
def get_me(user = Depends(get_current_user)):
    with _conn() as con, con.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute("select * from public.tenant_settings where tenant_id = %s", (user["tenant_id"],))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="settings not initialized")
        return _row_to_out(row)

@router.post("/save")
def save_settings(payload: SettingsIn, user = Depends(get_current_user)):
    now = datetime.utcnow().isoformat() + "Z"
    with _conn() as con, con.cursor() as cur:
        cur.execute("""
        insert into public.tenant_settings(
          tenant_id, user_id, shop_name, partner_id, kaspi_token, amount_fields, amount_divisor,
          date_field_default, business_day_start, use_business_day, store_accept_until,
          city_keys, allowed_origins, updated_at
        ) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now())
        on conflict (tenant_id) do update set
          shop_name=excluded.shop_name,
          partner_id=excluded.partner_id,
          kaspi_token=excluded.kaspi_token,
          amount_fields=excluded.amount_fields,
          amount_divisor=excluded.amount_divisor,
          date_field_default=excluded.date_field_default,
          business_day_start=excluded.business_day_start,
          use_business_day=excluded.use_business_day,
          store_accept_until=excluded.store_accept_until,
          city_keys=excluded.city_keys,
          allowed_origins=excluded.allowed_origins,
          updated_at=now();
        """, (
            user["tenant_id"], user["user_id"],
            payload.shop_name, payload.partner_id, payload.kaspi_token,
            payload.amount_fields, payload.amount_divisor,
            payload.date_field_default, payload.business_day_start, payload.use_business_day,
            payload.store_accept_until, payload.city_keys, payload.allowed_origins
        ))
    return {"ok": True, "updated_at": now}

@router.get("/check")
async def check_settings(user = Depends(get_current_user)):
    # Берём настройки и валидируем токен запросом к Kaspi
    with _conn() as con, con.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute("select kaspi_token from public.tenant_settings where tenant_id = %s", (user["tenant_id"],))
        row = cur.fetchone()
        if not row or not row["kaspi_token"]:
            raise HTTPException(status_code=404, detail="no kaspi_token")
        cli = KaspiClient(token=row["kaspi_token"])
        ok = await cli.verify_token()
        return {"ok": bool(ok)}
