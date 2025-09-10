from __future__ import annotations
import os, re, asyncio
from typing import Optional
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
import psycopg
import psycopg.rows
import httpx
from app.deps.auth import get_current_user

router = APIRouter(prefix="/settings", tags=["settings"])

# ──────────────────────────────────────────────────────────────────────────────
# PG URL: нормализуем под psycopg (убираем postgresql+psycopg, кавычки, +sslmode)
# ──────────────────────────────────────────────────────────────────────────────
def _normalize_pg_url(url: str) -> str:
    u = url.strip().strip('"').strip("'")
    u = re.sub(r"^postgresql\+[^:]+://", "postgresql://", u, flags=re.IGNORECASE)
    if "sslmode=" not in u:
        u += ("&" if "?" in u else "?") + "sslmode=require"
    return u

PG_URL = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL") or os.getenv("PROFIT_DB_URL")
if not PG_URL:
    raise RuntimeError("No Postgres URL found (SUPABASE_DB_URL / DATABASE_URL / PROFIT_DB_URL)")
PG_URL = _normalize_pg_url(PG_URL)

def _conn():
    return psycopg.connect(PG_URL, autocommit=True)

# ── DDL: создаём таблицу и FK/индексы, если ещё не созданы ────────────────────
DDL = """
create table if not exists public.tenant_settings (
  tenant_id text primary key,
  user_id uuid,
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
with _conn() as con:
    con.execute(DDL)
    con.execute("create index if not exists tenant_settings_user_id_idx on public.tenant_settings(user_id)")
    # FK на auth.users
    con.execute("""
    do $$
    begin
      if not exists (
        select 1 from pg_constraint
        where conrelid = 'public.tenant_settings'::regclass
          and conname  = 'tenant_settings_user_id_fkey'
      ) then
        alter table public.tenant_settings
          add constraint tenant_settings_user_id_fkey
          foreign key (user_id) references auth.users(id) on delete cascade;
      end if;
    end $$;
    """)

# ──────────────────────────────────────────────────────────────────────────────
# Модели
# ──────────────────────────────────────────────────────────────────────────────
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

def _row_to_out(r: dict) -> SettingsOut:
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
        updated_at=r["updated_at"].isoformat() if hasattr(r["updated_at"], "isoformat") else str(r["updated_at"]),
    )

# ──────────────────────────────────────────────────────────────────────────────
# API
# ──────────────────────────────────────────────────────────────────────────────
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
    return {"ok": True, "updated_at": datetime.utcnow().isoformat() + "Z"}

@router.get("/check")
async def check_settings(user = Depends(get_current_user)):
    # Берём токен и делаем минимальный запрос к Kaspi без привязки к конкретному «клиенту»
    with _conn() as con, con.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute("select kaspi_token from public.tenant_settings where tenant_id = %s", (user["tenant_id"],))
        row = cur.fetchone()
        if not row or not row["kaspi_token"]:
            raise HTTPException(status_code=404, detail="no kaspi_token")

    base = os.getenv("KASPI_BASE_URL", "https://kaspi.kz/shop/api/v2").rstrip("/")
    headers = {
        "X-Auth-Token": row["kaspi_token"],
        "Accept": "application/vnd.api+json",
        "Content-Type": "application/vnd.api+json",
        "User-Agent": "Mozilla/5.0",
    }
    params = {"page[number]":"0","page[size]":"1"}
    async with httpx.AsyncClient(timeout=httpx.Timeout(connect=10.0, read=20.0)) as cli:
        r = await cli.get(f"{base}/orders", headers=headers, params=params)
        if r.status_code in (200, 204):
            return {"ok": True}
        if r.status_code in (401, 403):
            return {"ok": False, "status": r.status_code}
        r.raise_for_status()
        return {"ok": True}
