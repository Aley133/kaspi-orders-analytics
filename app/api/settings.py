from __future__ import annotations
import os, json, sqlite3, contextlib
from typing import Optional, Dict, Any
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from app.deps.tenant import require_tenant

router = APIRouter(prefix="/settings", tags=["settings"])

# ── Хранилище: SQLite файл (стабилен на Render диске) ─────────────────────────
DATA_DIR = os.getenv("DATA_DIR", "data")
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(DATA_DIR, "settings.sqlite")

def _conn():
    con = sqlite3.connect(DB_PATH, timeout=30)
    con.row_factory = sqlite3.Row
    return con

def _init():
    with contextlib.closing(_conn()) as con, con:
        con.execute("""
        CREATE TABLE IF NOT EXISTS tenant_settings(
            tenant_id TEXT PRIMARY KEY,
            shop_name TEXT,
            partner_id TEXT,
            kaspi_token TEXT,
            amount_fields TEXT,
            amount_divisor REAL,
            date_field_default TEXT,
            business_day_start TEXT,
            use_business_day INTEGER,
            store_accept_until TEXT,
            city_keys TEXT,
            allowed_origins TEXT,
            updated_at TEXT
        )
        """)

_init()

# ── Модели ────────────────────────────────────────────────────────────────────
class SettingsIn(BaseModel):
    shop_name: str = Field(default="LeoXpress")
    partner_id: str = Field(default="")
    kaspi_token: str = Field(default="")
    amount_fields: str = Field(default="totalPrice")  # CSV
    amount_divisor: float = Field(default=1.0)
    date_field_default: str = Field(default="creationDate")
    business_day_start: str = Field(default="20:00")
    use_business_day: bool = Field(default=True)
    store_accept_until: str = Field(default="17:00")
    city_keys: str = Field(default="city,deliveryAddress.city")  # CSV
    allowed_origins: str = Field(default="")  # CSV

class SettingsOut(SettingsIn):
    updated_at: str

def _row_to_out(r: sqlite3.Row) -> SettingsOut:
    return SettingsOut(
        shop_name      = r["shop_name"] or "LeoXpress",
        partner_id     = r["partner_id"] or "",
        kaspi_token    = r["kaspi_token"] or "",
        amount_fields  = r["amount_fields"] or "totalPrice",
        amount_divisor = float(r["amount_divisor"] or 1.0),
        date_field_default = r["date_field_default"] or "creationDate",
        business_day_start = r["business_day_start"] or "20:00",
        use_business_day   = bool(r["use_business_day"] or 0),
        store_accept_until = r["store_accept_until"] or "17:00",
        city_keys          = r["city_keys"] or "city,deliveryAddress.city",
        allowed_origins    = r["allowed_origins"] or "",
        updated_at    = r["updated_at"] or datetime.utcnow().isoformat() + "Z",
    )

# ── API ───────────────────────────────────────────────────────────────────────
@router.get("/me", response_model=SettingsOut)
def get_me(tenant_id: str = Depends(require_tenant)):
    with contextlib.closing(_conn()) as con:
        cur = con.execute("SELECT * FROM tenant_settings WHERE tenant_id = ?", (tenant_id,))
        r = cur.fetchone()
        if not r:
            raise HTTPException(status_code=404, detail="settings not initialized")
        return _row_to_out(r)

@router.post("/save")
def save_settings(payload: SettingsIn, tenant_id: str = Depends(require_tenant)):
    now = datetime.utcnow().isoformat() + "Z"
    with contextlib.closing(_conn()) as con, con:
        con.execute("""
        INSERT INTO tenant_settings(
            tenant_id, shop_name, partner_id, kaspi_token, amount_fields, amount_divisor,
            date_field_default, business_day_start, use_business_day, store_accept_until,
            city_keys, allowed_origins, updated_at
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(tenant_id) DO UPDATE SET
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
            updated_at=excluded.updated_at
        """, (
            tenant_id,
            payload.shop_name, payload.partner_id, payload.kaspi_token,
            payload.amount_fields, payload.amount_divisor,
            payload.date_field_default, payload.business_day_start, int(payload.use_business_day),
            payload.store_accept_until, payload.city_keys, payload.allowed_origins,
            now
        ))
    return {"ok": True, "updated_at": now}
