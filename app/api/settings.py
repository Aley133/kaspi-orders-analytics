# app/api/settings.py
from __future__ import annotations
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, field_validator
from typing import Optional
import os
from datetime import time as dtime
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from jose import jwt, JWTError

router = APIRouter(prefix="/settings", tags=["settings"])

# ──────────────────────────────────────────────────────────────────────────────
# DB
# ──────────────────────────────────────────────────────────────────────────────
def _normalize_db_url(url: str | None) -> str:
    if not url:
        raise RuntimeError("DATABASE_URL is not set")
    # .env может содержать URL в кавычках
    return url.strip().strip('"').strip("'")

ENGINE: Engine = create_engine(
    _normalize_db_url(os.getenv("DATABASE_URL")),
    pool_pre_ping=True,
    future=True,
)

def init_schema() -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS user_settings (
        user_id           TEXT PRIMARY KEY,
        created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
        partner_id        BIGINT,
        shop_name         TEXT,
        kaspi_token       TEXT,   -- для простоты: хранение как есть (см. заметку об шифровании ниже)
        city_id           BIGINT,
        business_day_start TIME,
        timezone          TEXT DEFAULT 'Asia/Almaty',
        min_margin_pct    NUMERIC(8,2),
        auto_reprice      BOOLEAN DEFAULT FALSE
    );
    """
    trg = """
    CREATE OR REPLACE FUNCTION set_updated_at()
    RETURNS TRIGGER AS $$
    BEGIN
      NEW.updated_at = now();
      RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;

    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM pg_trigger WHERE tgname = 'trg_user_settings_updated_at'
      ) THEN
        CREATE TRIGGER trg_user_settings_updated_at
        BEFORE UPDATE ON user_settings
        FOR EACH ROW EXECUTE FUNCTION set_updated_at();
      END IF;
    END$$;
    """
    with ENGINE.begin() as conn:
        conn.exec_driver_sql(ddl)
        conn.exec_driver_sql(trg)

# ──────────────────────────────────────────────────────────────────────────────
# Auth: проверяем Supabase JWT (HS256) и достаём user_id (sub)
# ──────────────────────────────────────────────────────────────────────────────
SUPABASE_JWT_SECRET = os.getenv("SUPABASE_JWT_SECRET")
if not SUPABASE_JWT_SECRET:
    # Не падаем при импорте, но объясняем при первом запросе:
    SUPABASE_JWT_SECRET = ""

def get_current_user_id(req: Request) -> str:
    auth = req.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Bearer token")
    token = auth[7:]
    if not SUPABASE_JWT_SECRET:
        raise HTTPException(status_code=500, detail="SUPABASE_JWT_SECRET not configured")
    try:
        payload = jwt.decode(token, SUPABASE_JWT_SECRET, algorithms=["HS256"], options={"verify_aud": False})
        uid = payload.get("sub") or payload.get("user_id")
        if not uid:
            raise HTTPException(status_code=401, detail="JWT has no sub")
        return uid
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid JWT")

# ──────────────────────────────────────────────────────────────────────────────
# Schemas
# ──────────────────────────────────────────────────────────────────────────────
class SettingsIn(BaseModel):
    partner_id: Optional[int] = None
    shop_name: Optional[str] = None
    kaspi_token: Optional[str] = None  # передавай пустую строку/None, чтобы не менять
    city_id: Optional[int] = None
    business_day_start: Optional[str] = None  # '20:00'
    timezone: Optional[str] = None           # 'Asia/Almaty'
    min_margin_pct: Optional[float] = None
    auto_reprice: Optional[bool] = None

    @field_validator("business_day_start")
    @classmethod
    def _validate_bds(cls, v: Optional[str]) -> Optional[str]:
        if v is None or v == "":
            return v
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
    if not token:
        return None
    tail = token[-4:] if len(token) >= 4 else token
    return "••••" + tail

# ──────────────────────────────────────────────────────────────────────────────
# Routes
# ──────────────────────────────────────────────────────────────────────────────
@router.on_event("startup")
def _startup():
    init_schema()

@router.get("/me", response_model=SettingsOut)
def get_my_settings(user_id: str = Depends(get_current_user_id)):
    sql = """
    SELECT partner_id, shop_name, kaspi_token, city_id,
           to_char(business_day_start, 'HH24:MI') AS business_day_start,
           timezone, min_margin_pct, auto_reprice
    FROM user_settings WHERE user_id = :uid
    """
    with ENGINE.begin() as conn:
        row = conn.execute(text(sql), {"uid": user_id}).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="No settings yet")
    return SettingsOut(
        partner_id=row["partner_id"],
        shop_name=row["shop_name"],
        kaspi_token_masked=_mask(row["kaspi_token"]),
        city_id=row["city_id"],
        business_day_start=row["business_day_start"],
        timezone=row["timezone"],
        min_margin_pct=float(row["min_margin_pct"]) if row["min_margin_pct"] is not None else None,
        auto_reprice=bool(row["auto_reprice"]) if row["auto_reprice"] is not None else None,
    )

@router.post("/me", response_model=SettingsOut)
def upsert_my_settings(payload: SettingsIn, user_id: str = Depends(get_current_user_id)):
    # приводим business_day_start к TIME
    bds_sql = None
    if payload.business_day_start:
        bds_sql = "MAKE_TIME(split_part(:bds, ':', 1)::int, split_part(:bds, ':', 2)::int, 0)"

    # upsert
    sql = f"""
    INSERT INTO user_settings (user_id, partner_id, shop_name, kaspi_token, city_id, business_day_start, timezone, min_margin_pct, auto_reprice)
    VALUES (:uid, :partner_id, :shop_name, :kaspi_token, :city_id, {bds_sql or 'NULL'}, :timezone, :min_margin_pct, :auto_reprice)
    ON CONFLICT (user_id) DO UPDATE SET
        partner_id = COALESCE(EXCLUDED.partner_id, user_settings.partner_id),
        shop_name  = COALESCE(EXCLUDED.shop_name,  user_settings.shop_name),
        kaspi_token= COALESCE(EXCLUDED.kaspi_token, user_settings.kaspi_token),
        city_id    = COALESCE(EXCLUDED.city_id,    user_settings.city_id),
        business_day_start = COALESCE(EXCLUDED.business_day_start, user_settings.business_day_start),
        timezone   = COALESCE(EXCLUDED.timezone,   user_settings.timezone),
        min_margin_pct = COALESCE(EXCLUDED.min_margin_pct, user_settings.min_margin_pct),
        auto_reprice   = COALESCE(EXCLUDED.auto_reprice,   user_settings.auto_reprice)
    RETURNING partner_id, shop_name, kaspi_token,
              to_char(business_day_start, 'HH24:MI') AS business_day_start,
              city_id, timezone, min_margin_pct, auto_reprice
    """
    params = {
        "uid": user_id,
        "partner_id": payload.partner_id,
        "shop_name": payload.shop_name,
        "kaspi_token": (payload.kaspi_token or None),
        "city_id": payload.city_id,
        "bds": payload.business_day_start,
        "timezone": payload.timezone,
        "min_margin_pct": payload.min_margin_pct,
        "auto_reprice": payload.auto_reprice,
    }
    with ENGINE.begin() as conn:
        row = conn.execute(text(sql), params).mappings().first()

    return SettingsOut(
        partner_id=row["partner_id"],
        shop_name=row["shop_name"],
        kaspi_token_masked=_mask(row["kaspi_token"]),
        city_id=row["city_id"],
        business_day_start=row["business_day_start"],
        timezone=row["timezone"],
        min_margin_pct=float(row["min_margin_pct"]) if row["min_margin_pct"] is not None else None,
        auto_reprice=bool(row["auto_reprice"]) if row["auto_reprice"] is not None else None,
    )
