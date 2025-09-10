# app/api/settings.py
from __future__ import annotations
from typing import Optional, Any, Dict, List
from fastapi import APIRouter, Depends, HTTPException, Body
from pydantic import BaseModel

from app.deps.auth import get_current_user            # Supabase-JWT -> dict/claim
from app.deps.tenant import require_tenant_optional   # ВОЗВРАЩАЕТ UID ПОЛЬЗОВАТЕЛЯ (или None)
from app import db

router = APIRouter(prefix="/settings", tags=["settings"])

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
def _to_uid(user: Any) -> Optional[str]:
    if not user:
        return None
    if isinstance(user, dict):
        for k in ("sub", "id", "uid", "user_id"):
            v = user.get(k)
            if v:
                return str(v)
        inner = user.get("user")
        if isinstance(inner, dict):
            for k in ("sub", "id", "uid", "user_id"):
                v = inner.get(k)
                if v:
                    return str(v)
        return None
    return str(user)

def _mask_token(tok: Optional[str]) -> Optional[str]:
    if not tok:
        return None
    s = str(tok)
    if len(s) <= 8:
        return "*" * len(s)
    return s[:4] + "*" * (len(s) - 8) + s[-4:]

def _to_int(x: Any) -> Optional[int]:
    try:
        return int(x) if x is not None and str(x).strip() != "" else None
    except Exception:
        return None

def _to_float(x: Any) -> Optional[float]:
    try:
        return float(x) if x is not None and str(x).strip() != "" else None
    except Exception:
        return None

def _parse_bool(x: Any) -> Optional[bool]:
    if x is None or x == "":
        return None
    if isinstance(x, bool):
        return x
    sx = str(x).strip().lower()
    if sx in ("1", "true", "yes", "on"):  return True
    if sx in ("0", "false", "no", "off"): return False
    return None

# ──────────────────────────────────────────────────────────────────────────────
# ORG / TENANT helpers
# ──────────────────────────────────────────────────────────────────────────────
def _find_tenant_for_user(user_id: str) -> Optional[str]:
    row = db.fetchrow(
        "select tenant_id from public.org_members where user_id=%s limit 1",
        [user_id],
    )
    return row["tenant_id"] if row and row.get("tenant_id") else None

def _ensure_tenant_for_user(user_id: str) -> str:
    t_id = _find_tenant_for_user(user_id)
    if t_id:
        return t_id
    t = db.fetchrow("insert into public.tenants default values returning id", [])
    tenant_id = t["id"]
    db.execute(
        "insert into public.org_members(tenant_id, user_id, role) values (%s, %s, %s)",
        [tenant_id, user_id, "owner"],
    )
    return tenant_id

# ──────────────────────────────────────────────────────────────────────────────
# SETTINGS (под плоскую таблицу public.tenant_settings)
# ──────────────────────────────────────────────────────────────────────────────
SETTINGS_COLS_ORDER: List[str] = [
    "shop_name",
    "partner_id",
    "kaspi_token",
    "city_id",
    "business_day_start",
    "timezone",
    "min_margin_pct",
    "auto_reprice",
]

def _select_settings_row(tenant_id: str) -> Optional[Dict[str, Any]]:
    return db.fetchrow(
        """
        select
            tenant_id,
            shop_name,
            partner_id,
            kaspi_token,
            city_id,
            business_day_start,
            timezone,
            min_margin_pct,
            auto_reprice,
            updated_at
        from public.tenant_settings
        where tenant_id=%s
        """,
        [tenant_id],
    )

def _insert_settings(tenant_id: str, data: Dict[str, Any]) -> None:
    cols = ["tenant_id"]
    vals = [tenant_id]
    placeholders = ["%s"]

    for col in SETTINGS_COLS_ORDER:
        if col in data and data[col] is not None:
            cols.append(col)
            vals.append(data[col])
            placeholders.append("%s")

    db.execute(
        f"insert into public.tenant_settings ({', '.join(cols)}) values ({', '.join(placeholders)})",
        vals,
    )

def _update_settings(tenant_id: str, data: Dict[str, Any]) -> None:
    # Обновляем только переданные (не None) поля
    sets = []
    vals = []
    for col in SETTINGS_COLS_ORDER:
        if col in data and data[col] is not None:
            sets.append(f"{col} = %s")
            vals.append(data[col])

    if not sets:
        return

    sets.append("updated_at = now()")
    vals.append(tenant_id)
    db.execute(
        f"update public.tenant_settings set {', '.join(sets)} where tenant_id = %s",
        vals,
    )

def _upsert_settings(tenant_id: str, data: Dict[str, Any]) -> None:
    exists = _select_settings_row(tenant_id) is not None
    if exists:
        _update_settings(tenant_id, data)
    else:
        _insert_settings(tenant_id, data)

# ──────────────────────────────────────────────────────────────────────────────
# API
# ──────────────────────────────────────────────────────────────────────────────
class SettingsIn(BaseModel):
    partner_id: Optional[int] = None
    shop_name: Optional[str] = None
    kaspi_token: Optional[str] = None
    city_id: Optional[int] = None
    business_day_start: Optional[str] = None
    timezone: Optional[str] = None
    min_margin_pct: Optional[float] = None
    auto_reprice: Optional[bool] = None

@router.get("/me")
def get_my_settings(
    user: Any = Depends(get_current_user),
    uid: Optional[str] = Depends(require_tenant_optional),  # это UID пользователя (опционально)
):
    """
    Возвращает плоский JSON настроек для UI.
    401 — если не авторизован (бросает get_current_user)
    404 — если нет membership (первый заход) ИЛИ нет строки в tenant_settings
           → фронт редиректит на /ui/settings.html
    """
    user_id = _to_uid(user) or uid
    if not user_id:
        raise HTTPException(status_code=401, detail="Unauthorized")

    tenant_id = _find_tenant_for_user(user_id)
    if not tenant_id:
        # первый запуск — нет членства
        raise HTTPException(status_code=404, detail="settings not initialized")

    row = _select_settings_row(tenant_id)
    if not row:
        # членство есть, но настроек ещё нет
        raise HTTPException(status_code=404, detail="settings not initialized")

    return {
        "tenant_id": tenant_id,
        "partner_id": _to_int(row.get("partner_id")),
        "shop_name": row.get("shop_name"),
        "kaspi_token_masked": _mask_token(row.get("kaspi_token")),
        "city_id": _to_int(row.get("city_id")),
        "business_day_start": row.get("business_day_start") or "20:00",
        "timezone": row.get("timezone") or "Asia/Almaty",
        "min_margin_pct": _to_float(row.get("min_margin_pct")),
        "auto_reprice": bool(row.get("auto_reprice")) if row.get("auto_reprice") is not None else None,
        "updated_at": row.get("updated_at"),
    }

@router.post("/me")
def upsert_my_settings(
    payload: SettingsIn = Body(...),
    user: Any = Depends(get_current_user),
    uid: Optional[str] = Depends(require_tenant_optional),  # это UID пользователя (опционально)
):
    """
    Первый запуск:
      - если нет membership → создаём tenant + org_members(owner)
      - затем upsert в public.tenant_settings по переданным полям
    Семантика: None/пустые поля — НЕ изменяем существующее значение
    """
    user_id = _to_uid(user) or uid
    if not user_id:
        raise HTTPException(status_code=401, detail="Unauthorized")

    tenant_id = _find_tenant_for_user(user_id)
    if not tenant_id:
        tenant_id = _ensure_tenant_for_user(user_id)

    data = payload.model_dump(exclude_unset=True)  # только присланные поля

    # Касты под типы таблицы
    casted = {
        "partner_id":         _to_int(data.get("partner_id")),
        "shop_name":          (data.get("shop_name") or None),
        "kaspi_token":        (data.get("kaspi_token") or None),
        "city_id":            _to_int(data.get("city_id")),
        "business_day_start": (data.get("business_day_start") or None),
        "timezone":           (data.get("timezone") or None),
        "min_margin_pct":     _to_float(data.get("min_margin_pct")),
        "auto_reprice":       _parse_bool(data.get("auto_reprice")),
    }
    # удалим ключи со значением None — чтобы не затирать
    casted = {k: v for k, v in casted.items() if v is not None}

    if casted:
        _upsert_settings(tenant_id, casted)
    else:
        # если настроек не было — создадим пустую строку, чтобы последующие GET возвращали 200
        if not _select_settings_row(tenant_id):
            _insert_settings(tenant_id, {})

    return {"ok": True, "tenant_id": tenant_id}
