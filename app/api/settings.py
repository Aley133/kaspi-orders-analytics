# app/api/settings.py
from __future__ import annotations
from typing import Optional, Any, Dict
from fastapi import APIRouter, Depends, HTTPException, Body
from app.deps.auth import get_current_user  # должен возвращать dict с полем "sub" (uuid) из Supabase-JWT
from app.deps.tenant import require_tenant_optional  # сделаем опциональным: None -> создадим
from app import db

router = APIRouter(prefix="/settings", tags=["settings"])

# ----- Ключи в KV -----
K_PARTNER_ID       = "kaspi.partner_id"
K_TOKEN            = "kaspi.token"
K_SHOP_NAME        = "shop.name"
K_CITY_ID          = "city.id"
K_BIZDAY_START     = "bizday.start"
K_TZ               = "tz"
K_MIN_MARGIN       = "min.margin"
K_AUTO_REPRICE     = "auto.reprice"

ALL_KEYS = [
    K_PARTNER_ID, K_TOKEN, K_SHOP_NAME, K_CITY_ID,
    K_BIZDAY_START, K_TZ, K_MIN_MARGIN, K_AUTO_REPRICE
]

# ----- helpers -----
def _json_get(v: Any) -> Any:
    """
    value может быть jsonb/str. Поддержим варианты:
    - {"v": ...}
    - просто примитив (строка/число/булев)
    """
    if v is None:
        return None
    try:
        if isinstance(v, dict) and "v" in v:
            return v.get("v")
        return v
    except Exception:
        return v

def _json_wrap(v: Any) -> Any:
    # единообразно храним как {"v": <primitive>}
    return {"v": v} if v is not None else None

def _mask_token(tok: Optional[str]) -> Optional[str]:
    if not tok:
        return None
    s = str(tok)
    if len(s) <= 8:
        return "*" * len(s)
    return s[:4] + "*" * (len(s) - 8) + s[-4:]

def _parse_bool(x: Any) -> Optional[bool]:
    if x is None or x == "":
        return None
    if isinstance(x, bool):
        return x
    sx = str(x).strip().lower()
    if sx in ("1", "true", "yes", "on"):
        return True
    if sx in ("0", "false", "no", "off"):
        return False
    return None

# ----- авто-провижининг -----
def _ensure_tenant_for_user(user_id: str) -> str:
    # ищем tenant по membership
    row = db.fetchrow(
        "select tenant_id from org_members where user_id=%s limit 1",
        [user_id],
    )
    if row and row.get("tenant_id"):
        return row["tenant_id"]

    # создаём tenant и membership
    t = db.fetchrow("insert into tenants default values returning id", [])
    tenant_id = t["id"]

    db.execute(
        "insert into org_members(tenant_id, user_id, role) values (%s, %s, %s)",
        [tenant_id, user_id, "owner"],
    )
    return tenant_id

def _select_settings_kv(tenant_id: str) -> Dict[str, Any]:
    rows = db.fetchall(
        "select key, value from tenant_settings where tenant_id=%s",
        [tenant_id],
    )
    out: Dict[str, Any] = {}
    for r in rows:
        k = r["key"]
        v = r["value"]
        # если value текст — попробуем не ломаться
        if isinstance(v, str):
            out[k] = v
        else:
            out[k] = _json_get(v)
    return out

def _upsert_kv(tenant_id: str, key: str, value: Any):
    # если значение None — ничего не меняем (используем семантику "оставить как есть")
    if value is None:
        return
    # апсерт по (tenant_id, key)
    db.execute(
        """
        insert into tenant_settings(tenant_id, key, value)
        values (%s, %s, %s)
        on conflict (tenant_id, key)
        do update set value = excluded.value, updated_at = now()
        """,
        [tenant_id, key, _json_wrap(value)],
    )

# ----- API -----

@router.get("/me")
def get_my_settings(
    user = Depends(get_current_user),
    tenant_id: Optional[str] = Depends(require_tenant_optional),
):
    """
    Возвращает плоский JSON настроек для UI.
    Если tenant отсутствует (первый заход) — вернём 404,
    чтобы фронт редиректнул на /ui/settings.html.
    """
    user_id = user["sub"] if isinstance(user, dict) else user
    if not tenant_id:
        # нет членства -> считаем неинициализированным
        raise HTTPException(status_code=404, detail="settings not initialized")

    kv = _select_settings_kv(tenant_id)

    token_raw = kv.get(K_TOKEN)
    resp = {
        "tenant_id": tenant_id,
        "partner_id": _to_int(kv.get(K_PARTNER_ID)),
        "shop_name": kv.get(K_SHOP_NAME),
        "kaspi_token_masked": _mask_token(token_raw),
        "city_id": _to_int(kv.get(K_CITY_ID)),
        "business_day_start": kv.get(K_BIZDAY_START) or "20:00",
        "timezone": kv.get(K_TZ) or "Asia/Almaty",
        "min_margin_pct": _to_float(kv.get(K_MIN_MARGIN)),
        "auto_reprice": _parse_bool(kv.get(K_AUTO_REPRICE)),
    }
    return resp

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

class SettingsIn(dict):
    """
    Pydantic не обязателен — принимаем частичный JSON.
    Поддерживаем только поля, которые есть в UI.
    """
    pass

@router.post("/me")
def upsert_my_settings(
    payload: SettingsIn = Body(...),
    user = Depends(get_current_user),
    tenant_id: Optional[str] = Depends(require_tenant_optional),
):
    """
    Принимает частичный JSON. Пустые/отсутствующие поля не трогаем.
    Если у пользователя ещё нет tenant — создаём автоматически.
    """
    user_id = user["sub"] if isinstance(user, dict) else user
    if not tenant_id:
        tenant_id = _ensure_tenant_for_user(user_id)

    # Сопоставляем поля UI -> KV-ключи
    mapping = {
        "partner_id":        (K_PARTNER_ID,    _to_int),
        "shop_name":         (K_SHOP_NAME,     lambda v: v if v not in ("", None) else None),
        "kaspi_token":       (K_TOKEN,         lambda v: v if v not in ("", None) else None),
        "city_id":           (K_CITY_ID,       _to_int),
        "business_day_start":(K_BIZDAY_START,  lambda v: v if v not in ("", None) else None),
        "timezone":          (K_TZ,            lambda v: v if v not in ("", None) else None),
        "min_margin_pct":    (K_MIN_MARGIN,    _to_float),
        "auto_reprice":      (K_AUTO_REPRICE,  _parse_bool),
    }

    for field, (key, caster) in mapping.items():
        if field in payload:
            _upsert_kv(tenant_id, key, caster(payload.get(field)))

    return {"ok": True, "tenant_id": tenant_id}
