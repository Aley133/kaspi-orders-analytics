# app/deps/tenant.py
from __future__ import annotations
from typing import Optional, Any, Dict, Union
from fastapi import Depends, HTTPException

# 1) Импортируем единственную обязательную зависимость из твоего auth-модуля.
#    Никаких require_user/require_user_optional больше не требуем.
try:
    from app.deps.auth import get_current_user as _get_current_user
except Exception as e:
    # На случай, если модуль/функция отсутствует — дадим внятную ошибку при первом обращении.
    def _get_current_user(*args, **kwargs):
        raise HTTPException(status_code=401, detail="Unauthorized")

Claim = Union[Dict[str, Any], str, None]

def _extract_uid(user: Claim) -> Optional[str]:
    """
    Аккуратно достаём UID из claim'а (Supabase-JWT / произвольный словарь).
    Возвращаем None, если айди не нашли.
    """
    if not user:
        return None
    if isinstance(user, dict):
        for k in ("sub", "user_id", "uid", "id"):
            v = user.get(k)
            if v:
                return str(v)
        inner = user.get("user")
        if isinstance(inner, dict):
            for k in ("sub", "user_id", "uid", "id"):
                v = inner.get(k)
                if v:
                    return str(v)
        return None
    return str(user)

def get_current_user_optional(*args, **kwargs) -> Optional[Claim]:
    """
    Обёртка над get_current_user, которая НЕ роняет 401/403, а возвращает None.
    Позволяет делать "мягкие" зависимости (первый заход, неинициализированные настройки).
    """
    try:
        return _get_current_user(*args, **kwargs)
    except HTTPException as e:
        if e.status_code in (401, 403):
            return None
        raise

def require_tenant_optional(user: Claim = Depends(get_current_user_optional)) -> Optional[str]:
    """
    Мягкая зависимость: вернёт UID пользователя или None.
    Никогда не бросает 500 из-за отсутствия 'sub'.
    """
    return _extract_uid(user)

def require_tenant(user: Claim = Depends(_get_current_user)) -> str:
    """
    Жёсткая зависимость: требует авторизацию и наличие UID.
    """
    uid = _extract_uid(user)
    if not uid:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return uid
