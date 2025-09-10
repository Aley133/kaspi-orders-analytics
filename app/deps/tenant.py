# app/deps/tenant.py
from __future__ import annotations
from typing import Optional, Union, Dict, Any
from fastapi import Depends, HTTPException

from .auth import require_user, require_user_optional

Claim = Union[Dict[str, Any], str, None]

def _extract_uid(user: Claim) -> Optional[str]:
    """Пытаемся достать UID из разных форматов user/claims без падений."""
    if not user:
        return None
    if isinstance(user, dict):
        # Прямые ключи
        for k in ("sub", "user_id", "uid", "id"):
            v = user.get(k)
            if v:
                return str(v)
        # Часто user вложен как {"user": {...}}
        inner = user.get("user")
        if isinstance(inner, dict):
            for k in ("sub", "user_id", "uid", "id"):
                v = inner.get(k)
                if v:
                    return str(v)
        # Ничего не нашли — считаем, что неавторизован
        return None
    # Строка — уже UID
    return str(user)

def require_tenant_optional(user: Claim = Depends(require_user_optional)) -> Optional[str]:
    """Не бросает исключение, возвращает None если не удалось определить UID."""
    return _extract_uid(user)

def require_tenant(user: Claim = Depends(require_user)) -> str:
    """Требует авторизации: бросает 401, если UID не найден."""
    uid = _extract_uid(user)
    if not uid:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return uid
