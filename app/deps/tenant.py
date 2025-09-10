from __future__ import annotations
from typing import Optional, Union, Dict, Any
from fastapi import Depends, HTTPException
from .auth import require_user, require_user_optional

Claim = Union[Dict[str, Any], str, None]

def _extract_uid(user: Claim) -> Optional[str]:
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

def require_tenant_optional(user: Claim = Depends(require_user_optional)) -> Optional[str]:
    return _extract_uid(user)

def require_tenant(user: Claim = Depends(require_user)) -> str:
    uid = _extract_uid(user)
    if not uid:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return uid
