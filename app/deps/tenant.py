# app/deps/tenant.py
from __future__ import annotations
from typing import Optional, Any, Dict, Union
from fastapi import Depends, HTTPException
from app.deps.auth import get_current_user as _get_current_user


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


def get_current_user_optional(*args, **kwargs):
try:
return _get_current_user(*args, **kwargs)
except HTTPException as e:
if e.status_code in (401, 403):
return None
raise


def require_tenant_optional(user: Claim = Depends(get_current_user_optional)) -> Optional[str]:
return _extract_uid(user)


def require_tenant(user: Claim = Depends(_get_current_user)) -> str:
uid = _extract_uid(user)
if not uid:
raise HTTPException(status_code=401, detail="Unauthorized")
return uid
