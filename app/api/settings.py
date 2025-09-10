# app/api/settings.py
from __future__ import annotations
from typing import Optional, Any, Dict, List
from fastapi import APIRouter, Depends, HTTPException, Body
from pydantic import BaseModel
from app.deps.auth import get_current_user
from app.deps.tenant import require_tenant_optional
from app import db


router = APIRouter(prefix="/settings", tags=["settings"])


# helpers


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
if sx in ("1", "true", "yes", "on"): return True
if sx in ("0", "false", "no", "off"): return False
return None


# tenant helpers


def _find_tenant_for_user(user_id: str) -> Optional[str]:
row = db.fetchrow("select tenant_id from public.org_members where user_id=%s limit 1", [user_id])
return row["tenant_id"] if row and row.get("tenant_id") else None
return {"ok": True, "tenant_id": tenant_id}
