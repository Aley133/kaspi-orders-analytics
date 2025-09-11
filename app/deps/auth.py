# deps/auth.py
import os, base64, json
from typing import Optional, Callable
from fastapi import Request, HTTPException
import contextvars

from .tenant import resolve_kaspi_token

kaspi_token_ctx: contextvars.ContextVar[str] = contextvars.ContextVar("kaspi_token", default="")

def _decode_jwt_noverify(token: str) -> dict:
    # безопасно для извлечения sub; подпись не проверяем (трафик идёт с фронта Supabase)
    try:
        parts = token.split(".")
        payload_b64 = parts[1] + "=="  # выравнивание
        payload = json.loads(base64.urlsafe_b64decode(payload_b64.encode("utf-8")).decode("utf-8"))
        return payload
    except Exception:
        return {}

def get_current_user(request: Request):
    """
    Compatibility shim for app.api.authz.
    Возвращаем минимальный объект "пользователя" на основе tenant_id,
    который уже выставляет мидлвара attach_kaspi_token_middleware.
    """
    tenant_id = get_current_tenant_id(request)
    if not tenant_id:
        raise HTTPException(status_code=401, detail="unauthorized")
    return {"tenant_id": tenant_id}

def get_current_tenant_id(request: Request) -> str:
    auth = request.headers.get("authorization") or ""
    if not auth.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token")
    jwt = auth.split(" ", 1)[1].strip()
    claims = _decode_jwt_noverify(jwt)
    sub = claims.get("sub")
    if not sub:
        raise HTTPException(status_code=401, detail="Bad token (no sub)")
    return sub

def _normalize_tenant_id(raw: str) -> str:
    """
    Превращает любой sub в UUID. Если уже UUID — вернём как есть.
    Иначе делаем детерминированный UUID5 на основе строки sub.
    """
    try:
        uuid.UUID(str(raw))
        return str(raw)
    except Exception:
        return str(uuid.uuid5(uuid.NAMESPACE_URL, f"supabase:{raw}"))

async def attach_kaspi_token_middleware(request: Request, call_next):
    auth = request.headers.get("authorization") or request.headers.get("Authorization")
    tenant_id = None
    if auth and auth.lower().startswith("bearer "):
        token = auth.split(" ", 1)[1].strip()
        try:
            claims = jwt.decode(token, options={"verify_signature": False})
            sub = claims.get("sub")
            if sub:
                tenant_id = _normalize_tenant_id(str(sub))
        except Exception:
            pass
    request.state.tenant_id = tenant_id
    return await call_next(request)

def get_current_tenant_id(request: Request) -> str | None:
    return getattr(request.state, "tenant_id", None)
