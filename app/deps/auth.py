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

async def attach_kaspi_token_middleware(request: Request, call_next):
    """
    Глобальный middleware: на каждый запрос ставим контекстный Kaspi токен арендатора.
    """
    try:
        tenant_id = get_current_tenant_id(request)
        token = resolve_kaspi_token(tenant_id)
        if not token:
            # Явно запрещаем «глобальные» токены: без персонального — 401
            raise HTTPException(status_code=401, detail="Kaspi token is not set for this tenant")
        kaspi_token_ctx.set(token)
    except HTTPException as e:
        # Разрешаем **только** белый список открытых ручек
        open_paths = {"/auth/meta", "/openapi.json", "/docs", "/ui/", "/"}
        if not any(request.url.path.startswith(p) for p in open_paths):
            raise
    response = await call_next(request)
    return response

def get_current_kaspi_token() -> Optional[str]:
    tok = kaspi_token_ctx.get()
    return tok or None
