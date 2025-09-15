# app/deps/auth.py
from __future__ import annotations

import base64
import json
import uuid
import contextvars
from typing import Optional, Dict

from fastapi import Request, HTTPException

from .tenant import resolve_kaspi_token

# Храним текущий kaspi-token в ContextVar, чтобы его могли читать клиенты ниже по стеку
kaspi_token_ctx: contextvars.ContextVar[str] = contextvars.ContextVar("kaspi_token", default="")

    
def _decode_jwt_noverify(token: str) -> Dict:
    """
    Безопасно достаём payload из JWT без проверки подписи
    (подпись проверяет Supabase на фронте; нам оттуда прилетает bearer).
    """
    try:
        parts = token.split(".")
        if len(parts) < 2:
            return {}
        payload_b64 = parts[1]
        # выравнивание base64url
        padding = "=" * (-len(payload_b64) % 4)
        payload = base64.urlsafe_b64decode((payload_b64 + padding).encode("utf-8")).decode("utf-8")
        return json.loads(payload)
    except Exception:
        return {}

def _normalize_tenant_id(raw: str) -> str:
    """
    sub может быть UUID, а может — строка (например, логин GitHub/SMS).
    Превращаем в детерминированный UUID5.
    """
    try:
        uuid.UUID(str(raw))
        return str(raw)
    except Exception:
        return str(uuid.uuid5(uuid.NAMESPACE_URL, f"supabase:{raw}"))

def get_current_tenant_id(request: Request) -> Optional[str]:
    """
    Извлекаем tenant_id, который проставила мидлвара.
    """
    return getattr(request.state, "tenant_id", None)

def get_current_user(request: Request) -> Dict:
    tenant_id = get_current_tenant_id(request)
    if not tenant_id:
        raise HTTPException(status_code=401, detail="unauthorized")
    # подстрахуем ожидаемые ключи
    return {
        "tenant_id": tenant_id,
        "user_id": getattr(request.state, "user_id", None),
        "email": getattr(request.state, "email", None),
        "role": getattr(request.state, "role", None),
    }

def get_current_kaspi_token() -> Optional[str]:
    """
    Читаем текущий kaspi-token из контекста.
    """
    tok = kaspi_token_ctx.get()
    return tok or None

async def attach_kaspi_token_middleware(request: Request, call_next):
    """
    1) Парсим Bearer JWT → sub → нормализуем в UUID → кладём в request.state.tenant_id
    2) Резолвим kaspi_token из БД → кладём в request.state.kaspi_token и ContextVar
    """
    tenant_id: Optional[str] = None

    # NEW: сохраним сырой bearer, чтобы роуты знали «есть ли сессия»
    token_hdr = request.headers.get("authorization") or request.headers.get("Authorization") or ""
    jwt_token: Optional[str] = None
    if token_hdr.lower().startswith("bearer "):
        jwt_token = token_hdr.split(" ", 1)[1].strip()
        claims = _decode_jwt_noverify(jwt_token)
        sub = claims.get("sub")
        if sub:
            tenant_id = _normalize_tenant_id(str(sub))

    # NEW: кладём в state для мягкой проверки сессии
    request.state.supabase_token = jwt_token or ""

    request.state.tenant_id = tenant_id

    kaspi_tok = resolve_kaspi_token(tenant_id) if tenant_id else None
    request.state.kaspi_token = kaspi_tok or ""
    token_token = kaspi_token_ctx.set(kaspi_tok or "")

    try:
        response = await call_next(request)
    finally:
        kaspi_token_ctx.reset(token_token)

    return response
