# app/deps/auth.py
from __future__ import annotations
import os, time
from typing import Any, Dict, Optional, Tuple
import requests
from fastapi import Request, HTTPException
from jose import jwt, JWTError

# ──────────────────────────────────────────────────────────────────────────────
# Конфиг
# ──────────────────────────────────────────────────────────────────────────────
SUPABASE_PROJECT_REF = os.getenv("SUPABASE_PROJECT_REF")  # напр. "abcd1234"
SUPABASE_JWKS_URL    = os.getenv("SUPABASE_JWKS_URL") or (
    f"https://{SUPABASE_PROJECT_REF}.supabase.co/auth/v1/keys" if SUPABASE_PROJECT_REF else None
)
SUPABASE_JWT_SECRET  = os.getenv("SUPABASE_JWT_SECRET")  # для HS256 (legacy)
JWT_AUDIENCE         = os.getenv("JWT_AUD", None)        # можно не задавать
CLOCK_SKEW_SECONDS   = int(os.getenv("JWT_CLOCK_SKEW", "60"))

# Кэш JWKS: (timestamp, jwks_dict)
_JWKS_CACHE: Tuple[float, Dict[str, Any]] = (0.0, {})

def _get_jwks() -> Dict[str, Any]:
    """Берём JWKS с лёгким кэшем на 5 минут."""
    global _JWKS_CACHE  # <- важно объявить ПЕРЕД любым использованием
    if not SUPABASE_JWKS_URL:
        # Конфиг не задан — лучше 401, чем 500, чтобы UI мог отреагировать
        raise HTTPException(401, "Invalid token")

    ts, jwks = _JWKS_CACHE
    now = time.time()
    if jwks and now - ts < 300:
        return jwks

    try:
        resp = requests.get(SUPABASE_JWKS_URL, timeout=5)
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, dict) or "keys" not in data:
            raise HTTPException(401, "Invalid token")

        _JWKS_CACHE = (now, data)
        return data
    except Exception:
        # Любая ошибка сети/парсинга — как битый токен
        raise HTTPException(401, "Invalid token")

def _get_bearer_token(req: Request) -> Optional[str]:
    """Достаём токен из Authorization/Cookie/Query."""
    auth = req.headers.get("Authorization") or req.headers.get("authorization") or ""
    if auth.lower().startswith("bearer "):
        tok = auth[7:].strip()
        if tok:
            return tok
    # иногда Supabase кладёт токен в cookie
    for name in ("sb-access-token", "access_token", "token"):
        tok = req.cookies.get(name)
        if tok:
            return tok
    # на крайний случай — из query
    tok = req.query_params.get("access_token")
    if tok:
        return tok
    return None

def _decode_hs256(token: str) -> Dict[str, Any]:
    """HS256 (legacy), если задан SUPABASE_JWT_SECRET."""
    try:
        return jwt.decode(
            token,
            SUPABASE_JWT_SECRET,
            algorithms=["HS256"],
            audience=JWT_AUDIENCE,
            options={"verify_aud": bool(JWT_AUDIENCE), "verify_at_hash": False},
            leeway=CLOCK_SKEW_SECONDS,
        )
    except JWTError:
        raise HTTPException(401, "Invalid token")
    except Exception:
        raise HTTPException(401, "Invalid token")

def _decode_rs256_with_jwks(token: str) -> Dict[str, Any]:
    """RS256 через JWKS (дефолт для Supabase)."""
    # Быстрая проверка структуры
    if token.count(".") != 2:
        raise HTTPException(401, "Invalid token")
    try:
        header = jwt.get_unverified_header(token)
        kid = header.get("kid")
        jwks = _get_jwks()
        key = None
        for k in jwks.get("keys", []):
            if k.get("kid") == kid:
                key = k
                break
        if not key:
            # fallback: первый ключ
            keys = jwks.get("keys", [])
            if keys:
                key = keys[0]
        if not key:
            raise HTTPException(401, "Invalid token")

        return jwt.decode(
            token,
            key,
            algorithms=["RS256"],
            audience=JWT_AUDIENCE,
            options={"verify_aud": bool(JWT_AUDIENCE), "verify_at_hash": False},
            leeway=CLOCK_SKEW_SECONDS,
        )
    except JWTError:
        raise HTTPException(401, "Invalid token")
    except Exception:
        raise HTTPException(401, "Invalid token")

def get_current_user(req: Request) -> Dict[str, Any]:
    """
    Обязательная авторизация. Любая проблема с токеном => 401 без 500.
    Возвращает claims (dict). Ожидается поле sub (uuid), но downstream-код готов к его отсутствию.
    """
    token = _get_bearer_token(req)
    if not token:
        raise HTTPException(401, "Missing Bearer token")

    if SUPABASE_JWT_SECRET:
        claims = _decode_hs256(token)
    else:
        claims = _decode_rs256_with_jwks(token)

    # мягкая проверка наличия sub (у нас tenant.py уже умеет работать без sub)
    _ = claims.get("sub") or claims.get("user_id") or claims.get("uid") or claims.get("id")
    return claims
