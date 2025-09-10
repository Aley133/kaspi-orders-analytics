# app/deps/auth.py
from __future__ import annotations
import os, time, json
from typing import Any, Dict, Optional, Tuple
from fastapi import Request, HTTPException
from jose import jwt, JWTError
import urllib.request
import urllib.error

# ──────────────────────────────────────────────────────────────────────────────
# Конфиг
# ──────────────────────────────────────────────────────────────────────────────
SUPABASE_PROJECT_REF = os.getenv("SUPABASE_PROJECT_REF")  # напр. "evmdqyngzleandtfkanv"
SUPABASE_JWKS_URL    = os.getenv("SUPABASE_JWKS_URL") or (
    f"https://{SUPABASE_PROJECT_REF}.supabase.co/auth/v1/keys" if SUPABASE_PROJECT_REF else None
)
SUPABASE_JWT_SECRET  = os.getenv("SUPABASE_JWT_SECRET")  # для HS256 (legacy)
JWT_AUDIENCE         = os.getenv("JWT_AUD", None)        # можно не задавать
CLOCK_SKEW_SECONDS   = int(os.getenv("JWT_CLOCK_SKEW", "60"))

# Кэш JWKS: (timestamp, jwks_dict)
_JWKS_CACHE: Tuple[float, Dict[str, Any]] = (0.0, {})

def _get_jwks() -> Dict[str, Any]:
    """Берём JWKS с лёгким кэшем на 5 минут (без внешних зависимостей)."""
    global _JWKS_CACHE
    if not SUPABASE_JWKS_URL:
        # лучше 401, чтобы фронт корректно редиректил на логин
        raise HTTPException(401, "Invalid token")

    ts, jwks = _JWKS_CACHE
    now = time.time()
    if jwks and now - ts < 300:
        return jwks

    try:
        req = urllib.request.Request(
            SUPABASE_JWKS_URL,
            headers={"User-Agent": "kaspi-orders-analytics/1.0"}
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode("utf-8", errors="ignore"))
        if not isinstance(data, dict) or "keys" not in data:
            raise HTTPException(401, "Invalid token")
        _JWKS_CACHE = (now, data)
        return data
    except (urllib.error.URLError, urllib.error.HTTPError, ValueError):
        # любые сетевые/парсинговые ошибки — как битый токен
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
    # запасной вариант — query
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
    """RS256 через JWKS (дефолт для Supabase без HS-секрета)."""
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
    Обязательная авторизация. Любая проблема с токеном => 401 (не 500).
    Возвращает claims (dict).
    """
    token = _get_bearer_token(req)
    if not token:
        raise HTTPException(401, "Missing Bearer token")

    if SUPABASE_JWT_SECRET:
        claims = _decode_hs256(token)
    else:
        claims = _decode_rs256_with_jwks(token)

    # мягкая проверка, sub может отсутствовать
    _ = claims.get("sub") or claims.get("user_id") or claims.get("uid") or claims.get("id")
    return claims
