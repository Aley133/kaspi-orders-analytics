# app/deps/auth.py
from __future__ import annotations
import os, time
from typing import Any, Dict, Optional, Tuple
import requests
from fastapi import Request, HTTPException, Depends
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

# Кэш JWKS
_JWKS_CACHE: Tuple[float, Dict[str, Any]] = (0.0, {})

def _get_jwks() -> Dict[str, Any]:
    """Берём JWKS с лёгким кэшем на 5 минут."""
    if not SUPABASE_JWKS_URL:
        raise HTTPException(500, "Auth is misconfigured (no JWKS url and no HS256 secret)")
    ts, jwks = _JWKS_CACHE
    now = time.time()
    if jwks and now - ts < 300:
        return jwks
    try:
        resp = requests.get(SUPABASE_JWKS_URL, timeout=5)
        resp.raise_for_status()
        data = resp.json()
        # ожидаем {"keys":[...]}
        if not isinstance(data, dict) or "keys" not in data:
            raise HTTPException(500, "Invalid JWKS response")
        # обновляем кэш
        global _JWKS_CACHE
        _JWKS_CACHE = (now, data)
        return data
    except Exception:
        # не выдаём 500 наружу при обычных вызовах — лучше 401
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
    # в крайнем случае из query ?access_token=
    tok = req.query_params.get("access_token")
    if tok:
        return tok
    return None

def _decode_hs256(token: str) -> Dict[str, Any]:
    """HS256 (legacy) — когда задан SUPABASE_JWT_SECRET."""
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
    """RS256 через JWKS (основной путь для Supabase)."""
    # Быстрая проверка структуры
    if token.count(".") != 2:
        raise HTTPException(401, "Invalid token")
    # Пробуем без выбора ключа (python-jose сам достанет key по kid, если передать jwk dict)
    try:
        # jose не принимает сразу весь JWKS, поэтому достанем kid и найдём конкретный ключ
        header = jwt.get_unverified_header(token)
        kid = header.get("kid")
        jwks = _get_jwks()
        key = None
        for k in jwks.get("keys", []):
            if k.get("kid") == kid:
                key = k
                break
        if not key:
            # если не нашли точный — допустим первый подходящий
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
        # Любая ошибка — 401, чтобы не было 500
        raise HTTPException(401, "Invalid token")

# ──────────────────────────────────────────────────────────────────────────────
# Публичные зависимости
# ──────────────────────────────────────────────────────────────────────────────
def get_current_user(req: Request) -> Dict[str, Any]:
    """
    Обязательная авторизация. Любая проблема с токеном => 401 без 500.
    Возвращает claims (dict). Ожидается поле sub (uuid).
    """
    token = _get_bearer_token(req)
    if not token:
        raise HTTPException(401, "Missing Bearer token")

    # если есть легаси-секрет — используем HS256, иначе RS256+JWKS
    if SUPABASE_JWT_SECRET:
        claims = _decode_hs256(token)
    else:
        claims = _decode_rs256_with_jwks(token)

    # минимальная валидация
    sub = claims.get("sub") or claims.get("user_id") or claims.get("uid") or claims.get("id")
    if not sub:
        # допустим, но downstream код должен быть готов (мы уже чинили tenant.py)
        pass
    return claims
