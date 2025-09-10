# app/deps/auth.py
from __future__ import annotations
import os, time, httpx, logging
from typing import Dict, Any
from fastapi import HTTPException, Request
from jose import jwt, JWTError, jwk
from jose.utils import base64url_decode

log = logging.getLogger("auth")

SUPABASE_URL = os.getenv("SUPABASE_URL")  # вида https://<ref>.supabase.co
LEGACY_SECRET = os.getenv("SUPABASE_JWT_SECRET")  # HS256 (Legacy)
ALGO_LEGACY = "HS256"

_JWKS_CACHE: Dict[str, Any] = {}
_JWKS_TS: float | None = None

def _load_jwks() -> Dict[str, Any]:
    """Кэшируем JWKS ключи Supabase на 10 минут."""
    global _JWKS_CACHE, _JWKS_TS
    if _JWKS_TS and (time.time() - _JWKS_TS) < 600 and _JWKS_CACHE:
        return _JWKS_CACHE
    if not SUPABASE_URL:
        raise HTTPException(500, "Server misconfigured: SUPABASE_URL not set")
    jwks_url = SUPABASE_URL.rstrip("/") + "/auth/v1/keys"
    try:
        with httpx.Client(timeout=10) as c:
            resp = c.get(jwks_url)
            resp.raise_for_status()
            _JWKS_CACHE = resp.json()
            _JWKS_TS = time.time()
            return _JWKS_CACHE
    except Exception as e:
        log.error("Failed to fetch JWKS: %s", e)
        raise HTTPException(500, "Auth keys fetch failed")

def _verify_with_jwks(token: str) -> Dict[str, Any]:
    """Проверка подписи через публичные ключи (RS256/EdDSA)."""
    headers = jwt.get_unverified_header(token)
    kid = headers.get("kid")
    if not kid:
        raise HTTPException(401, "Invalid token header")
    jwks = _load_jwks()
    keys = jwks.get("keys", [])
    key = next((k for k in keys if k.get("kid") == kid), None)
    if not key:
        raise HTTPException(401, "Unknown signing key")

    # Проверяем подпись вручную
    public_key = jwk.construct(key)  # jose сам выберет тип (RSA/OKP/EC)
    try:
        signing_input, encoded_sig = token.rsplit(".", 1)
        decoded_sig = base64url_decode(encoded_sig.encode("utf-8"))
        if not public_key.verify(signing_input.encode("utf-8"), decoded_sig):
            raise HTTPException(401, "Invalid token signature")
        # Разбираем payload без повторной верификации подписи
        claims = jwt.get_unverified_claims(token)
    except JWTError:
        raise HTTPException(401, "Invalid token")

    # exp check
    exp = claims.get("exp")
    if exp is not None and time.time() > float(exp):
        raise HTTPException(401, "Token expired")
    return claims

def _verify_with_legacy(token: str) -> Dict[str, Any]:
    if not LEGACY_SECRET:
        raise HTTPException(500, "Server misconfigured: SUPABASE_JWT_SECRET not set")
    try:
        return jwt.decode(
            token, LEGACY_SECRET,
            algorithms=[ALGO_LEGACY],
            options={"verify_aud": False}
        )
    except JWTError as e:
        raise HTTPException(401, "Invalid token")

def get_current_user(req: Request) -> Dict[str, Any]:
    auth = req.headers.get("authorization") or req.headers.get("Authorization")
    if not auth or not auth.startswith("Bearer "):
        raise HTTPException(401, "Missing Bearer token")
    token = auth.split(" ", 1)[1]

    # Пытаемся как HS256 (legacy). Если не вышло — JWKS.
    try:
        claims = _verify_with_legacy(token)
    except HTTPException:
        claims = _verify_with_jwks(token)

    sub = claims.get("sub")
    if not sub:
        raise HTTPException(401, "Bad token payload")
    return {"user_id": sub, "email": claims.get("email")}
