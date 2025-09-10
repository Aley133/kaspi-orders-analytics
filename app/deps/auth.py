from __future__ import annotations
import base64, json, hmac, hashlib, os
from typing import Any, Dict, Optional
from fastapi import HTTPException, Header

def _b64url_decode(s: str) -> bytes:
    pad = '=' * (-len(s) % 4)
    return base64.urlsafe_b64decode(s + pad)

def _jwt_decode_noverify(token: str) -> Dict[str, Any]:
    try:
        header_b64, payload_b64, _sig = token.split('.', 2)
        return {"header": json.loads(_b64url_decode(header_b64)),
                "payload": json.loads(_b64url_decode(payload_b64))}
    except Exception:
        raise HTTPException(status_code=401, detail="Bad JWT")

def _jwt_verify_hs256(token: str, secret: str) -> bool:
    try:
        header_b64, payload_b64, sig_b64 = token.split('.', 2)
        signing_input = f"{header_b64}.{payload_b64}".encode("utf-8")
        expected = hmac.new(secret.encode("utf-8"), signing_input, hashlib.sha256).digest()
        got = _b64url_decode(sig_b64)
        return hmac.compare_digest(expected, got)
    except Exception:
        return False

def _extract_tenant(payload: Dict[str, Any]) -> str:
    for path in [("app_metadata","tenant_id"), ("user_metadata","tenant_id")]:
        cur = payload; ok = True
        for p in path:
            if not isinstance(cur, dict) or p not in cur:
                ok = False; break
            cur = cur[p]
        if ok and isinstance(cur, (str,int)) and str(cur):
            return str(cur)
    sub = payload.get("sub")
    if isinstance(sub, str) and sub:
        return sub
    raise HTTPException(status_code=401, detail="tenant_id not found in JWT")

def _extract_email(payload: Dict[str, Any]) -> Optional[str]:
    for k in ("email","user_email","preferred_username"):
        v = payload.get(k)
        if isinstance(v, str) and v:
            return v
    return None

def _bearer_to_token(authorization: Optional[str]) -> str:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing Bearer token")
    return authorization.split(" ", 1)[1].strip()

async def get_current_user(authorization: Optional[str] = Header(None)) -> Dict[str, Any]:
    token = _bearer_to_token(authorization)
    data = _jwt_decode_noverify(token)
    secret = os.getenv("SUPABASE_JWT_SECRET", "").strip()
    if secret and not _jwt_verify_hs256(token, secret):
        raise HTTPException(status_code=401, detail="Invalid JWT signature")
    payload = data["payload"]
    return {
        "tenant_id": _extract_tenant(payload),
        "user_id": payload.get("sub"),
        "email": _extract_email(payload),
        "role": payload.get("role") or payload.get("app_metadata", {}).get("role"),
        "raw": payload,
        "token": token,
    }

# ── Хелпер для middleware: мягко достаём tenant_id, без исключений ────────────
def try_extract_tenant_from_authorization(authorization: Optional[str]) -> Optional[str]:
    try:
        if not authorization or not authorization.lower().startswith("bearer "):
            return None
        token = authorization.split(" ", 1)[1].strip()
        header_b64, payload_b64, _sig = token.split('.', 2)
        payload = json.loads(_b64url_decode(payload_b64))
        # тот же приоритет
        for path in [("app_metadata","tenant_id"), ("user_metadata","tenant_id")]:
            cur = payload; ok = True
            for p in path:
                if not isinstance(cur, dict) or p not in cur: ok = False; break
                cur = cur[p]
            if ok and str(cur): return str(cur)
        return payload.get("sub")
    except Exception:
        return None
