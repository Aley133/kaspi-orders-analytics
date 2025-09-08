# app/api/authz.py
from fastapi import APIRouter, Depends, HTTPException, Request
from jose import jwt, JWTError
import os, logging

router = APIRouter(prefix="/auth", tags=["auth"])
log = logging.getLogger("auth")

ALGO = "HS256"
JWT_SECRET = os.getenv("SUPABASE_JWT_SECRET")

def get_current_user(req: Request):
    if not JWT_SECRET:
        raise HTTPException(500, "Server misconfigured: SUPABASE_JWT_SECRET not set")

    auth = req.headers.get("authorization") or req.headers.get("Authorization")
    if not auth or not auth.startswith("Bearer "):
        raise HTTPException(401, "Missing Bearer token")

    token = auth.split(" ", 1)[1]
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[ALGO], options={"verify_aud": False})
    except JWTError as e:
        log.warning("JWT decode failed: %s", e)
        raise HTTPException(401, "Invalid token")

    sub = payload.get("sub")
    if not sub:
        raise HTTPException(401, "Bad token payload")
    return {"user_id": sub, "email": payload.get("email")}

@router.get("/whoami")
def whoami(user = Depends(get_current_user)):
    return {"ok": True, "user": user}
