# app/api/authz.py
from fastapi import APIRouter, Depends, HTTPException, Request
from jose import jwt, JWTError
import os

router = APIRouter(prefix="/auth", tags=["auth"])

ALGO = "HS256"
JWT_SECRET = os.getenv("SUPABASE_JWT_SECRET")

def get_current_user(req: Request):
    auth = req.headers.get("authorization") or req.headers.get("Authorization")
    if not auth or not auth.startswith("Bearer "):
        raise HTTPException(401, "Missing Bearer token")
    token = auth.split(" ", 1)[1]
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[ALGO], options={"verify_aud": False})
    except JWTError:
        raise HTTPException(401, "Invalid token")
    return {"user_id": payload.get("sub"), "email": payload.get("email")}

@router.get("/whoami")
def whoami(user = Depends(get_current_user)):
    return {"ok": True, "user": user}
