# app/deps/auth.py
from fastapi import HTTPException, Request
from jose import jwt, JWTError
import os

ALGO = "HS256"
JWT_SECRET = os.getenv("SUPABASE_JWT_SECRET")  # Studio → JWT Keys → JWT secret

def get_current_user(req: Request) -> dict:
    auth = req.headers.get("authorization") or req.headers.get("Authorization")
    if not auth or not auth.startswith("Bearer "):
        raise HTTPException(401, "Missing Bearer token")
    token = auth.split(" ", 1)[1]
    try:
        # Supabase JWT: HS256, aud/iss можно не проверять
        payload = jwt.decode(token, JWT_SECRET, algorithms=[ALGO], options={"verify_aud": False})
    except JWTError:
        raise HTTPException(401, "Invalid token")
    uid = payload.get("sub")
    email = payload.get("email")
    if not uid:
        raise HTTPException(401, "Bad token payload")
    return {"user_id": uid, "email": email}

