# app/api/authz.py
from fastapi import APIRouter, Depends
from app.deps.auth import get_current_user

router = APIRouter(prefix="/auth", tags=["auth"])

@router.get("/whoami")
def whoami(user = Depends(get_current_user)):
    # здесь позже подвяжем tenant_id / авто-провижининг
    return {"ok": True, "user": user}

