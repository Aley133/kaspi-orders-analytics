from __future__ import annotations
from fastapi import APIRouter, Depends
from app.deps.auth import get_current_user

router = APIRouter(prefix="/auth", tags=["auth"])

@router.get("/whoami")
async def whoami(user = Depends(get_current_user)):
    # Безопасный ответ (не возвращаем весь payload)
    return {
        "tenant_id": user["tenant_id"],
        "user_id": user["user_id"],
        "email": user["email"],
        "role": user.get("role"),
    }
