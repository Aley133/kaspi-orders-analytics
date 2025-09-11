from __future__ import annotations
from fastapi import APIRouter, Depends
from app.deps.auth import get_current_user

router = APIRouter(prefix="/auth", tags=["auth"])

@router.get("/whoami")
async def whoami(user = Depends(get_current_user)):
    return {
        "tenant_id": user["tenant_id"],
        "user_id": user["user_id"],
        "email": user["email"],
        "role": user.get("role"),
    }

try:
    from app.api.settings import router as settings_router
    router.include_router(settings_router, prefix="/settings", tags=["settings"])
except Exception:
    # чтобы не падать, если файл ещё не завезли
    pass
