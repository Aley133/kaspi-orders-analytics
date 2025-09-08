# app/api/authz.py
from fastapi import APIRouter, Depends
from app.deps.auth import get_current_user
from app.deps.tenant import require_tenant
from app import db

router = APIRouter(prefix="/auth", tags=["auth"])

@router.get("/whoami")
def whoami(user = Depends(get_current_user)):
    return {"ok": True, "user": user}

@router.get("/tenant")
def my_tenant(tenant_id: str = Depends(require_tenant)):
    # отдадим и настройки, чтобы видеть что всё создалось
    settings = db.fetchall(
        "select key, value from tenant_settings where tenant_id=%s order by key",
        [tenant_id],
    )
    return {"ok": True, "tenant_id": tenant_id, "settings": settings}
