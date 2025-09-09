# app/deps/kaspi_client.py
from __future__ import annotations
import os
from fastapi import Depends, HTTPException
from app.deps.tenant import require_tenant
from app import db
try:
    from cryptography.fernet import Fernet
except Exception:
    Fernet = None

from app.kaspi_client import KaspiClient

_F = None
if Fernet and os.getenv("SETTINGS_CRYPT_KEY"):
    _F = Fernet(os.getenv("SETTINGS_CRYPT_KEY"))

def _dec(s: str | None) -> str | None:
    if not s: return s
    return _F.decrypt(s.encode()).decode() if _F else s

def get_kaspi_client(tenant_id: str = Depends(require_tenant)) -> KaspiClient:
    row = db.fetchrow("select value from tenant_settings where tenant_id=%s and key='kaspi.token'", [tenant_id])
    token = _dec(row["value"]["v"]) if row and row["value"] else None
    if not token:
        raise HTTPException(400, "Kaspi token is not configured")
    base_url = os.getenv("KASPI_BASE_URL", "https://kaspi.kz/shop/api/v2")
    return KaspiClient(token=token, base_url=base_url)
