from __future__ import annotations
from typing import Dict
from fastapi import Depends
from .auth import get_current_user

async def require_tenant(user: Dict = Depends(get_current_user)) -> str:
    return str(user["tenant_id"])
