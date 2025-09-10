from __future__ import annotations
from typing import Dict, Optional
from fastapi import Depends
from .auth import get_current_user
import os, re, psycopg, psycopg.rows

async def require_tenant(user: Dict = Depends(get_current_user)) -> str:
    return str(user["tenant_id"])

# ── PG URL нормализация (кавычки, схема) ──────────────────────────────────────
def _normalize_pg_url(url: str) -> str:
    u = (url or "").strip().strip('"').strip("'")
    u = re.sub(r"^postgresql\+[^:]+://", "postgresql://", u, flags=re.IGNORECASE)
    if u and "sslmode=" not in u:
        u += ("&" if "?" in u else "?") + "sslmode=require"
    return u

_PG_URL = _normalize_pg_url(
    os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL") or os.getenv("PROFIT_DB_URL") or ""
)

def _conn():
    if not _PG_URL:
        raise RuntimeError("No Postgres URL (SUPABASE_DB_URL / DATABASE_URL / PROFIT_DB_URL)")
    return psycopg.connect(_PG_URL, autocommit=True)

# ── Резолвер токена: ищем value->>'kaspi_token' по tenant_id ──────────────────
def resolve_kaspi_token(tenant_id: Optional[str]) -> Optional[str]:
    if not tenant_id:
        return None
    q = """
      select (value->>'kaspi_token') as kaspi_token
      from public.tenant_settings
      where tenant_id = %s and (key = 'settings' or key = 'config' or key = 'core')
      limit 1;
    """
    fall_q = """
      select (value->>'kaspi_token') as kaspi_token
      from public.tenant_settings
      where tenant_id = %s
      limit 1;
    """
    with _conn() as con, con.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(q, (tenant_id,))
        row = cur.fetchone()
        if not row or not row["kaspi_token"]:
            cur.execute(fall_q, (tenant_id,))
            row = cur.fetchone()
        tok = (row or {}).get("kaspi_token")
        tok = (tok or "").strip()
        return tok or None
