# app/deps/tenant.py
from __future__ import annotations

import json
from typing import Optional
from app.db import get_conn

TENANTS_DDL = """
create table if not exists public.tenants (
    id uuid primary key,
    email text,
    phone text,
    created_at timestamptz default now(),
    is_active boolean default true
);
"""

SETTINGS_DDL = """
create table if not exists public.tenant_settings (
    tenant_id uuid primary key
        references public.tenants(id) on delete cascade,
    value jsonb not null,
    updated_at timestamptz default now()
);
"""

def _ensure_tables() -> None:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(TENANTS_DDL)
        cur.execute(SETTINGS_DDL)
        conn.commit()

def ensure_tenant_exists(tenant_id: str, email: Optional[str] = None) -> None:
    _ensure_tables()
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "insert into public.tenants (id, email, is_active) "
            "values (%s, %s, true) on conflict (id) do nothing",
            (tenant_id, email),
        )
        conn.commit()

def get_settings_row(tenant_id: str) -> Optional[dict]:
    _ensure_tables()
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "select value from public.tenant_settings where tenant_id=%s",
            (tenant_id,),
        )
        row = cur.fetchone()
        return row[0] if row else None

def upsert_settings(tenant_id: str, value: dict) -> None:
    _ensure_tables()
    ensure_tenant_exists(tenant_id)
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            insert into public.tenant_settings(tenant_id, value)
            values (%s, %s::jsonb)
            on conflict (tenant_id) do update set
              value = excluded.value,
              updated_at = now()
            """,
            (tenant_id, json.dumps(value)),
        )
        conn.commit()

def resolve_kaspi_token(tenant_id: Optional[str]) -> Optional[str]:
    if not tenant_id:
        return None
    row = get_settings_row(tenant_id)
    if isinstance(row, dict):
        tok = row.get("kaspi_token") or row.get("KASPI_TOKEN")
        if isinstance(tok, str) and tok.strip():
            return tok.strip()
    return None
