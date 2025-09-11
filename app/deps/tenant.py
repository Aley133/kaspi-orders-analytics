# app/deps/tenant.py
from __future__ import annotations
import json
from typing import Optional
from app.db import get_conn

def _ensure_settings_table() -> None:
    """
    Гарантирует наличие таблицы tenant_settings и PK по tenant_id.
    Если таблица уже была создана без PK — добавим.
    """
    ddl_create = """
    create table if not exists public.tenant_settings (
        tenant_id uuid primary key,
        value     jsonb not null,
        updated_at timestamptz default now()
    );
    """
    ddl_add_pk = """
    do $$
    begin
        if not exists (
            select 1
            from information_schema.table_constraints
            where table_schema = 'public'
              and table_name   = 'tenant_settings'
              and constraint_type = 'PRIMARY KEY'
        ) then
            alter table public.tenant_settings
                add constraint tenant_settings_pkey primary key (tenant_id);
        end if;
    exception
        when duplicate_table then null;
        when duplicate_object then null;
    end$$;
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(ddl_create)
        cur.execute(ddl_add_pk)
        conn.commit()

def get_settings(tenant_id: str) -> Optional[dict]:
    _ensure_settings_table()
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "select value from public.tenant_settings where tenant_id = %s::uuid",
            (tenant_id,)
        )
        row = cur.fetchone()
        return row[0] if row else None

def upsert_settings(tenant_id: str, value: dict) -> None:
    """
    Без ON CONFLICT: сначала UPDATE (меняем updated_at),
    если не затронули ни одной строки — INSERT.
    """
    _ensure_settings_table()
    payload = json.dumps(value)
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "update public.tenant_settings "
            "set value = %s::jsonb, updated_at = now() "
            "where tenant_id = %s::uuid",
            (payload, tenant_id),
        )
        if cur.rowcount == 0:
            cur.execute(
                "insert into public.tenant_settings (tenant_id, value) "
                "values (%s::uuid, %s::jsonb)",
                (tenant_id, payload),
            )
        conn.commit()

def resolve_kaspi_token(tenant_id: Optional[str]) -> Optional[str]:
    if not tenant_id:
        return None
    data = get_settings(tenant_id)
    if isinstance(data, dict):
        tok = data.get("kaspi_token") or data.get("KASPI_TOKEN")
        if isinstance(tok, str) and tok.strip():
            return tok.strip()
    return None

def get_settings_row(tenant_id: str):
    """Alias для старого импорта из settings.py."""
    return get_settings(tenant_id)
