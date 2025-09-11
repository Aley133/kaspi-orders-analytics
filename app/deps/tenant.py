# app/deps/tenant.py
from __future__ import annotations

import json
from typing import Optional
from app.db import get_conn  # коннектор к БД

TENANTS_TABLE = "public.tenants"
SETTINGS_TABLE = "public.tenant_settings"


def _ensure_schema() -> None:
    """Гарантируем, что нужные таблицы существуют."""
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(f"""
            create table if not exists {TENANTS_TABLE} (
                id uuid primary key,
                email text,
                phone text,
                created_at timestamptz default now(),
                is_active boolean default true
            );
        """)
        cur.execute(f"""
            create table if not exists {SETTINGS_TABLE} (
                tenant_id uuid primary key
                    references {TENANTS_TABLE}(id) on delete cascade,
                value jsonb not null,
                updated_at timestamptz default now()
            );
        """)
        conn.commit()


def ensure_tenant_exists(tenant_id: str, email: Optional[str] = None) -> None:
    """Создаём запись о тенанте (если её нет)."""
    _ensure_schema()
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            insert into {TENANTS_TABLE} (id, email, is_active)
            values (%s, %s, true)
            on conflict (id) do nothing
            """,
            (tenant_id, email),
        )
        conn.commit()


def load_settings(tenant_id: str) -> Optional[dict]:
    """Оставил для совместимости — читает из настроек."""
    return get_settings(tenant_id)


def get_settings(tenant_id: str) -> Optional[dict]:
    """Возвращает JSON с настройками тенанта, либо None."""
    _ensure_schema()
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            f"select value from {SETTINGS_TABLE} where tenant_id=%s",
            (tenant_id,),
        )
        row = cur.fetchone()
        return row[0] if row else None


def upsert_settings(tenant_id: str, value: dict) -> None:
    """Сохраняет настройки (insert … on conflict do update)."""
    ensure_tenant_exists(tenant_id)
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            insert into {SETTINGS_TABLE} (tenant_id, value)
            values (%s, %s::jsonb)
            on conflict (tenant_id) do update
                set value = excluded.value,
                    updated_at = now()
            """,
            (tenant_id, json.dumps(value)),
        )
        conn.commit()


def resolve_kaspi_token(tenant_id: Optional[str]) -> Optional[str]:
    """Достаём kaspi_token из настроек данного тенанта."""
    if not tenant_id:
        return None
    val = get_settings(tenant_id)
    if isinstance(val, dict):
        tok = val.get("kaspi_token") or val.get("KASPI_TOKEN")
        if isinstance(tok, str) and tok.strip():
            return tok.strip()
    return None
