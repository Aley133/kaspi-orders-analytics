# app/deps/tenant.py
from __future__ import annotations

import json
from typing import Optional
from app.db import get_conn


def ensure_tenant_tables() -> None:
    """
    Создаём минимально необходимые таблицы, если их ещё нет.
    Никаких доп. колонок (email/phone) не требуем — только id.
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            create table if not exists public.tenants (
                id uuid primary key
            );
        """)
        cur.execute("""
            create table if not exists public.tenant_settings (
                tenant_id uuid primary key
                    references public.tenants(id) on delete cascade,
                value jsonb not null,
                updated_at timestamptz default now()
            );
        """)
        conn.commit()


def ensure_tenant_exists(tenant_id: str) -> None:
    """
    Гарантируем, что запись в tenants существует.
    Вставляем ТОЛЬКО id — без email/phone, чтобы не падало на чужой схеме.
    """
    ensure_tenant_tables()
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "insert into public.tenants (id) values (%s) on conflict (id) do nothing",
            (tenant_id,),
        )
        conn.commit()


def get_settings_row(tenant_id: str) -> Optional[dict]:
    """
    Возвращает JSON-настройки или None.
    """
    ensure_tenant_tables()
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "select value from public.tenant_settings where tenant_id=%s",
            (tenant_id,),
        )
        row = cur.fetchone()
        return row[0] if row else None


def get_settings(tenant_id: str) -> Optional[dict]:
    # alias для совместимости с существующими импортами
    return get_settings_row(tenant_id)


def upsert_settings(tenant_id: str, value: dict) -> None:
    """
    Создаёт/обновляет JSON-настройки тенанта.
    """
    ensure_tenant_exists(tenant_id)
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            insert into public.tenant_settings (tenant_id, value)
            values (%s, %s::jsonb)
            on conflict (tenant_id)
            do update set value = excluded.value,
                          updated_at = now()
            """,
            (tenant_id, json.dumps(value)),
        )
        conn.commit()


def resolve_kaspi_token(tenant_id: Optional[str]) -> Optional[str]:
    """
    Достаём kaspi_token из сохранённых настроек.
    """
    if not tenant_id:
        return None
    data = get_settings_row(tenant_id)
    if isinstance(data, dict):
        tok = data.get("kaspi_token") or data.get("KASPI_TOKEN")
        if isinstance(tok, str) and tok.strip():
            return tok.strip()
    return None
