# app/deps/tenant.py
from __future__ import annotations

import json
from typing import Optional
from app.db import get_conn


def _ensure_tenants_table() -> None:
    """
    Создаём минимальную таблицу tenants (если её ещё нет).
    Никаких лишних колонок, чтобы не упасть на старой схеме.
    """
    ddl = """
    create table if not exists public.tenants (
        id uuid primary key,
        created_at timestamptz default now()
    );
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(ddl)
        conn.commit()


def _ensure_settings_table() -> None:
    """
    Создаём таблицу настроек. Если в проде уже есть старая версия без PK/UNIQUE —
    мы всё равно сможем жить за счёт upsert через UPDATE→INSERT (см. ниже).
    """
    ddl = """
    create table if not exists public.tenant_settings (
        tenant_id uuid primary key,
        value jsonb not null,
        updated_at timestamptz default now()
    );
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(ddl)
        conn.commit()


def ensure_tenant_exists(tenant_id: str) -> None:
    """
    Гарантируем, что запись о тенанте есть.
    Вставляем ТОЛЬКО id — без несуществующих колонок типа email/phone.
    """
    _ensure_tenants_table()
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "insert into public.tenants(id) values (%s) on conflict (id) do nothing",
            (tenant_id,),
        )
        conn.commit()


def get_settings(tenant_id: str) -> Optional[dict]:
    """
    Возвращаем value JSON по тенанту, либо None.
    """
    _ensure_settings_table()
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "select value from public.tenant_settings where tenant_id = %s",
            (tenant_id,),
        )
        row = cur.fetchone()
        return row[0] if row else None


def upsert_settings(tenant_id: str, value: dict) -> None:
    """
    Универсальный upsert, который не требует UNIQUE/PK:
    1) UPDATE (если есть строка) → updated_at = now()
    2) Если обновлено 0 строк → INSERT
    """
    ensure_tenant_exists(tenant_id)
    _ensure_settings_table()
    payload = json.dumps(value)
    with get_conn() as conn, conn.cursor() as cur:
        # пробуем обновить
        cur.execute(
            "update public.tenant_settings set value = %s::jsonb, updated_at = now() where tenant_id = %s",
            (payload, tenant_id),
        )
        if cur.rowcount == 0:
            cur.execute(
                "insert into public.tenant_settings(tenant_id, value) values (%s, %s::jsonb)",
                (tenant_id, payload),
            )
        conn.commit()


def resolve_kaspi_token(tenant_id: Optional[str]) -> Optional[str]:
    """
    Достаём kaspi_token из настроек конкретного тенанта.
    """
    if not tenant_id:
        return None
    val = get_settings(tenant_id)
    if isinstance(val, dict):
        tok = val.get("kaspi_token") or val.get("KASPI_TOKEN")
        if isinstance(tok, str) and tok.strip():
            return tok.strip()
    return None
