# app/deps/tenant.py
from __future__ import annotations
import json
from typing import Optional

from app.db import get_conn

SETTINGS_KEY = "settings"  # одна запись настроек на тенанта

def _ensure_tenants_table(cur) -> None:
    cur.execute("""
    create table if not exists public.tenants (
        id uuid primary key,
        email text,
        phone text,
        created_at timestamptz default now(),
        is_active boolean default true
    );
    """)

def _ensure_settings_table(cur) -> None:
    """
    Приводим public.tenant_settings к унифицированной схеме:
      tenant_id uuid not null  (FK -> tenants)
      key       text not null default 'settings'
      value     jsonb not null
      updated_at timestamptz default now()
      PRIMARY KEY (tenant_id, key)
    При наличии «легаси»-версий добавляем/чинем столбцы и ограничения.
    """
    # Если таблицы нет — создаём сразу правильную
    cur.execute("""
    create table if not exists public.tenant_settings (
        tenant_id uuid not null
            references public.tenants(id) on delete cascade,
        key text not null default 'settings',
        value jsonb not null,
        updated_at timestamptz default now(),
        primary key (tenant_id, key)
    );
    """)

    # Инвентаризация колонок (на случай, если она уже была в другой схеме)
    cur.execute("""
      select column_name
      from information_schema.columns
      where table_schema='public' and table_name='tenant_settings';
    """)
    cols = {r[0] for r in cur.fetchall()}

    if 'key' not in cols:
        cur.execute("alter table public.tenant_settings add column key text;")

    # дефолт и not null для key
    cur.execute("alter table public.tenant_settings alter column key set default 'settings';")
    cur.execute("update public.tenant_settings set key='settings' where key is null;")
    cur.execute("alter table public.tenant_settings alter column key set not null;")

    # убедимся, что есть уникальность/PK по (tenant_id, key)
    cur.execute("""
    do $$
    begin
      if not exists (
        select 1 from pg_indexes
        where schemaname = 'public'
          and tablename  = 'tenant_settings'
          and indexname  = 'tenant_settings_tenant_id_key_idx'
      ) then
        -- индекс (на случай, если PK уже есть — индекс просто появится)
        create unique index tenant_settings_tenant_id_key_idx
          on public.tenant_settings(tenant_id, key);
      end if;
      -- если primary key отсутствует — добавим
      if not exists (
        select 1 from pg_constraint
        where conrelid = 'public.tenant_settings'::regclass
          and contype = 'p'
      ) then
        alter table public.tenant_settings
          add constraint tenant_settings_pkey primary key (tenant_id, key);
      end if;
    end $$;
    """)

def ensure_tenant_exists(tenant_id: str, email: Optional[str] = None) -> None:
    with get_conn() as conn, conn.cursor() as cur:
        _ensure_tenants_table(cur)
        cur.execute("""
          insert into public.tenants (id, email, is_active)
          values (%s, %s, true)
          on conflict (id) do nothing;
        """, (tenant_id, email))
        conn.commit()

def get_settings(tenant_id: str) -> Optional[dict]:
    with get_conn() as conn, conn.cursor() as cur:
        _ensure_tenants_table(cur)
        _ensure_settings_table(cur)
        cur.execute("""
          select value
          from public.tenant_settings
          where tenant_id=%s and key=%s
          limit 1;
        """, (tenant_id, SETTINGS_KEY))
        row = cur.fetchone()
        return row[0] if row else None

# Чтобы не рушить импорты в settings.py
def get_settings_row(tenant_id: str) -> Optional[dict]:
    return get_settings(tenant_id)

def upsert_settings(tenant_id: str, value: dict) -> None:
    with get_conn() as conn, conn.cursor() as cur:
        _ensure_tenants_table(cur)
        _ensure_settings_table(cur)
        cur.execute("""
          insert into public.tenant_settings (tenant_id, key, value, updated_at)
          values (%s, %s, %s::jsonb, now())
          on conflict (tenant_id, key)
          do update set value = excluded.value, updated_at = now();
        """, (tenant_id, SETTINGS_KEY, json.dumps(value)))
        conn.commit()

def resolve_kaspi_token(tenant_id: Optional[str]) -> Optional[str]:
    if not tenant_id:
        return None
    val = get_settings(tenant_id)
    if isinstance(val, dict):
        tok = val.get("kaspi_token") or val.get("KASPI_TOKEN")
        if isinstance(tok, str) and tok.strip():
            return tok.strip()
    return None
