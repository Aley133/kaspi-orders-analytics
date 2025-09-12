# app/deps/tenant.py
from __future__ import annotations

import json
from typing import Optional
from app.db import get_conn

SETTINGS_KEY = "settings"  # одна запись на тенанта


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


def _fetch_col_names(cur) -> set[str]:
    cur.execute("""
      select column_name
      from information_schema.columns
      where table_schema='public' and table_name='tenant_settings';
    """)
    cols: set[str] = set()
    for r in cur.fetchall():
        if isinstance(r, dict):
            cols.add(r.get("column_name"))
        else:
            cols.add(r[0])
    return {c for c in cols if c}


def _ensure_settings_table(cur) -> None:
    # создаём таблицу, если её нет — сразу в корректной схеме
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

    cols = _fetch_col_names(cur)

    if "key" not in cols:
        cur.execute("alter table public.tenant_settings add column key text;")

    cur.execute("alter table public.tenant_settings alter column key set default 'settings';")
    cur.execute("update public.tenant_settings set key='settings' where key is null;")
    cur.execute("alter table public.tenant_settings alter column key set not null;")

    # если нет PK — добавим
    cur.execute("""
    do $$
    begin
      if not exists (
        select 1
        from   pg_constraint
        where  conrelid = 'public.tenant_settings'::regclass
        and    contype  = 'p'
      ) then
        alter table public.tenant_settings
          add constraint tenant_settings_pkey primary key (tenant_id, key);
      end if;
    end $$;
    """)

    cur.execute("""
    create unique index if not exists tenant_settings_tenant_id_key_idx
      on public.tenant_settings(tenant_id, key);
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
        if not row:
            return None
        return row.get("value") if isinstance(row, dict) else row[0]


# для совместимости с существующими import'ами
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
    st = get_settings(tenant_id)
    if isinstance(st, dict):
        tok = st.get("kaspi_token") or st.get("KASPI_TOKEN")
        if isinstance(tok, str) and tok.strip():
            return tok.strip()
    return None
