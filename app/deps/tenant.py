# deps/tenant.py
import os, json
import psycopg  # psycopg3
from typing import Optional

_DB_URL = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL") or os.getenv("PROFIT_DB_URL")

def _conn():
    # снимаем кавычки, приводим схемы
    dsn = (_DB_URL or "").strip().strip('"').replace("postgresql+psycopg://","postgresql://")
    return psycopg.connect(dsn, autocommit=True)

def get_settings_row(tenant_id: str) -> Optional[dict]:
    sql = """
    select value
    from public.tenant_settings
    where tenant_id = %s and key = 'settings'
    limit 1
    """
    with _conn() as cx, cx.cursor() as cur:
        cur.execute(sql, (tenant_id,))
        row = cur.fetchone()
        if not row: return None
        return row[0]  # jsonb as dict

def ensure_tenant_exists(tenant_id: str):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            create table if not exists public.tenants (
              id uuid primary key,
              email text,
              phone text,
              created_at timestamptz default now(),
              is_active boolean default true
            );
        """)
        cur.execute(
            "insert into public.tenants (id) values (%s) on conflict (id) do nothing",
            (tenant_id,)
        )

def upsert_settings(tenant_id: str, value: dict):
    ensure_tenant_exists(tenant_id)  # <--- ДО апсерта настроек
    with get_conn() as conn, conn.cursor() as cur:
        sql = """
        insert into public.tenant_settings (tenant_id, key, value)
        values (%s, 'settings', %s::jsonb)
        on conflict (tenant_id, key) do update
          set value = excluded.value,
              updated_at = now();
        """
        cur.execute(sql, (tenant_id, json.dumps(value)))
        conn.commit()

def resolve_kaspi_token(tenant_id: str) -> Optional[str]:
    row = get_settings_row(tenant_id)
    if not row: return None
    token = (row or {}).get("kaspi_token") or ""
    return token.strip() or None
