# app/deps/tenant.py
import json
from typing import Optional

# важно: берем коннектор из вашего app/db.py
from app.db import get_conn


def ensure_tenant_exists(tenant_id: str, email: Optional[str] = None) -> None:
    """
    Гарантируем, что запись в tenants существует (для FK на tenant_settings).
    Вызывается перед апсертом настроек.
    """
    ddl_tenants = """
    create table if not exists public.tenants (
        id uuid primary key,
        email text,
        phone text,
        created_at timestamptz default now(),
        is_active boolean default true
    );
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(ddl_tenants)
        cur.execute(
            "insert into public.tenants (id, email, is_active) values (%s, %s, true) on conflict (id) do nothing",
            (tenant_id, email),
        )
        conn.commit()


def _ensure_settings_table() -> None:
    ddl_settings = """
    create table if not exists public.tenant_settings (
        tenant_id uuid primary key
            references public.tenants(id) on delete cascade,
        value jsonb not null,
        updated_at timestamptz default now()
    );
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(ddl_settings)
        conn.commit()


def get_settings(tenant_id: str) -> Optional[dict]:
    _ensure_settings_table()
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("select value from public.tenant_settings where tenant_id=%s", (tenant_id,))
        row = cur.fetchone()
        return (row[0] if row else None)


def upsert_settings(tenant_id: str, value: dict) -> None:
    """
    Сохраняем JSON настроек для тенанта.
    - сначала гарантируем запись в tenants (иначе FK падает)
    - никакого created_at — только updated_at
    """
    ensure_tenant_exists(tenant_id)
    _ensure_settings_table()

    sql = """
    insert into public.tenant_settings (tenant_id, value)
    values (%s, %s::jsonb)
    on conflict (tenant_id) do update
      set value = excluded.value,
          updated_at = now()
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (tenant_id, json.dumps(value)))
        conn.commit()


def resolve_kaspi_token(tenant_id: Optional[str]) -> Optional[str]:
    """
    Достаём kaspi_token для текущего тенанта (используется в каспи-клиенте).
    """
    if not tenant_id:
        return None
    val = get_settings(tenant_id)
    if isinstance(val, dict):
        tok = val.get("kaspi_token") or val.get("KASPI_TOKEN")
        if isinstance(tok, str) and tok.strip():
            return tok.strip()
    return None
