# app/deps/tenant.py
from fastapi import Depends
from app.deps.auth import get_current_user
from app import db

def require_tenant(user = Depends(get_current_user)) -> str:
    # пробуем найти привязку
    row = db.fetchrow(
        "select tenant_id from org_members where user_id = %s limit 1",
        [user["user_id"]],
    )
    if row:
        return row["tenant_id"]

    # нет — создаём тенант и членство
    ten = db.fetchrow(
        "insert into tenants(name) values (%s) returning id",
        [user.get("email") or "My Shop"]
    )
    db.execute(
        "insert into org_members(user_id, tenant_id, role) values (%s,%s,'owner') "
        "on conflict (user_id, tenant_id) do nothing",
        [user["user_id"], ten["id"]],
    )

    # (опционально) создаём пустые настройки для удобства
    db.execute(
        """
        insert into tenant_settings(tenant_id, key, value)
        values
          (%s, 'kaspi.partner_id', jsonb_build_object('v','')),
          (%s, 'kaspi.token',       jsonb_build_object('v',''))
        on conflict (tenant_id, key) do nothing
        """,
        [ten["id"], ten["id"]],
    )
    return ten["id"]

