import os
import psycopg
from psycopg.rows import dict_row
import re
def _normalize_dsn(url: str) -> str:
    # postgresql+psycopg:// -> postgresql://
    return re.sub(r"^postgresql\+[^:]+://", "postgresql://", url)

_DB_URL = os.getenv("DATABASE_URL", "").replace("postgresql+psycopg://", "postgresql://", 1)

def get_conn():
    return psycopg.connect(_DB_URL, row_factory=dict_row)

def fetchrow(sql, args=()):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, args)
        return cur.fetchone()

def fetchall(sql, args=()):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, args)
        return cur.fetchall()

def execute(sql, args=()):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, args)
