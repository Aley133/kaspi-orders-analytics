import os, re, psycopg
from psycopg.rows import dict_row

def _normalize_dsn(url: str) -> str:
    # postgresql+psycopg:// -> postgresql://
    return re.sub(r"^postgresql\+[^:]+://", "postgresql://", url)

_DB_URL = _normalize_dsn(os.getenv("DATABASE_URL", ""))

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
