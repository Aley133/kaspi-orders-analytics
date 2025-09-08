# app/db.py
import os, psycopg
from psycopg.rows import dict_row

_DB_URL = os.getenv("DATABASE_URL")

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

