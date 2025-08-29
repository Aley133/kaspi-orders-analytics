# app/api/products.py
from __future__ import annotations

from typing import Callable, Optional, List, Dict, Any, Tuple
from fastapi import APIRouter, HTTPException, Query, UploadFile, File, Body, Depends, Request
from fastapi.responses import Response, FileResponse
from pydantic import BaseModel
import secrets
import io
import os, shutil
import sqlite3
import re
from xml.etree import ElementTree as ET
from contextlib import contextmanager
from datetime import datetime, timedelta, time as dt_time

# ──────────────────────────────────────────────────────────────────────────────
# API-ключ (для write-операций)
# ──────────────────────────────────────────────────────────────────────────────
def require_api_key(req: Request) -> bool:
    api_key = os.getenv("API_KEY")
    if not api_key:
        return True
    sent = req.headers.get("X-API-Key") or req.query_params.get("api_key")
    if sent != api_key:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return True

# ──────────────────────────────────────────────────────────────────────────────
# Optional deps (Excel)
# ──────────────────────────────────────────────────────────────────────────────
try:
    import openpyxl
    _OPENPYXL_AVAILABLE = True
except Exception:
    _OPENPYXL_AVAILABLE = False

# ──────────────────────────────────────────────────────────────────────────────
# Optional imports from kaspi_client
# ──────────────────────────────────────────────────────────────────────────────
try:
    from app.kaspi_client import ProductStock, normalize_row  # type: ignore
except Exception:
    try:
        from ..kaspi_client import ProductStock, normalize_row  # type: ignore
    except Exception:
        try:
            from kaspi_client import ProductStock, normalize_row  # type: ignore
        except Exception:
            ProductStock = None
            normalize_row = None

# ──────────────────────────────────────────────────────────────────────────────
# DB backend switch (PG via SQLAlchemy / fallback SQLite)
# ──────────────────────────────────────────────────────────────────────────────
try:
    from sqlalchemy import create_engine, text  # type: ignore
    _SQLA_OK = True
except Exception:
    _SQLA_OK = False

DATABASE_URL = os.getenv("DATABASE_URL")  # postgresql+psycopg://... (Neon)
_USE_PG = bool(DATABASE_URL and _SQLA_OK)

def _resolve_db_path() -> str:
    target = os.getenv("DB_PATH", "/data/kaspi-orders.sqlite3")
    target_dir = os.path.dirname(target)
    try:
        os.makedirs(target_dir, exist_ok=True)
        if os.access(target_dir, os.W_OK):
            return target
    except Exception:
        pass
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data.sqlite3"))

DB_PATH = _resolve_db_path()
_OLD_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data.sqlite3"))
if DB_PATH != _OLD_PATH and os.path.exists(_OLD_PATH) and not os.path.exists(DB_PATH):
    try:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        shutil.copy2(_OLD_PATH, DB_PATH)
    except Exception:
        pass

if _USE_PG:
    _engine = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)

@contextmanager
def _db():
    # Явный commit/rollback для SQLite; в PG используем transactional begin()
    if _USE_PG:
        with _engine.begin() as conn:
            yield conn
    else:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

def _q(sql: str):
    return text(sql) if _USE_PG else sql

def _rows_to_dicts(rows):
    if _USE_PG:
        return [dict(r._mapping) for r in rows]
    return [dict(r) for r in rows]

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
def _gen_batch_code_sqlite(conn: sqlite3.Connection) -> str:
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    while True:
        code = "".join(secrets.choice(alphabet) for _ in range(6))
        if not conn.execute("SELECT 1 FROM batches WHERE batch_code=?", (code,)).fetchone():
            return code

def _table_exists(c, name: str) -> bool:
    if _USE_PG:
        r = c.execute(_q(
            "SELECT 1 FROM information_schema.tables WHERE table_name=:t"
        ), {"t": name}).first()
        return bool(r)
    else:
        r = c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone()
        return bool(r)

def _has_column(c, table: str, col: str) -> bool:
    if _USE_PG:
        r = c.execute(_q("""
            SELECT 1 FROM information_schema.columns
             WHERE table_name=:t AND column_name=:c
        """), {"t": table, "c": col}).first()
        return bool(r)
    else:
        rows = c.execute(f"PRAGMA table_info({table})").fetchall()
        cols = {row["name"] for row in rows}
        return col in cols

def _lower_cols(c, table: str) -> set[str]:
    if _USE_PG:
        rows = c.execute(_q(
            "SELECT column_name FROM information_schema.columns WHERE table_name=:t"
        ), {"t": table}).all()
        return {str(r._mapping["column_name"]).lower() for r in rows}
    else:
        rows = c.execute(f"PRAGMA table_info({table})").fetchall()
        return {str(r["name"]).lower() for r in rows}

# ──────────────────────────────────────────────────────────────────────────────
# Schema & migrations
# ──────────────────────────────────────────────────────────────────────────────
def _ensure_schema():
    if _USE_PG:
        with _db() as c:
            # products
            c.execute(_q("""
            CREATE TABLE IF NOT EXISTS products(
                sku TEXT PRIMARY KEY,
                name TEXT,
                brand TEXT,
                category TEXT,
                price DOUBLE PRECISION,
                quantity INTEGER,
                active INTEGER DEFAULT 1,
                barcode TEXT,
                updated_at TIMESTAMP DEFAULT NOW()
            );
            """))
            # batches
            c.execute(_q("""
            CREATE TABLE IF NOT EXISTS batches(
                id SERIAL PRIMARY KEY,
                sku TEXT NOT NULL REFERENCES products(sku) ON DELETE CASCADE,
                date DATE NOT NULL,
                qty INTEGER NOT NULL,
                unit_cost DOUBLE PRECISION NOT NULL,
                note TEXT,
                commission_pct DOUBLE PRECISION,
                batch_code TEXT UNIQUE,
                qty_sold INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT NOW()
            );
            """))
            # categories
            c.execute(_q("""
            CREATE TABLE IF NOT EXISTS categories(
                name TEXT PRIMARY KEY,
                base_percent DOUBLE PRECISION DEFAULT 0.0,
                extra_percent DOUBLE PRECISION DEFAULT 3.0,
                tax_percent DOUBLE PRECISION DEFAULT 0.0
            );
            """))
            # writeoffs
            c.execute(_q("""
            CREATE TABLE IF NOT EXISTS writeoffs(
                id SERIAL PRIMARY KEY,
                ts TIMESTAMP DEFAULT NOW(),
                sku TEXT NOT NULL,
                qty INTEGER NOT NULL,
                note TEXT,
                batch_id INTEGER
            );
            """))
            c.execute(_q("CREATE INDEX IF NOT EXISTS idx_writeoffs_ts ON writeoffs(ts)"))
            c.execute(_q("CREATE INDEX IF NOT EXISTS idx_writeoffs_sku ON writeoffs(sku)"))
            # migrations: batches.qty_sold
            c.execute(_q("""
            DO $$
            BEGIN
              IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                 WHERE table_name='batches' AND column_name='qty_sold'
              ) THEN
                ALTER TABLE batches ADD COLUMN qty_sold INTEGER DEFAULT 0;
              END IF;
            END$$;
            """))
            # migrations: writeoffs add order fields
            c.execute(_q("""
            DO $$
            BEGIN
              IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                 WHERE table_name='writeoffs' AND column_name='order_id'
              ) THEN
                ALTER TABLE writeoffs ADD COLUMN order_id   TEXT;
              END IF;
              IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                 WHERE table_name='writeoffs' AND column_name='order_code'
              ) THEN
                ALTER TABLE writeoffs ADD COLUMN order_code TEXT;
              END IF;
              IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                 WHERE table_name='writeoffs' AND column_name='line_index'
              ) THEN
                ALTER TABLE writeoffs ADD COLUMN line_index INTEGER;
              END IF;
            END$$;
            """))
            # order_writeoffs table
            c.execute(_q("""
            CREATE TABLE IF NOT EXISTS order_writeoffs(
              id SERIAL PRIMARY KEY,
              ts TIMESTAMP DEFAULT NOW(),
              order_id   TEXT NOT NULL,
              order_code TEXT,
              line_index INTEGER NOT NULL,
              sku        TEXT NOT NULL,
              qty        INTEGER NOT NULL,
              unit_price DOUBLE PRECISION,
              total_price DOUBLE PRECISION,
              note       TEXT,
              CONSTRAINT order_writeoffs_uniq UNIQUE(order_id, line_index)
            );
            """))
            c.execute(_q("CREATE INDEX IF NOT EXISTS ix_order_writeoffs_ts ON order_writeoffs(ts)"))
            c.execute(_q("CREATE INDEX IF NOT EXISTS ix_order_writeoffs_sku ON order_writeoffs(sku)"))

            c.execute(_q("CREATE INDEX IF NOT EXISTS idx_batches_sku  ON batches(sku)"))
            c.execute(_q("CREATE INDEX IF NOT EXISTS idx_batches_date ON batches(date)"))

            # fill batch_code for old rows
            c.execute(_q("""
            UPDATE batches
               SET batch_code = CONCAT(
                    SUBSTRING('ABCDEFGHJKLMNPQRSTUVWXYZ23456789' FROM floor(random()*32)::int + 1 FOR 1),
                    SUBSTRING('ABCDEFGHJKLMNPQRSTUVWXYZ23456789' FROM floor(random()*32)::int + 1 FOR 1),
                    SUBSTRING('ABCDEFGHJKLMNPQRSTUVWXYZ23456789' FROM floor(random()*32)::int + 1 FOR 1),
                    SUBSTRING('ABCDEFGHJKLMNPQRSTUVWXYZ23456789' FROM floor(random()*32)::int + 1 FOR 1),
                    SUBSTRING('ABCDEFGHJKLMNPQRSTUVWXYZ23456789' FROM floor(random()*32)::int + 1 FOR 1),
                    SUBSTRING('ABCDEFGHJKLMNPQRSTUVWXYZ23456789' FROM floor(random()*32)::int + 1 FOR 1)
               )
             WHERE (batch_code IS NULL OR batch_code='');
            """))
    else:
        with _db() as c:
            c.executescript("""
            PRAGMA journal_mode=WAL;

            CREATE TABLE IF NOT EXISTS products(
                sku TEXT PRIMARY KEY,
                name TEXT,
                brand TEXT,
                category TEXT,
                price REAL,
                quantity INTEGER,
                active INTEGER DEFAULT 1,
                barcode TEXT,
                updated_at TEXT
            );

            CREATE TABLE IF NOT EXISTS batches(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sku TEXT NOT NULL,
                date TEXT NOT NULL,
                qty INTEGER NOT NULL,
                unit_cost REAL NOT NULL,
                note TEXT,
                commission_pct REAL,
                batch_code TEXT,
                qty_sold INTEGER DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(sku) REFERENCES products(sku) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS categories(
                name TEXT PRIMARY KEY,
                base_percent REAL DEFAULT 0.0,
                extra_percent REAL DEFAULT 3.0,
                tax_percent REAL DEFAULT 0.0
            );

            CREATE TABLE IF NOT EXISTS writeoffs(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT DEFAULT CURRENT_TIMESTAMP,
                sku TEXT NOT NULL,
                qty INTEGER NOT NULL,
                note TEXT,
                batch_id INTEGER
            );
            """)
            # batches columns
            cols_b = {r["name"] for r in c.execute("PRAGMA table_info(batches)")}
            if "commission_pct" not in cols_b:
                c.execute("ALTER TABLE batches ADD COLUMN commission_pct REAL")
            if "batch_code" not in cols_b:
                c.execute("ALTER TABLE batches ADD COLUMN batch_code TEXT")
            if "qty_sold" not in cols_b:
                c.execute("ALTER TABLE batches ADD COLUMN qty_sold INTEGER DEFAULT 0")
            # writeoffs columns (order context)
            cols_w = {r["name"] for r in c.execute("PRAGMA table_info(writeoffs)")}
            if "order_id" not in cols_w:
                c.execute("ALTER TABLE writeoffs ADD COLUMN order_id TEXT")
            if "order_code" not in cols_w:
                c.execute("ALTER TABLE writeoffs ADD COLUMN order_code TEXT")
            if "line_index" not in cols_w:
                c.execute("ALTER TABLE writeoffs ADD COLUMN line_index INTEGER")

            # order_writeoffs table + indexes
            c.executescript("""
            CREATE TABLE IF NOT EXISTS order_writeoffs(
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts TEXT DEFAULT CURRENT_TIMESTAMP,
              order_id   TEXT NOT NULL,
              order_code TEXT,
              line_index INTEGER NOT NULL,
              sku        TEXT NOT NULL,
              qty        INTEGER NOT NULL,
              unit_price REAL,
              total_price REAL,
              note       TEXT,
              UNIQUE(order_id, line_index)
            );
            CREATE INDEX IF NOT EXISTS ix_order_writeoffs_ts  ON order_writeoffs(ts);
            CREATE INDEX IF NOT EXISTS ix_order_writeoffs_sku ON order_writeoffs(sku);
            """)

            c.execute("CREATE INDEX IF NOT EXISTS idx_batches_sku  ON batches(sku)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_batches_date ON batches(date)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_writeoffs_ts  ON writeoffs(ts)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_writeoffs_sku ON writeoffs(sku)")
            # проставить batch_code где пусто
            for r in c.execute("SELECT id FROM batches WHERE batch_code IS NULL OR batch_code=''").fetchall():
                c.execute("UPDATE batches SET batch_code=? WHERE id=?", (_gen_batch_code_sqlite(c), r["id"]))

def _seed_categories_if_empty():
    _ensure_schema()
    with _db() as c:
        if _USE_PG:
            cnt = c.execute(_q("SELECT COUNT(*) AS c FROM categories")).scalar_one()
        else:
            cnt = c.execute("SELECT COUNT(*) AS c FROM categories").fetchone()["c"]
        if int(cnt) == 0:
            defaults = [
                ("Витамины/БАДы", 10.0, 3.0, 0.0),
                ("Сад/освещение", 10.0, 3.0, 0.0),
                ("Товары для дома", 10.0, 3.0, 0.0),
                ("Прочее", 10.0, 3.0, 0.0),
            ]
            if _USE_PG:
                for n,b,e,t in defaults:
                    c.execute(_q("""
                        INSERT INTO categories(name,base_percent,extra_percent,tax_percent)
                        VALUES(:n,:b,:e,:t)
                        ON CONFLICT (name) DO NOTHING
                    """), {"n": n, "b": b, "e": e, "t": t})
            else:
                c.executemany(
                    "INSERT OR IGNORE INTO categories(name,base_percent,extra_percent,tax_percent) VALUES(?,?,?,?)",
                    defaults
                )

# ──────────────────────────────────────────────────────────────────────────────
# Utils
# ──────────────────────────────────────────────────────────────────────────────
def _to_float(v) -> float:
    try:
        return float(str(v).replace(" ", "").replace(",", "."))
    except Exception:
        return 0.0

def _to_int(v) -> int:
    try:
        return int(float(str(v).replace(" ", "").replace(",", ".")))
    except Exception:
        return 0

def _sku_of(row: dict) -> str:
    return str(
        row.get("code") or row.get("sku") or row.get("vendorCode")
        or row.get("barcode") or row.get("Barcode") or row.get("id") or ""
    ).strip()

def _avg_cost(sku: str) -> float | None:
    _ensure_schema()
    with _db() as c:
        if _USE_PG:
            r = c.execute(_q(
                "SELECT SUM(qty*unit_cost) AS tc, SUM(qty) AS tq FROM batches WHERE sku=:sku"
            ), {"sku": sku}).first()
            if not r or not r.tq:
                return None
            return float(r.tc) / float(r.tq)
        else:
            r = c.execute(
                "SELECT SUM(qty*unit_cost) AS tc, SUM(qty) AS tq FROM batches WHERE sku=?",
                (sku,)
            ).fetchone()
            if not r or not r["tq"]:
                return None
            return float(r["tc"]) / float(r["tq"])

def _leftovers_by_sku(c, sku: str) -> List[Dict[str, Any]]:
    if _USE_PG:
        rows = c.execute(_q("""
            SELECT id, date, qty, COALESCE(qty_sold,0) AS qty_sold,
                   (qty-COALESCE(qty_sold,0)) AS left, unit_cost, commission_pct
              FROM batches
             WHERE sku=:sku AND (qty-COALESCE(qty_sold,0)) > 0
             ORDER BY date, id
        """), {"sku": sku}).all()
        return _rows_to_dicts(rows)
    else:
        rows = c.execute("""
            SELECT id, date, qty, COALESCE(qty_sold,0) AS qty_sold,
                   (qty-COALESCE(qty_sold,0)) AS left, unit_cost, commission_pct
              FROM batches
             WHERE sku=? AND (qty-COALESCE(qty_sold,0)) > 0
             ORDER BY date, id
        """, (sku,)).fetchall()
        return [dict(r) for r in rows]

def _sum_leftovers(c, sku: str) -> int:
    if _USE_PG:
        val = c.execute(_q("""
            SELECT COALESCE(SUM(qty-COALESCE(qty_sold,0)),0)
              FROM batches WHERE sku=:sku
        """), {"sku": sku}).scalar_one()
        return int(val or 0)
    else:
        row = c.execute("""
            SELECT COALESCE(SUM(qty-COALESCE(qty_sold,0)),0) AS left
              FROM batches WHERE sku=?
        """, (sku,)).fetchone()
        return int(row["left"] or 0)

def _recompute_product_qty(c, sku: str):
    left_total = _sum_leftovers(c, sku)
    if _USE_PG:
        c.execute(_q("UPDATE products SET quantity=:q, updated_at=NOW() WHERE sku=:sku"),
                  {"q": left_total, "sku": sku})
    else:
        c.execute("UPDATE products SET quantity=?, updated_at=datetime('now') WHERE sku=?",
                  (left_total, sku))
    return left_total

def _recompute_all_products_qty():
    _ensure_schema()
    with _db() as c:
        if _USE_PG:
            c.execute(_q("""
                UPDATE products p
                   SET quantity = COALESCE(b.left,0), updated_at = NOW()
                  FROM (
                        SELECT sku, SUM(qty-COALESCE(qty_sold,0)) AS left
                          FROM batches GROUP BY sku
                       ) b
                 WHERE p.sku = b.sku
            """))
            c.execute(_q("""
                UPDATE products p
                   SET quantity = 0, updated_at = NOW()
                 WHERE NOT EXISTS (SELECT 1 FROM batches b WHERE b.sku = p.sku)
            """))
        else:
            c.execute("UPDATE products SET quantity=0, updated_at=datetime('now')")
            rows = c.execute("""
                SELECT sku, SUM(qty-COALESCE(qty_sold,0)) AS left
                  FROM batches GROUP BY sku
            """).fetchall()
            for r in rows:
                c.execute("UPDATE products SET quantity=?, updated_at=datetime('now') WHERE sku=?",
                          (int(r["left"] or 0), r["sku"]))

# ──────────────────────────────────────────────────────────────────────────────
# Upsert products
# ──────────────────────────────────────────────────────────────────────────────
def _upsert_products(items: List[Dict[str, Any]]) -> Tuple[int,int]:
    _ensure_schema()
    inserted = updated = 0
    with _db() as c:
        for it in items:
            sku = _sku_of(it)
            if not sku:
                continue
            name = it.get("name") or it.get("model") or it.get("title") or sku
            brand = it.get("brand") or it.get("vendor")
            category = it.get("category")
            price = _to_float(it.get("price"))
            qty = _to_int(it.get("qty") or it.get("quantity") or it.get("stock"))
            barcode = it.get("barcode") or it.get("Barcode")
            active = it.get("active")
            if isinstance(active, str):
                active = 1 if active.lower() in ("1","true","yes","on") else 0
            elif isinstance(active, bool):
                active = 1 if active else 0
            else:
                active = 1 if qty > 0 else 0

            if _USE_PG:
                existed = c.execute(_q("SELECT 1 FROM products WHERE sku=:sku"), {"sku": sku}).first()
                c.execute(_q("""
                    INSERT INTO products(sku,name,brand,category,price,quantity,active,barcode,updated_at)
                    VALUES(:sku,:name,:brand,:category,:price,:qty,:active,:barcode,NOW())
                    ON CONFLICT (sku) DO UPDATE SET
                        name=EXCLUDED.name,
                        brand=EXCLUDED.brand,
                        category=EXCLUDED.category,
                        price=EXCLUDED.price,
                        quantity=EXCLUDED.quantity,
                        active=EXCLUDED.active,
                        barcode=EXCLUDED.barcode,
                        updated_at=NOW()
                """), {"sku": sku, "name": name, "brand": brand, "category": category,
                       "price": price, "qty": qty, "active": active, "barcode": barcode})
            else:
                existed = c.execute("SELECT 1 FROM products WHERE sku=?", (sku,)).fetchone()
                c.execute("""
                    INSERT INTO products(sku,name,brand,category,price,quantity,active,barcode,updated_at)
                    VALUES(?,?,?,?,?,?,?,?,datetime('now'))
                    ON CONFLICT(sku) DO UPDATE SET
                        name=excluded.name,
                        brand=excluded.brand,
                        category=excluded.category,
                        price=excluded.price,
                        quantity=excluded.quantity,
                        active=excluded.active,
                        barcode=excluded.barcode,
                        updated_at=excluded.updated_at
                """, (sku, name, brand, category, price, qty, active, barcode))

            if existed: updated += 1
            else: inserted += 1
    return inserted, updated

# ──────────────────────────────────────────────────────────────────────────────
# Kaspi client helpers (optional)
# ──────────────────────────────────────────────────────────────────────────────
try:
    from app.kaspi_client import KaspiClient  # type: ignore
except Exception:
    try:
        from ..kaspi_client import KaspiClient  # type: ignore
    except Exception:
        from kaspi_client import KaspiClient  # type: ignore

def _pick(attrs: Dict[str, Any], *keys: str) -> str:
    for k in keys:
        v = attrs.get(k)
        if v not in (None, ""):
            return str(v)
    return ""

def _normalize_active(val: Any) -> Optional[bool]:
    if val is None or val == "":
        return None
    if isinstance(val, bool):
        return val
    s = str(val).strip().lower()
    if s in ("1", "true", "yes", "on", "published", "active"):
        return True
    if s in ("0", "false", "no", "off", "unpublished", "inactive"):
        return False
    return None

def _num(x: Any) -> float:
    try:
        return float(x)
    except Exception:
        try:
            return float(str(x).replace(" ", "").replace(",", "."))
        except Exception:
            return 0.0

def _find_iter_fn(client: Any) -> Optional[Callable]:
    for name in ("iter_products", "iter_offers", "iter_catalog"):
        if hasattr(client, name):
            return getattr(client, name)
    return None

def _collect_products(client: KaspiClient, active_only: Optional[bool]) -> Tuple[List[Dict[str, Any]], int, Optional[str]]:
    iter_fn = _find_iter_fn(client)
    if iter_fn is None:
        raise HTTPException(
            status_code=501,
            detail="В kaspi_client нет метода каталога. Ожидается iter_products/iter_offers/iter_catalog."
        )
    items: List[Dict[str, Any]] = []
    seen = set()
    total = 0
    note: Optional[str] = None

    def add_row(item: Dict[str, Any]):
        nonlocal total
        attrs = item.get("attributes", {}) or {}
        pid = item.get("id") or _pick(attrs, "id", "sku", "code", "offerId") or _pick(attrs, "name")
        if not pid or pid in seen:
            return
        seen.add(pid)
        total += 1
        code = _pick(attrs, "code", "sku", "offerId", "article", "barcode")
        name = _pick(attrs, "name", "title", "productName", "offerName")
        price = _num(_pick(attrs, "price", "basePrice", "salePrice", "currentPrice", "totalPrice"))
        qty = int(_num(_pick(attrs, "quantity", "availableAmount", "stockQuantity", "qty")))
        brand = _pick(attrs, "brand", "producer", "manufacturer")
        category = _pick(attrs, "category", "categoryName", "group")
        barcode = _pick(attrs, "barcode", "ean")
        active_val = _normalize_active(_pick(attrs, "active", "isActive", "isPublished", "visible", "isVisible", "status"))
        if active_only is not None and active_val is not None and active_only and active_val is False:
            return
        iid = code or pid
        items.append({
            "id": iid,
            "code": iid,
            "name": name,
            "price": price,
            "qty": qty,
            "active": True if qty > 0 else False if active_val is False else None,
            "brand": brand,
            "category": category,
            "barcode": barcode,
        })

    try:
        try:
            for it in iter_fn(active_only=bool(active_only) if active_only is not None else True):
                add_row(it)
        except TypeError:
            for it in iter_fn():
                add_row(it)
    except Exception:
        items = []
        total = 0

    if not items:
        try:
            for it in client.iter_products_from_orders(days=60):
                add_row(it)
            note = "Каталог по API недоступен, показаны товары, собранные из последних заказов (60 дней)."
        except Exception:
            note = "Каталог по API недоступен."
            items, total = [], 0
    return items, total, note

# ──────────────────────────────────────────────────────────────────────────────
# Pydantic
# ──────────────────────────────────────────────────────────────────────────────
class BatchIn(BaseModel):
    date: str
    qty: int
    unit_cost: float
    note: str | None = None
    commission_pct: float | None = None
    batch_code: str | None = None

class BatchListIn(BaseModel):
    entries: List[BatchIn]

class CategoryIn(BaseModel):
    name: str
    base_percent: float = 0.0
    extra_percent: float = 3.0
    tax_percent: float = 0.0

class WriteoffIn(BaseModel):
    sku: str
    qty: int
    note: str | None = None

# Новые модели: списание по позиции заказа
class OrderWriteoffIn(BaseModel):
    order_id: str
    line_index: int
    sku: str
    qty: int
    order_code: str | None = None
    unit_price: float | None = None
    total_price: float | None = None
    ts_ms: int | None = None
    note: str | None = None

class OrderWriteoffBulk(BaseModel):
    rows: List[OrderWriteoffIn]

# ──────────────────────────────────────────────────────────────────────────────
# FIFO recount helper — обновляет batches.qty_sold из леджера
# ──────────────────────────────────────────────────────────────────────────────
def _find_ledger_table(c) -> tuple[Optional[str], Optional[str], Optional[str]]:
    preferred = ["fifo_ledger", "profit_fifo_ledger", "ledger_fifo"]
    candidates = list(preferred)
    # дополнительно — любые таблицы, где в названии есть ledger
    if _USE_PG:
        rows = c.execute(_q("""
          SELECT table_name FROM information_schema.tables
           WHERE table_schema='public' AND table_name ILIKE '%ledger%'
        """)).all()
        extra = [r._mapping["table_name"] for r in rows]
    else:
        rows = c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%ledger%'").fetchall()
        extra = [r["name"] for r in rows]
    for t in extra:
        if t not in candidates:
            candidates.append(t)

    for t in candidates:
        cols = _lower_cols(c, t)
        if not cols:
            continue
        batch_key = next((x for x in ("batch_id", "bid", "batch", "batch_code") if x in cols), None)
        qty_col   = next((x for x in ("qty", "quantity", "qty_used", "used_qty") if x in cols), None)
        if batch_key and qty_col:
            return t, batch_key, qty_col
    return None, None, None

def _recount_qty_sold_from_ledger() -> int:
    _ensure_schema()
    updated = 0
    with _db() as c:
        table, batch_key, qty_col = _find_ledger_table(c)
        if not table:
            return 0

        if _USE_PG:
            rows = c.execute(_q(
                f"SELECT {batch_key} AS bkey, SUM({qty_col}) AS used FROM {table} GROUP BY {batch_key}"
            )).all()
            used_map = {str(r._mapping["bkey"]): int(r._mapping["used"] or 0) for r in rows}
            c.execute(_q("UPDATE batches SET qty_sold = COALESCE(qty_sold,0)"))
            if batch_key == "batch_code":
                for bcode, val in used_map.items():
                    r = c.execute(_q("""
                      UPDATE batches SET qty_sold=:v
                       WHERE batch_code=:bc AND COALESCE(qty_sold,0) <> :v
                    """), {"v": int(val), "bc": bcode})
                    updated += (r.rowcount or 0)
            else:
                for bid, val in used_map.items():
                    r = c.execute(_q("""
                      UPDATE batches SET qty_sold=:v
                       WHERE id=CAST(:bid AS INT) AND COALESCE(qty_sold,0) <> :v
                    """), {"v": int(val), "bid": bid})
                    updated += (r.rowcount or 0)
            c.execute(_q("UPDATE batches SET qty_sold = qty WHERE qty_sold > qty"))
        else:
            rows = c.execute(
                f"SELECT {batch_key} AS bkey, SUM({qty_col}) AS used FROM {table} GROUP BY {batch_key}"
            ).fetchall()
            used_map = {str(r["bkey"]): int(r["used"] or 0) for r in rows}
            c.execute("UPDATE batches SET qty_sold = COALESCE(qty_sold,0)")
            if batch_key == "batch_code":
                for bcode, val in used_map.items():
                    cur = c.execute(
                        "UPDATE batches SET qty_sold=? WHERE batch_code=? AND COALESCE(qty_sold,0) <> ?",
                        (int(val), bcode, int(val))
                    )
                    updated += cur.rowcount or 0
            else:
                for bid, val in used_map.items():
                    cur = c.execute(
                        "UPDATE batches SET qty_sold=? WHERE id=? AND COALESCE(qty_sold,0) <> ?",
                        (int(val), int(bid) if str(bid).isdigit() else -1, int(val))
                    )
                    updated += cur.rowcount or 0
            c.execute("UPDATE batches SET qty_sold = qty WHERE qty_sold > qty")
    return updated

# ──────────────────────────────────────────────────────────────────────────────
# FIFO списание (ручное/по заказу)
# ──────────────────────────────────────────────────────────────────────────────
def _fifo_writeoff(
    sku: str,
    qty: int,
    note: Optional[str],
    order_id: Optional[str] = None,
    order_code: Optional[str] = None,
    line_index: Optional[int] = None
) -> Dict[str, Any]:
    if qty <= 0:
        raise HTTPException(status_code=400, detail="qty должен быть > 0")

    _ensure_schema()
    total_written = 0
    batches_touched = 0
    with _db() as c:
        available = _sum_leftovers(c, sku)
        if available <= 0:
            raise HTTPException(status_code=409, detail=f"Нет остатков по SKU {sku}")
        if qty > available:
            raise HTTPException(status_code=409, detail=f"Недостаточно остатков: есть {available}, нужно {qty}")

        rows = _leftovers_by_sku(c, sku)
        need = qty
        for r in rows:
            if need <= 0:
                break
            take = min(int(r["left"]), need)
            if take <= 0:
                continue
            if _USE_PG:
                c.execute(_q("UPDATE batches SET qty_sold = COALESCE(qty_sold,0) + :t WHERE id=:bid"),
                          {"t": int(take), "bid": int(r["id"])})
                c.execute(_q("""
                    INSERT INTO writeoffs(sku,qty,note,batch_id,order_id,order_code,line_index)
                    VALUES(:sku,:q,:note,:bid,:oid,:ocode,:lidx)
                """), {"sku": sku, "q": int(take), "note": note,
                       "bid": int(r["id"]), "oid": order_id,
                       "ocode": order_code, "lidx": line_index})
            else:
                c.execute("UPDATE batches SET qty_sold = COALESCE(qty_sold,0) + ? WHERE id=?", (int(take), int(r["id"])))
                c.execute("""
                  INSERT INTO writeoffs(sku,qty,note,batch_id,order_id,order_code,line_index)
                  VALUES(?,?,?,?,?,?,?)
                """, (sku, int(take), note, int(r["id"]), order_id, order_code, line_index))
            total_written += int(take)
            batches_touched += 1
            need -= int(take)

        if _USE_PG:
            c.execute(_q("UPDATE batches SET qty_sold = qty WHERE qty_sold > qty"))
        else:
            c.execute("UPDATE batches SET qty_sold = qty WHERE qty_sold > qty")

        left_total = _recompute_product_qty(c, sku)

    return {
        "written_off": int(total_written),
        "batches_touched": int(batches_touched),
        "left_total": int(left_total),
    }

# ──────────────────────────────────────────────────────────────────────────────
# Router
# ──────────────────────────────────────────────────────────────────────────────
def get_products_router(client: Optional["KaspiClient"]) -> APIRouter:
    router = APIRouter(tags=["products"])

    # Ping
    @router.get("/db/ping")
    async def db_ping():
        _ensure_schema()
        if _USE_PG:
            with _db() as c:
                c.execute(_q("SELECT 1"))
            return {"ok": True, "driver": "pg"}
        else:
            with _db() as c:
                ok = c.execute("PRAGMA integrity_check").fetchone()[0]
            return {"ok": ok == "ok", "driver": "sqlite"}

    # Каталог (Kaspi)
    @router.get("/list")
    async def list_products(
        active: int = Query(1, description="1 — только активные, 0 — все"),
        q: Optional[str] = Query(None, description="поиск по названию/коду"),
        page: int = Query(1, ge=1),
        page_size: int = Query(500, ge=1, le=2000),
    ):
        if client is None:
            raise HTTPException(status_code=500, detail="KASPI_TOKEN is not set")
        items, total, note = _collect_products(client, active_only=bool(active))
        if q:
            ql = q.strip().lower()
            items = [r for r in items if ql in (r["name"] or "").lower() or ql in (r["code"] or "").lower()]
        start = (page - 1) * page_size
        end = start + page_size
        return {"items": items[start:end], "total": len(items), "note": note}

    @router.get("/export.csv")
    async def export_products_csv(active: int = Query(1)):
        if client is None:
            raise HTTPException(status_code=500, detail="KASPI_TOKEN is not set")
        items, _, _ = _collect_products(client, active_only=bool(active))

        def esc(s: str) -> str:
            s = "" if s is None else str(s)
            if any(c in s for c in [",", '"', "\n"]):
                s = '"' + s.replace('"', '""') + '"'
            return s

        header = "id,code,name,price,qty,active,brand,category,barcode\n"
        body = "".join(
            [
                ",".join(
                    esc(x)
                    for x in [
                        r["id"], r["code"], r["name"], r["price"], r["qty"],
                        1 if r["active"] else 0 if r["active"] is False else "",
                        r["brand"], r["category"], r["barcode"],
                    ]
                )
                + "\n"
                for r in items
            ]
        )
        return Response(
            content=header + body,
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": 'attachment; filename="products.csv"'},
        )

    # Ручная загрузка (XML/Excel) → в БД
    def _parse_xml(content: bytes) -> List[Dict[str, Any]]:
        try:
            root = ET.fromstring(content)
        except ET.ParseError as e:
            raise HTTPException(status_code=400, detail=f"Некорректный XML: {e}")

        def strip(tag: str) -> str:
            return tag.split("}", 1)[-1] if "}" in tag else tag

        def child_text(parent: ET.Element, *names: str) -> str:
            for el in parent.iter():
                if strip(el.tag) in names:
                    t = (el.text or "").strip()
                    if t:
                        return t
            return ""

        rows: List[Dict[str, Any]] = []
        offers = [el for el in root.iter() if strip(el.tag) == "offer"]
        for off in offers:
            code = (off.get("sku") or off.get("shop-sku") or off.get("code") or off.get("id") or "").strip()
            name = child_text(off, "model", "name", "title")
            brand = child_text(off, "brand")
            qty = 0
            active: Optional[bool] = None
            for el in off.iter():
                if strip(el.tag) == "availability":
                    sc = el.get("stockCount")
                    if sc:
                        try: qty = int(float(sc))
                        except Exception: qty = 0
                    av = (el.get("available") or "").strip().lower()
                    if av in ("yes", "true", "1"): active = True
                    elif av in ("no", "false", "0"): active = False
                    break
            price = 0.0
            for el in off.iter():
                if strip(el.tag) == "cityprice":
                    txt = (el.text or "").strip()
                    if txt:
                        try: price = float(txt.replace(" ", "").replace(",", "."))
                        except Exception: price = 0.0
                    break
            rows.append({
                "id": code, "code": code, "name": name or code,
                "brand": brand or None, "qty": qty, "price": price, "active": active,
            })
        return rows

    def _parse_excel(file: UploadFile) -> List[Dict[str, Any]]:
        if not _OPENPYXL_AVAILABLE:
            raise HTTPException(status_code=500, detail="openpyxl не установлен на сервере.")
        try:
            data = file.file.read()
            wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Не удалось открыть Excel: {e}")
        ws = wb.active
        headers = [str(c.value or '').strip() for c in ws[1]]
        rows: List[Dict[str, Any]] = []
        for row in ws.iter_rows(min_row=2, values_only=True):
            item = {h: v for h, v in zip(headers, row)}
            if any(v not in (None, "", []) for v in item.values()):
                rows.append(item)
        return rows

    @router.post("/manual-upload")
    async def manual_upload(file: UploadFile = File(...), _: bool = Depends(require_api_key)):
        filename = (file.filename or "").lower()
        content = await file.read()
        if filename.endswith(".xml"):
            raw_rows = _parse_xml(content)
        elif filename.endswith(".xlsx") or filename.endswith(".xls"):
            file.file = io.BytesIO(content)
            raw_rows = _parse_excel(file)
        else:
            raise HTTPException(status_code=400, detail="Поддерживаются только XML или Excel (.xlsx/.xls).")
        normalized: List[Dict[str, Any]] = []
        for r in raw_rows:
            if normalize_row:
                try:
                    d = normalize_row(r).to_dict()
                except Exception:
                    d = dict(r)
            else:
                d = dict(r)
            d.setdefault("code", (d.get("sku") or d.get("vendorCode") or d.get("barcode") or d.get("id") or ""))
            d["code"] = str(d["code"]).strip()
            d.setdefault("id", d["code"])
            d.setdefault("name", d.get("model") or d.get("title") or d.get("Name") or d.get("name") or d["code"])
            d["name"] = str(d["name"]).strip()
            d["qty"] = _to_int(d.get("qty") or d.get("quantity") or d.get("stock"))
            d["price"] = _to_float(d.get("price"))
            d["brand"] = d.get("brand") or d.get("vendor")
            d["barcode"] = d.get("barcode") or d.get("Barcode")
            normalized.append(d)
        inserted, updated = _upsert_products(normalized)
        normalized.sort(key=lambda x: (x.get("name") or x.get("Name") or x.get("model") or x.get("title") or '').lower())
        return {"count": len(normalized), "items": normalized, "inserted": inserted, "updated": updated}

    # Сохранить таблицу (как на кнопке «Сохранить таблицу в БД»)
    @router.post("/db/bulk-upsert")
    async def bulk_upsert(rows: List[Dict[str, Any]] = Body(...), _: bool = Depends(require_api_key)):
        inserted, updated = _upsert_products(rows or [])
        return {"inserted": inserted, "updated": updated}

    # ──────────────────────────────────────────────────────────────────────────
    # DB: список товаров (+ мета по партиям)
    # ──────────────────────────────────────────────────────────────────────────
    @router.get("/db/list")
    async def db_list(active_only: int = Query(1), search: str = Query("", alias="q")):
        _ensure_schema()
        _seed_categories_if_empty()
        with _db() as c:
            # категории
            if _USE_PG:
                cats_rows = c.execute(_q("SELECT name, base_percent, extra_percent, tax_percent FROM categories ORDER BY name")).all()
                cats = {r._mapping["name"]: dict(r._mapping) for r in cats_rows}
            else:
                cats = {r["name"]: dict(r) for r in c.execute("SELECT * FROM categories")}
            # продукты
            if _USE_PG:
                sql = "SELECT sku,name,brand,category,price,quantity,active FROM products"
                conds, params = [], {}
                if active_only: conds.append("active=1")
                if search:
                    conds.append("(sku ILIKE :q OR name ILIKE :q)")
                    params["q"] = f"%{search}%"
                if conds: sql += " WHERE " + " AND ".join(conds)
                sql += " ORDER BY name"
                rows = c.execute(_q(sql), params).all()
            else:
                sql = "SELECT sku,name,brand,category,price,quantity,active FROM products"
                conds, params = [], []
                if active_only: conds.append("active=1")
                if search:
                    conds.append("(sku LIKE ? OR name LIKE ?)")
                    params += [f"%{search}%", f"%{search}%"]
                if conds: sql += " WHERE " + " AND ".join(conds)
                sql += " ORDER BY name COLLATE NOCASE"
                rows = c.execute(sql, params).fetchall()
            rows = _rows_to_dicts(rows)

            # мета по партиям
            if _USE_PG:
                bc_rows = c.execute(_q("SELECT sku, COUNT(*) AS cnt FROM batches GROUP BY sku")).all()
                bc = {r._mapping["sku"]: r._mapping["cnt"] for r in bc_rows}
                last_rows = c.execute(_q(
                    "SELECT sku, date, unit_cost, commission_pct FROM batches ORDER BY date"
                )).all()
                last = {}
                for r in last_rows:
                    m = r._mapping
                    last[m["sku"]] = (m["unit_cost"], m["commission_pct"])
                left_rows = c.execute(_q(
                    "SELECT sku, SUM(qty - COALESCE(qty_sold,0)) AS left FROM batches GROUP BY sku"
                )).all()
                left_by_sku = {r._mapping["sku"]: int(r._mapping["left"] or 0) for r in left_rows}
            else:
                bc = {r["sku"]: r["cnt"] for r in c.execute("SELECT sku, COUNT(*) AS cnt FROM batches GROUP BY sku")}
                last = {}
                for r in c.execute("SELECT sku, date, unit_cost, commission_pct FROM batches ORDER BY date"):
                    last[r["sku"]] = (r["unit_cost"], r["commission_pct"])
                left_by_sku = {r["sku"]: int(r["left"] or 0) for r in c.execute(
                    "SELECT sku, SUM(qty - COALESCE(qty_sold,0)) AS left FROM batches GROUP BY sku"
                ).fetchall()}

        items: List[Dict[str, Any]] = []
        for r in rows:
            sku = r["sku"]; price = float(r.get("price") or 0); qty = int(r.get("quantity") or 0)
            cat = r.get("category") or ""
            commissions = cats.get(cat)
            last_margin = None
            if sku in last:
                ucost, comm = last[sku]
                eff_comm = float(comm) if comm is not None else (
                    (float(commissions["base_percent"]) + float(commissions["extra_percent"]) + float(commissions["tax_percent"])) if commissions else 0.0
                )
                last_margin = price - (price * eff_comm/100.0) - float(ucost)
            items.append({
                "code": sku, "id": sku, "name": r.get("name"), "brand": r.get("brand"), "category": cat,
                "qty": qty, "price": price, "active": bool(r.get("active")),
                "batch_count": int(bc.get(sku, 0)),
                "last_margin": round(last_margin, 2) if last_margin is not None else None,
                "left_total": int(left_by_sku.get(sku, 0)),
            })
        return {"count": len(items), "items": items}

    # ──────────────────────────────────────────────────────────────────────────
    # DB: партии (просмотр/добавление/редактирование/удаление)
    # ──────────────────────────────────────────────────────────────────────────
    @router.get("/db/price-batches/{sku}")
    async def get_batches(sku: str):
        _ensure_schema()
        with _db() as c:
            if _USE_PG:
                rows = c.execute(_q(
                    "SELECT id, date, qty, qty_sold, (qty - COALESCE(qty_sold,0)) AS left, "
                    "unit_cost, commission_pct, batch_code, note "
                    "FROM batches WHERE sku=:sku ORDER BY date, id"
                ), {"sku": sku}).all()
                rows = _rows_to_dicts(rows)
            else:
                rows = [dict(r) for r in c.execute(
                    "SELECT id, date, qty, qty_sold, (qty - COALESCE(qty_sold,0)) AS left, "
                    "unit_cost, commission_pct, batch_code, note "
                    "FROM batches WHERE sku=? ORDER BY date, id", (sku,)
                )]
        avgc = _avg_cost(sku)
        return {"batches": rows, "avg_cost": round(avgc, 2) if avgc is not None else None}

    @router.post("/db/price-batches/{sku}")
    async def add_batches(sku: str, payload: BatchListIn = Body(...), _: bool = Depends(require_api_key)):
        _ensure_schema()
        with _db() as c:
            for e in payload.entries:
                if _USE_PG:
                    code = e.batch_code
                    if not code:
                        code = c.execute(_q(
                            "SELECT CONCAT("
                            "SUBSTRING('ABCDEFGHJKLMNPQRSTUVWXYZ23456789' FROM floor(random()*32)::int + 1 FOR 1),"
                            "SUBSTRING('ABCDEFGHJKLMNPQRSTUVWXYZ23456789' FROM floor(random()*32)::int + 1 FOR 1),"
                            "SUBSTRING('ABCDEFGHJKLMNPQRSTUVWXYZ23456789' FROM floor(random()*32)::int + 1 FOR 1),"
                            "SUBSTRING('ABCDEFGHJKLMNPQRSTUVWXYZ23456789' FROM floor(random()*32)::int + 1 FOR 1),"
                            "SUBSTRING('ABCDEFGHJKLMNPQRSTUVWXYZ23456789' FROM floor(random()*32)::int + 1 FOR 1),"
                            "SUBSTRING('ABCDEFGHJKLMNPQRSTUVWXYZ23456789' FROM floor(random()*32)::int + 1 FOR 1))"
                        )).scalar_one()
                    c.execute(_q(
                        "INSERT INTO batches(sku,date,qty,unit_cost,note,commission_pct,batch_code,qty_sold) "
                        "VALUES(:sku,:date,:qty,:ucost,:note,:comm,:code,0)"),
                        {"sku": sku, "date": e.date, "qty": int(e.qty),
                         "ucost": float(e.unit_cost), "note": e.note,
                         "comm": float(e.commission_pct) if e.commission_pct is not None else None,
                         "code": code}
                    )
                else:
                    code = e.batch_code or _gen_batch_code_sqlite(c)
                    c.execute(
                        "INSERT INTO batches(sku,date,qty,unit_cost,note,commission_pct,batch_code,qty_sold) "
                        "VALUES(?,?,?,?,?,?,?,0)",
                        (sku, e.date, int(e.qty), float(e.unit_cost), e.note,
                         float(e.commission_pct) if e.commission_pct is not None else None, code)
                    )
            _recompute_product_qty(c, sku)
        avgc = _avg_cost(sku)
        return {"status": "ok", "avg_cost": round(avgc, 2) if avgc is not None else None}

    @router.put("/db/price-batches/{sku}/{bid}")
    async def update_batch(sku: str, bid: int, payload: Dict[str, Any] = Body(...), _: bool = Depends(require_api_key)):
        _ensure_schema()
        fields = {
            "date": payload.get("date"),
            "qty": payload.get("qty"),
            "unit_cost": payload.get("unit_cost"),
            "note": payload.get("note"),
            "commission_pct": payload.get("commission_pct"),
            "batch_code": payload.get("batch_code"),
        }
        sets = {k: v for k, v in fields.items() if v is not None}
        if not sets:
            return {"status": "noop"}
        with _db() as c:
            if _USE_PG:
                parts = [f"{k}=:{k}" for k in sets.keys()]
                sets["bid"] = bid; sets["sku"] = sku
                c.execute(_q(f"UPDATE batches SET {', '.join(parts)} WHERE id=:bid AND sku=:sku"), sets)
                c.execute(_q("UPDATE batches SET qty_sold = LEAST(qty_sold, qty) WHERE id=:bid AND sku=:sku"),
                          {"bid": bid, "sku": sku})
            else:
                parts = [f"{k}=?" for k in sets.keys()]
                params = list(sets.values()) + [bid, sku]
                c.execute(f"UPDATE batches SET {', '.join(parts)} WHERE id=? AND sku=?", params)
                c.execute("UPDATE batches SET qty_sold = MIN(qty_sold, qty) WHERE id=? AND sku=?", (bid, sku))
            _recompute_product_qty(c, sku)
        return {"status": "ok"}

    @router.delete("/db/price-batches/{sku}/{bid}")
    async def delete_batch(sku: str, bid: int, _: bool = Depends(require_api_key)):
        _ensure_schema()
        with _db() as c:
            if _USE_PG:
                c.execute(_q("DELETE FROM batches WHERE id=:bid AND sku=:sku"), {"bid": bid, "sku": sku})
            else:
                c.execute("DELETE FROM batches WHERE id=? AND sku=?", (bid, sku))
            _recompute_product_qty(c, sku)
        return {"status": "ok"}

    # ──────────────────────────────────────────────────────────────────────────
    # DB: категории/комиссии
    # ──────────────────────────────────────────────────────────────────────────
    @router.get("/db/categories")
    async def list_categories():
        _seed_categories_if_empty()
        with _db() as c:
            if _USE_PG:
                rows = c.execute(_q(
                    "SELECT name, base_percent, extra_percent, tax_percent FROM categories ORDER BY name"
                )).all()
                rows = _rows_to_dicts(rows)
            else:
                rows = [dict(r) for r in c.execute("SELECT * FROM categories ORDER BY name")]
        return {"categories": rows}

    @router.post("/db/categories")
    async def save_categories(cats: List[CategoryIn], _: bool = Depends(require_api_key)):
        _ensure_schema()
        with _db() as c:
            for cat in cats:
                if _USE_PG:
                    c.execute(_q("""
                        INSERT INTO categories(name,base_percent,extra_percent,tax_percent)
                        VALUES(:n,:b,:e,:t)
                        ON CONFLICT (name) DO UPDATE
                        SET base_percent=EXCLUDED.base_percent,
                            extra_percent=EXCLUDED.extra_percent,
                            tax_percent=EXCLUDED.tax_percent
                    """), {"n": cat.name, "b": float(cat.base_percent), "e": float(cat.extra_percent), "t": float(cat.tax_percent)})
                else:
                    c.execute("""
                        INSERT INTO categories(name,base_percent,extra_percent,tax_percent)
                        VALUES(?,?,?,?)
                        ON CONFLICT(name) DO UPDATE SET
                          base_percent=excluded.base_percent,
                          extra_percent=excluded.extra_percent,
                          tax_percent=excluded.tax_percent
                    """, (cat.name, float(cat.base_percent), float(cat.extra_percent), float(cat.tax_percent)))
        return {"status": "ok"}

    @router.put("/db/product-category/{sku}")
    async def set_product_category(sku: str, payload: Dict[str, Any] = Body(...), _: bool = Depends(require_api_key)):
        _ensure_schema()
        category = (payload.get("category") or "").strip()
        with _db() as c:
            if _USE_PG:
                c.execute(_q("UPDATE products SET category=:cat, updated_at=NOW() WHERE sku=:sku"),
                          {"cat": category, "sku": sku})
            else:
                c.execute("UPDATE products SET category=?, updated_at=datetime('now') WHERE sku=?", (category, sku))
        return {"status": "ok", "sku": sku, "category": category}

    # ──────────────────────────────────────────────────────────────────────────
    # Бэкап/восстановление (SQLite only)
    # ──────────────────────────────────────────────────────────────────────────
    @router.get("/db/backup.sqlite3")
    async def backup_db():
        _ensure_schema()
        if _USE_PG:
            raise HTTPException(status_code=501, detail="Backup доступен только для локальной SQLite.")
        fname = os.path.basename(DB_PATH) or "data.sqlite3"
        return FileResponse(DB_PATH, media_type="application/octet-stream", filename=fname)

    @router.post("/db/restore")
    async def restore_db(file: UploadFile = File(...)):
        if _USE_PG:
            raise HTTPException(status_code=501, detail="Restore доступен только для локальной SQLite.")
        content = await file.read()
        try:
            with _db() as c:
                c.execute("PRAGMA wal_checkpoint(TRUNCATE);")
        except Exception:
            pass
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        with open(DB_PATH, "wb") as f:
            f.write(content)
        with _db() as c:
            ok = c.execute("PRAGMA integrity_check").fetchone()[0]
        return {"status": "ok", "integrity": ok}

    # ──────────────────────────────────────────────────────────────────────────
    # Остатки и пересчёт qty_sold
    # ──────────────────────────────────────────────────────────────────────────
    @router.get("/batches/with-leftovers")
    async def batches_with_leftovers(q: Optional[str] = Query(None, description="поиск по SKU")):
        _ensure_schema()
        with _db() as c:
            if _USE_PG:
                base = "SELECT id, sku, date, qty, qty_sold, (qty-COALESCE(qty_sold,0)) AS left, unit_cost, commission_pct, batch_code, note FROM batches"
                params: Dict[str, Any] = {}
                if q:
                    base += " WHERE sku ILIKE :q"
                    params["q"] = f"%{q}%"
                base += " ORDER BY sku, date, id"
                rows = c.execute(_q(base), params).all()
                rows = _rows_to_dicts(rows)
            else:
                base = "SELECT id, sku, date, qty, qty_sold, (qty-COALESCE(qty_sold,0)) AS left, unit_cost, commission_pct, batch_code, note FROM batches"
                params: List[Any] = []
                if q:
                    base += " WHERE sku LIKE ?"; params.append(f"%{q}%")
                base += " ORDER BY sku, date, id"
                rows = [dict(r) for r in c.execute(base, params).fetchall()]
        for r in rows:
            r["qty"] = int(r.get("qty") or 0)
            r["qty_sold"] = int(r.get("qty_sold") or 0)
            r["left"] = int(r.get("left") or 0)
            r["unit_cost"] = float(r.get("unit_cost") or 0)
            r["commission_pct"] = float(r.get("commission_pct") or 0)
        return {"rows": rows}

    @router.post("/batches/recount-sold")
    async def batches_recount_sold(_: bool = Depends(require_api_key)):
        changed = _recount_qty_sold_from_ledger()
        _recompute_all_products_qty()
        return {"ok": True, "updated_batches": int(changed)}

    # ──────────────────────────────────────────────────────────────────────────
    # ПРОСТОЕ СПИСАНИЕ СО СКЛАДА (FIFO) + МЕТРИКИ ДЛЯ «ШАПКИ»
    # ──────────────────────────────────────────────────────────────────────────
    @router.post("/db/writeoff")
    async def writeoff(payload: WriteoffIn = Body(...), _: bool = Depends(require_api_key)):
        res = _fifo_writeoff(payload.sku.strip(), int(payload.qty), payload.note)
        return {"ok": True, **res}

    # УПРОЩЁННЫЙ «бейдж списаний» (календарные сутки сегодня или за всё время)
    @router.get("/db/writeoffs/header")
    async def writeoffs_header(
        all_time: int = Query(0, description="1 — без фильтра по дате (сумма за всё время)"),
    ):
        """
        Метрика для шапки: количество строк и суммарный qty списаний.
        Источник — таблица леджера, из которой считается FIFO.
        По умолчанию считаем за календарный день «сегодня» (локальное время сервера).
        """
        _ensure_schema()
        with _db() as c:
            table, _bk, qty_col = _find_ledger_table(c)
            if not table:
                return {"count": 0, "total_qty": 0}

            # Без фильтра по дате — сумма по всему леджеру
            if all_time:
                if _USE_PG:
                    row = c.execute(_q(f"""
                        SELECT COUNT(*) AS c, COALESCE(SUM({qty_col}),0) AS t
                        FROM {table}
                    """)).first()
                    return {"count": int(row._mapping["c"] or 0),
                            "total_qty": int(row._mapping["t"] or 0)}
                else:
                    r = c.execute(f"""
                        SELECT COUNT(*) AS c, IFNULL(SUM({qty_col}),0) AS t
                        FROM {table}
                    """).fetchone()
                    return {"count": int(r["c"] or 0), "total_qty": int(r["t"] or 0)}

            # Календарные сутки «сегодня»
            today = datetime.now().strftime("%Y-%m-%d")

            # Ищем колонку даты/времени (включая *_ms)
            cols = _lower_cols(c, table)
            candidate_dates = ["date","order_date","created_at","ts","timestamp","ts_ms","date_ms"]
            date_col = next((x for x in candidate_dates if x in cols), None)

            # Если явной датовой колонки нет — считаем по всему леджеру
            if not date_col:
                if _USE_PG:
                    row = c.execute(_q(f"""
                        SELECT COUNT(*) AS c, COALESCE(SUM({qty_col}),0) AS t
                        FROM {table}
                    """)).first()
                    return {"count": int(row._mapping["c"] or 0),
                            "total_qty": int(row._mapping["t"] or 0)}
                else:
                    r = c.execute(f"""
                        SELECT COUNT(*) AS c, IFNULL(SUM({qty_col}),0) AS t
                        FROM {table}
                    """).fetchone()
                    return {"count": int(r["c"] or 0), "total_qty": int(r["t"] or 0)}

            # Подбираем выражение даты
            if _USE_PG:
                date_expr = f"to_timestamp({date_col}/1000)::date" if date_col.endswith("_ms") else f"{date_col}::date"
                row = c.execute(_q(f"""
                    SELECT COUNT(*) AS c, COALESCE(SUM({qty_col}),0) AS t
                    FROM {table}
                    WHERE {date_expr} = :d
                """), {"d": today}).first()
                return {"count": int(row._mapping["c"] or 0),
                        "total_qty": int(row._mapping["t"] or 0)}
            else:
                date_expr = (
                    f"date(datetime({date_col}/1000,'unixepoch','localtime'))"
                    if date_col.endswith("_ms")
                    else f"substr({date_col},1,10)"
                )
                r = c.execute(f"""
                    SELECT COUNT(*) AS c, IFNULL(SUM({qty_col}),0) AS t
                    FROM {table}
                    WHERE {date_expr} = ?
                """, (today,)).fetchone()
                return {"count": int(r["c"] or 0), "total_qty": int(r["t"] or 0)}

    # Сводка по таблице writeoffs (ручные списания)
    @router.get("/db/writeoffs/summary")
    async def writeoffs_summary(
        start: Optional[str] = Query(None, description="UTC ISO: 2025-08-29T00:00:00"),
        end: Optional[str] = Query(None, description="UTC ISO: 2025-08-30T00:00:00"),
        sku: Optional[str] = Query(None)
    ):
        _ensure_schema()
        if not start or not end:
            end_dt = datetime.utcnow()
            start_dt = end_dt - timedelta(days=30)
        else:
            try:
                start_dt = datetime.fromisoformat(start.replace("Z",""))
                end_dt = datetime.fromisoformat(end.replace("Z",""))
            except Exception:
                raise HTTPException(status_code=400, detail="start/end должны быть ISO datetime")
        with _db() as c:
            if _USE_PG:
                base = "SELECT COUNT(*) AS cnt, COALESCE(SUM(qty),0) AS total FROM writeoffs WHERE ts >= :s AND ts < :e"
                params: Dict[str, Any] = {"s": start_dt, "e": end_dt}
                if sku:
                    base += " AND sku = :sku"
                    params["sku"] = sku
                row = c.execute(_q(base), params).first()
                cnt = int(row.cnt or 0)
                total = int(row.total or 0)
            else:
                base = "SELECT COUNT(*) AS cnt, COALESCE(SUM(qty),0) AS total FROM writeoffs WHERE ts >= ? AND ts < ?"
                params: List[Any] = [start_dt.strftime("%Y-%m-%d %H:%M:%S"), end_dt.strftime("%Y-%m-%d %H:%M:%S")]
                if sku:
                    base += " AND sku = ?"; params.append(sku)
                r = c.execute(base, params).fetchone()
                cnt = int(r["cnt"] or 0)
                total = int(r["total"] or 0)
        return {"count": cnt, "total_qty": total, "start": start_dt.isoformat(), "end": end_dt.isoformat(), "sku": sku}

    # ──────────────────────────────────────────────────────────────────────────
    # Списание по позиции заказа (идемпотентность по (order_id, line_index))
    # ──────────────────────────────────────────────────────────────────────────
    @router.post("/db/writeoff/by-order")
    async def writeoff_by_order(p: OrderWriteoffIn = Body(...), _: bool = Depends(require_api_key)):
        _ensure_schema()
        # идемпотентность
        with _db() as c:
            ex = (c.execute(_q("SELECT 1 FROM order_writeoffs WHERE order_id=:o AND line_index=:i"),
                            {"o": p.order_id, "i": int(p.line_index)}).first()
                  if _USE_PG
                  else c.execute("SELECT 1 FROM order_writeoffs WHERE order_id=? AND line_index=?",
                                 (p.order_id, int(p.line_index))).fetchone())
            if ex:
                return {"ok": True, "skipped": True, "reason": "already_written"}

        # FIFO-списание с контекстом заказа
        res = _fifo_writeoff(
            p.sku.strip(), int(p.qty), p.note,
            order_id=p.order_id.strip(),
            order_code=(p.order_code or None),
            line_index=int(p.line_index)
        )

        # записываем «шапку» позиции заказа
        ts_val_pg = None
        ts_val_sqlite = None
        if p.ts_ms:
            dt = datetime.utcfromtimestamp(p.ts_ms/1000.0)
            ts_val_pg = dt
            ts_val_sqlite = dt.strftime("%Y-%m-%d %H:%M:%S")

        with _db() as c:
            if _USE_PG:
                c.execute(_q("""
                  INSERT INTO order_writeoffs(order_id,order_code,line_index,sku,qty,unit_price,total_price,note,ts)
                  VALUES(:o,:oc,:i,:s,:q,:u,:t,:n, COALESCE(:ts, NOW()))
                """), {"o": p.order_id, "oc": p.order_code, "i": int(p.line_index),
                       "s": p.sku, "q": int(p.qty),
                       "u": float(p.unit_price) if p.unit_price is not None else None,
                       "t": float(p.total_price) if p.total_price is not None else None,
                       "n": p.note, "ts": ts_val_pg})
            else:
                c.execute("""
                  INSERT INTO order_writeoffs(order_id,order_code,line_index,sku,qty,unit_price,total_price,note,ts)
                  VALUES(?,?,?,?,?,?,?,?, COALESCE(?, CURRENT_TIMESTAMP))
                """, (p.order_id, p.order_code, int(p.line_index), p.sku, int(p.qty),
                      float(p.unit_price) if p.unit_price is not None else None,
                      float(p.total_price) if p.total_price is not None else None,
                      p.note, ts_val_sqlite))

        return {"ok": True, **res}

    @router.post("/db/writeoff/by-order/bulk")
    async def writeoff_by_order_bulk(payload: OrderWriteoffBulk = Body(...), _: bool = Depends(require_api_key)):
        _ensure_schema()
        out = []
        for p in payload.rows or []:
            try:
                r = await writeoff_by_order(p)
                out.append({"ok": True, **r, "order_id": p.order_id, "line_index": int(p.line_index),
                            "sku": p.sku, "qty": int(p.qty)})
            except HTTPException as e:
                out.append({"ok": False, "order_id": p.order_id, "line_index": int(p.line_index),
                            "sku": p.sku, "error": e.detail})
            except Exception as e:
                out.append({"ok": False, "order_id": p.order_id, "line_index": int(p.line_index),
                            "sku": p.sku, "error": str(e)})
        return {"results": out}

    # ──────────────────────────────────────────────────────────────────────────
    # Отчёты
    # ──────────────────────────────────────────────────────────────────────────
    @router.get("/db/report/periods")
    async def report_periods(
        start: Optional[str] = Query(None),
        end: Optional[str] = Query(None),
        limit: int = Query(60, ge=1, le=365)
    ):
        _ensure_schema()
        if not start or not end:
            end_dt = datetime.utcnow()
            start_dt = end_dt - timedelta(days=limit)
        else:
            end_dt = datetime.fromisoformat(end.replace("Z",""))
            start_dt = datetime.fromisoformat(start.replace("Z",""))

        if _USE_PG:
            sql = """
            SELECT
              (ow.ts::date) AS period,
              SUM(COALESCE(ow.total_price, ow.unit_price*ow.qty))                                                   AS revenue,
              SUM(COALESCE(w.qty,0) * COALESCE(ow.unit_price,0) * COALESCE(b.commission_pct,0)/100.0)              AS commission,
              SUM(COALESCE(w.qty,0) * COALESCE(b.unit_cost,0))                                                     AS cost,
              SUM(COALESCE(ow.total_price, ow.unit_price*ow.qty)
                  - COALESCE(w.qty,0)*COALESCE(ow.unit_price,0)*COALESCE(b.commission_pct,0)/100.0
                  - COALESCE(w.qty,0)*COALESCE(b.unit_cost,0))                                                     AS profit
            FROM order_writeoffs ow
            LEFT JOIN writeoffs w
              ON w.order_id = ow.order_id AND w.line_index = ow.line_index AND w.sku = ow.sku
            LEFT JOIN batches b ON b.id = w.batch_id
            WHERE ow.ts >= :s AND ow.ts < :e
            GROUP BY 1
            ORDER BY 1 DESC
            """
            with _db() as c:
                rows = [dict(r._mapping) for r in c.execute(_q(sql), {"s": start_dt, "e": end_dt}).all()]
        else:
            sql = """
            SELECT
              substr(ow.ts,1,10) AS period,
              SUM(COALESCE(ow.total_price, ow.unit_price*ow.qty))                                                   AS revenue,
              SUM(COALESCE(w.qty,0) * COALESCE(ow.unit_price,0) * COALESCE(b.commission_pct,0)/100.0)              AS commission,
              SUM(COALESCE(w.qty,0) * COALESCE(b.unit_cost,0))                                                     AS cost,
              SUM(COALESCE(ow.total_price, ow.unit_price*ow.qty)
                  - COALESCE(w.qty,0)*COALESCE(ow.unit_price,0)*COALESCE(b.commission_pct,0)/100.0
                  - COALESCE(w.qty,0)*COALESCE(b.unit_cost,0))                                                     AS profit
            FROM order_writeoffs ow
            LEFT JOIN writeoffs w
              ON w.order_id = ow.order_id AND w.line_index = ow.line_index AND w.sku = ow.sku
            LEFT JOIN batches b ON b.id = w.batch_id
            WHERE ow.ts >= ? AND ow.ts < ?
            GROUP BY 1
            ORDER BY 1 DESC
            """
            with _db() as c:
                rows = [dict(r) for r in c.execute(sql, (start_dt.strftime("%Y-%m-%d %H:%M:%S"),
                                                         end_dt.strftime("%Y-%m-%d %H:%M:%S"))).fetchall()]
        for r in rows:
            for k in ("revenue","commission","cost","profit"):
                r[k] = round(float(r.get(k) or 0), 2)
        return {"rows": rows}

    @router.get("/db/report/top-lines")
    async def report_top_lines(
        start: Optional[str] = Query(None),
        end: Optional[str] = Query(None),
        limit: int = Query(100, ge=1, le=1000)
    ):
        _ensure_schema()
        if not start or not end:
            end_dt = datetime.utcnow()
            start_dt = end_dt - timedelta(days=30)
        else:
            end_dt = datetime.fromisoformat(end.replace("Z",""))
            start_dt = datetime.fromisoformat(start.replace("Z",""))

        base_pg = """
          SELECT
            ow.order_code, ow.order_id, ow.line_index, ow.sku, ow.qty,
            COALESCE(ow.total_price, ow.unit_price*ow.qty)                                                        AS revenue,
            SUM(COALESCE(w.qty,0) * COALESCE(ow.unit_price,0) * COALESCE(b.commission_pct,0)/100.0)               AS commission,
            SUM(COALESCE(w.qty,0) * COALESCE(b.unit_cost,0))                                                     AS cost,
            COALESCE(ow.total_price, ow.unit_price*ow.qty)
              - SUM(COALESCE(w.qty,0)*COALESCE(ow.unit_price,0)*COALESCE(b.commission_pct,0)/100.0)
              - SUM(COALESCE(w.qty,0)*COALESCE(b.unit_cost,0))                                                   AS profit
          FROM order_writeoffs ow
          LEFT JOIN writeoffs w
            ON w.order_id = ow.order_id AND w.line_index = ow.line_index AND w.sku = ow.sku
          LEFT JOIN batches b ON b.id = w.batch_id
          WHERE ow.ts >= :s AND ow.ts < :e
          GROUP BY ow.order_code, ow.order_id, ow.line_index, ow.sku, ow.qty, ow.unit_price, ow.total_price
          ORDER BY profit DESC
          LIMIT :lim
        """
        base_sqlite = """
          SELECT
            ow.order_code, ow.order_id, ow.line_index, ow.sku, ow.qty,
            COALESCE(ow.total_price, ow.unit_price*ow.qty)                                                        AS revenue,
            SUM(COALESCE(w.qty,0) * COALESCE(ow.unit_price,0) * COALESCE(b.commission_pct,0)/100.0)               AS commission,
            SUM(COALESCE(w.qty,0) * COALESCE(b.unit_cost,0))                                                     AS cost,
            COALESCE(ow.total_price, ow.unit_price*ow.qty)
              - SUM(COALESCE(w.qty,0)*COALESCE(ow.unit_price,0)*COALESCE(b.commission_pct,0)/100.0)
              - SUM(COALESCE(w.qty,0)*COALESCE(b.unit_cost,0))                                                   AS profit
          FROM order_writeoffs ow
          LEFT JOIN writeoffs w
            ON w.order_id = ow.order_id AND w.line_index = ow.line_index AND w.sku = ow.sku
          LEFT JOIN batches b ON b.id = w.batch_id
          WHERE ow.ts >= ? AND ow.ts < ?
          GROUP BY ow.order_code, ow.order_id, ow.line_index, ow.sku, ow.qty, ow.unit_price, ow.total_price
          ORDER BY profit DESC
          LIMIT ?
        """

        with _db() as c:
            if _USE_PG:
                rows = [dict(r._mapping) for r in c.execute(_q(base_pg),
                        {"s": start_dt, "e": end_dt, "lim": limit}).all()]
            else:
                rows = [dict(r) for r in c.execute(base_sqlite,
                        (start_dt.strftime("%Y-%m-%d %H:%M:%S"),
                         end_dt.strftime("%Y-%m-%d %H:%M:%S"), limit)).fetchall()]

        for r in rows:
            for k in ("revenue","commission","cost","profit"):
                r[k] = round(float(r.get(k) or 0), 2)
        return {"rows": rows}

    return router
