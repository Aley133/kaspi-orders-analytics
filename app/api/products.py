# app/api/products.py
from __future__ import annotations

from typing import Callable, Optional, List, Dict, Any, Tuple
from fastapi import (
    APIRouter, HTTPException, Query, UploadFile, File, Body, Depends, Request
)
from fastapi.responses import Response, FileResponse
from pydantic import BaseModel
from contextlib import contextmanager
import secrets
import io
import os
import shutil
import sqlite3

# ──────────────────────────────────────────────────────────────────────────────
# Optional deps (Excel)
# ──────────────────────────────────────────────────────────────────────────────
try:
    import openpyxl
    _OPENPYXL_AVAILABLE = True
except Exception:
    _OPENPYXL_AVAILABLE = False

# ──────────────────────────────────────────────────────────────────────────────
# Kaspi client (optional)
# ──────────────────────────────────────────────────────────────────────────────
def _safe_import_kaspi():
    for path in ("app.kaspi_client", "..kaspi_client", "kaspi_client"):
        try:
            mod = __import__(path.replace("..", "").replace(".", ""), fromlist=["KaspiClient"])
            return getattr(mod, "KaspiClient", None)
        except Exception:
            continue
    return None

KaspiClient = _safe_import_kaspi()

# ──────────────────────────────────────────────────────────────────────────────
# DB backend switch (PG via SQLAlchemy / fallback SQLite)
# ──────────────────────────────────────────────────────────────────────────────
try:
    from sqlalchemy import create_engine, text
    _SQLA_OK = True
except Exception:
    _SQLA_OK = False

DATABASE_URL = os.getenv("DATABASE_URL")  # e.g. postgresql+psycopg://...
_USE_PG = bool(DATABASE_URL and _SQLA_OK)

def _resolve_db_path() -> str:
    target = os.getenv("DB_PATH", "/data/kaspi-orders.sqlite3")
    tdir = os.path.dirname(target)
    try:
        os.makedirs(tdir, exist_ok=True)
        if os.access(tdir, os.W_OK):
            return target
    except Exception:
        pass
    # fallback — рядом с кодом
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
    if _USE_PG:
        with _engine.begin() as conn:
            yield conn
    else:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

def _q(sql: str):
    return text(sql) if _USE_PG else sql

def _rows_to_dicts(rows):
    if _USE_PG:
        return [dict(r._mapping) for r in rows]
    return [dict(r) for r in rows]

# ──────────────────────────────────────────────────────────────────────────────
# Auth
# ──────────────────────────────────────────────────────────────────────────────
def _require_api_key(req: Request) -> bool:
    api_key = os.getenv("API_KEY") or os.getenv("KASPI_API_KEY") or os.getenv("PRODUCTS_API_KEY")
    if not api_key:
        return True
    sent = req.headers.get("X-API-Key") or req.query_params.get("api_key")
    if sent != api_key:
        raise HTTPException(401, "Invalid API key")
    return True

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
def _gen_batch_code() -> str:
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    return "".join(secrets.choice(alphabet) for _ in range(6))

def _table_exists(c, name: str) -> bool:
    if _USE_PG:
        r = c.execute(_q("SELECT 1 FROM information_schema.tables WHERE table_name=:t"), {"t": name}).first()
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
        return col in {row["name"] for row in rows}

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

def _avg_cost(sku: str) -> float | None:
    _ensure_schema()
    with _db() as c:
        if _USE_PG:
            r = c.execute(_q(
                "SELECT SUM(qty*unit_cost) AS tc, SUM(qty) AS tq FROM batches WHERE sku=:sku"
            ), {"sku": sku}).first()
            if not r or not r._mapping["tq"]:
                return None
            return float(r._mapping["tc"]) / float(r._mapping["tq"])
        else:
            r = c.execute(
                "SELECT SUM(qty*unit_cost) AS tc, SUM(qty) AS tq FROM batches WHERE sku=?",
                (sku,)
            ).fetchone()
            if not r or not r["tq"]:
                return None
            return float(r["tc"]) / float(r["tq"])

def _sku_of(row: dict) -> str:
    return str(
        row.get("sku") or row.get("code") or row.get("vendorCode") or
        row.get("barcode") or row.get("id") or ""
    ).strip()

# ──────────────────────────────────────────────────────────────────────────────
# Schema & migrations
# ──────────────────────────────────────────────────────────────────────────────
def _ensure_schema():
    if _USE_PG:
        with _db() as c:
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
            c.execute(_q("""
            CREATE TABLE IF NOT EXISTS categories(
                name TEXT PRIMARY KEY,
                base_percent DOUBLE PRECISION DEFAULT 0.0,
                extra_percent DOUBLE PRECISION DEFAULT 3.0,
                tax_percent DOUBLE PRECISION DEFAULT 0.0
            );
            """))
            # safety
            c.execute(_q("CREATE INDEX IF NOT EXISTS idx_batches_sku ON batches(sku)"))
            c.execute(_q("CREATE INDEX IF NOT EXISTS idx_batches_date ON batches(date)"))
            # migrations
            if not _has_column(c, "batches", "qty_sold"):
                c.execute(_q("ALTER TABLE batches ADD COLUMN qty_sold INTEGER DEFAULT 0"))
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
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS categories(
                name TEXT PRIMARY KEY,
                base_percent REAL DEFAULT 0.0,
                extra_percent REAL DEFAULT 3.0,
                tax_percent REAL DEFAULT 0.0
            );

            CREATE INDEX IF NOT EXISTS idx_batches_sku ON batches(sku);
            CREATE INDEX IF NOT EXISTS idx_batches_date ON batches(date);
            """)
            # migrations
            cols = {r["name"] for r in c.execute("PRAGMA table_info(batches)")}
            if "commission_pct" not in cols:
                c.execute("ALTER TABLE batches ADD COLUMN commission_pct REAL")
            if "batch_code" not in cols:
                c.execute("ALTER TABLE batches ADD COLUMN batch_code TEXT")
            if "qty_sold" not in cols:
                c.execute("ALTER TABLE batches ADD COLUMN qty_sold INTEGER DEFAULT 0")
            # проставить недостающие batch_code
            for r in c.execute("SELECT id FROM batches WHERE batch_code IS NULL OR batch_code=''").fetchall():
                c.execute("UPDATE batches SET batch_code=? WHERE id=?", (_gen_batch_code(), r["id"]))

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
# Kaspi fetch helpers (подкачка каталога)
# ──────────────────────────────────────────────────────────────────────────────
def _find_iter_fn(client: Any) -> Optional[Callable]:
    for name in ("iter_products", "iter_offers", "iter_catalog"):
        if hasattr(client, name):
            return getattr(client, name)
    return None

def _collect_products_from_kaspi(client: Any, active_only: Optional[bool]) -> List[Dict[str, Any]]:
    iter_fn = _find_iter_fn(client)
    if iter_fn is None:
        raise HTTPException(501, "В kaspi_client нет метода каталога (iter_products/iter_offers/iter_catalog).")
    items: List[Dict[str, Any]] = []
    seen = set()

    def add_row(item: Dict[str, Any]):
        attrs = item.get("attributes", {}) or {}
        pid = (item.get("id")
               or attrs.get("id") or attrs.get("sku") or attrs.get("code")
               or attrs.get("offerId") or attrs.get("name"))
        if not pid:
            return
        code = (attrs.get("code") or attrs.get("sku") or attrs.get("offerId")
                or attrs.get("article") or attrs.get("barcode") or pid)
        if code in seen:
            return
        seen.add(code)
        name = (attrs.get("name") or attrs.get("title") or attrs.get("productName")
                or attrs.get("offerName") or code)
        price = _to_float(attrs.get("price") or attrs.get("basePrice")
                          or attrs.get("salePrice") or attrs.get("currentPrice") or attrs.get("totalPrice"))
        qty = _to_int(attrs.get("quantity") or attrs.get("availableAmount")
                      or attrs.get("stockQuantity") or attrs.get("qty"))
        brand = (attrs.get("brand") or attrs.get("producer") or attrs.get("manufacturer"))
        category = (attrs.get("category") or attrs.get("categoryName") or attrs.get("group"))
        barcode = (attrs.get("barcode") or attrs.get("ean"))
        active_val = attrs.get("active") or attrs.get("isActive") or attrs.get("isPublished") \
                     or attrs.get("visible") or attrs.get("isVisible") or attrs.get("status")
        active: Optional[bool]
        if active_val is None:
            active = None
        else:
            s = str(active_val).strip().lower()
            active = True if s in ("1","true","yes","on","published","active") else False if s in ("0","false","no","off") else None
        if active_only and active is False:
            return
        items.append({
            "id": code, "code": code, "sku": code,
            "name": name, "price": price, "qty": qty, "active": active,
            "brand": brand, "category": category, "barcode": barcode,
        })

    try:
        # большинство реализаций поддерживают active_only
        try:
            for it in iter_fn(active_only=bool(active_only) if active_only is not None else True):
                add_row(it)
        except TypeError:
            for it in iter_fn():
                add_row(it)
    except Exception as e:
        raise HTTPException(502, f"Ошибка при чтении каталога Kaspi: {e}")
    return items

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

# ──────────────────────────────────────────────────────────────────────────────
# FIFO recount helper — обновляет batches.qty_sold из леджера
# ──────────────────────────────────────────────────────────────────────────────
def _recount_qty_sold_from_ledger() -> int:
    _ensure_schema()
    updated = 0
    with _db() as c:
        ledgers = ["profit_fifo_ledger", "fifo_ledger", "ledger_fifo"]
        ledger = next((t for t in ledgers if _table_exists(c, t)), None)
        if not ledger:
            return 0
        batch_col = next((col for col in ("batch_id", "bid", "batch") if _has_column(c, ledger, col)), None)
        qty_col   = next((col for col in ("qty", "quantity", "qty_used") if _has_column(c, ledger, col)), None)
        if not batch_col or not qty_col:
            return 0

        if _USE_PG:
            rows = c.execute(_q(
                f"SELECT {batch_col} AS bid, SUM({qty_col}) AS used FROM {ledger} GROUP BY {batch_col}"
            )).all()
            used = {int(r._mapping["bid"]): int(r._mapping["used"] or 0) for r in rows}
            c.execute(_q("UPDATE batches SET qty_sold = COALESCE(qty_sold,0)"))
            for bid, val in used.items():
                r = c.execute(_q(
                    "UPDATE batches SET qty_sold=:v WHERE id=:bid AND COALESCE(qty_sold,0) <> :v"
                ), {"v": int(val), "bid": int(bid)})
                updated += (r.rowcount or 0)
            # защита
            c.execute(_q("UPDATE batches SET qty_sold = qty WHERE qty_sold > qty"))
        else:
            rows = c.execute(
                f"SELECT {batch_col} AS bid, SUM({qty_col}) AS used FROM {ledger} GROUP BY {batch_col}"
            ).fetchall()
            used = {int(r["bid"]): int(r["used"] or 0) for r in rows}
            c.execute("UPDATE batches SET qty_sold = COALESCE(qty_sold,0)")
            for bid, val in used.items():
                cur = c.execute(
                    "UPDATE batches SET qty_sold=? WHERE id=? AND COALESCE(qty_sold,0) <> ?",
                    (int(val), int(bid), int(val))
                )
                updated += cur.rowcount or 0
            c.execute("UPDATE batches SET qty_sold = qty WHERE qty_sold > qty")
    return updated

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

    # ──────────────────────────────────────────────────────────────────────
    # DB: список товаров (+ мета по партиям) — под твой HTML
    # ──────────────────────────────────────────────────────────────────────
    @router.get("/db/list")
    async def db_list(active_only: int = Query(1), search: str = Query("", alias="q")):
        _ensure_schema()
        _seed_categories_if_empty()
        with _db() as c:
            # категории (для комиссий)
            if _USE_PG:
                cats_rows = c.execute(_q(
                    "SELECT name, base_percent, extra_percent, tax_percent FROM categories ORDER BY name"
                )).all()
                cats = {r._mapping["name"]: dict(r._mapping) for r in cats_rows}
                # продукты
                sql = "SELECT sku,name,brand,category,price,quantity,active FROM products"
                conds, params = [], {}
                if active_only:
                    conds.append("active=1")
                if search:
                    conds.append("(sku ILIKE :q OR name ILIKE :q)")
                    params["q"] = f"%{search}%"
                if conds:
                    sql += " WHERE " + " AND ".join(conds)
                sql += " ORDER BY name"
                rows = c.execute(_q(sql), params).all()
            else:
                cats = {r["name"]: dict(r) for r in c.execute("SELECT * FROM categories")}
                sql = "SELECT sku,name,brand,category,price,quantity,active FROM products"
                conds, params = [], []
                if active_only:
                    conds.append("active=1")
                if search:
                    conds.append("(sku LIKE ? OR name LIKE ?)")
                    params += [f"%{search}%", f"%{search}%"]
                if conds:
                    sql += " WHERE " + " AND ".join(conds)
                sql += " ORDER BY name COLLATE NOCASE"
                rows = c.execute(sql, params).fetchall()
            rows = _rows_to_dicts(rows)

            # мета по партиям
            if _USE_PG:
                bc_rows = c.execute(_q("SELECT sku, COUNT(*) AS cnt FROM batches GROUP BY sku")).all()
                bc = {r._mapping["sku"]: r._mapping["cnt"] for r in bc_rows}
                last_rows = c.execute(_q(
                    "SELECT DISTINCT ON (sku) sku, unit_cost, commission_pct FROM batches ORDER BY sku, date DESC, id DESC"
                )).all()
                last = {r._mapping["sku"]: (r._mapping["unit_cost"], r._mapping["commission_pct"]) for r in last_rows}
                left_rows = c.execute(_q(
                    "SELECT sku, SUM(qty - COALESCE(qty_sold,0)) AS left FROM batches GROUP BY sku"
                )).all()
                left_by_sku = {r._mapping["sku"]: int(r._mapping["left"] or 0) for r in left_rows}
            else:
                bc = {r["sku"]: r["cnt"] for r in c.execute(
                    "SELECT sku, COUNT(*) AS cnt FROM batches GROUP BY sku"
                )}
                last = {}
                for r in c.execute(
                    "SELECT sku, unit_cost, commission_pct FROM batches ORDER BY sku, date DESC, id DESC"
                ):
                    if r["sku"] not in last:
                        last[r["sku"]] = (r["unit_cost"], r["commission_pct"])
                left_by_sku = {r["sku"]: int(r["left"] or 0) for r in c.execute(
                    "SELECT sku, SUM(qty - COALESCE(qty_sold,0)) AS left FROM batches GROUP BY sku"
                ).fetchall()}

        items: List[Dict[str, Any]] = []
        for r in rows:
            sku = r["sku"]
            price = float(r.get("price") or 0)
            qty = int(r.get("quantity") or 0)
            cat = r.get("category") or ""
            commissions = cats.get(cat)
            last_margin = None
            if sku in last:
                ucost, comm = last[sku]
                eff_comm = float(comm) if comm is not None else (
                    (float(commissions["base_percent"]) + float(commissions["extra_percent"]) + float(commissions["tax_percent"]))
                    if commissions else 0.0
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

    # Массовый upsert из таблицы (кнопка «Сохранить таблицу в БД»)
    @router.post("/db/bulk-upsert", dependencies=[Depends(_require_api_key)])
    async def bulk_upsert(rows: List[Dict[str, Any]] = Body(...)):
        if not isinstance(rows, list):
            raise HTTPException(400, "Body должен быть списком объектов")
        inserted, updated = _upsert_products(rows)
        return {"ok": True, "inserted": inserted, "updated": updated}

    # Экспорт CSV именно из БД (под кнопку «Экспорт CSV (БД)»)
    @router.get("/db/export.csv")
    async def export_db_csv(active_only: int = Query(1), q: str = Query("", alias="search")):
        _ensure_schema()
        with _db() as c:
            if _USE_PG:
                sql = "SELECT sku,name,brand,category,price,quantity,active,barcode FROM products"
                conds, params = [], {}
                if active_only:
                    conds.append("active=1")
                if q:
                    conds.append("(sku ILIKE :q OR name ILIKE :q)")
                    params["q"] = f"%{q}%"
                if conds:
                    sql += " WHERE " + " AND ".join(conds)
                sql += " ORDER BY name"
                rows = c.execute(_q(sql), params).all()
                rows = _rows_to_dicts(rows)
            else:
                sql = "SELECT sku,name,brand,category,price,quantity,active,barcode FROM products"
                conds, params = [], []
                if active_only:
                    conds.append("active=1")
                if q:
                    conds.append("(sku LIKE ? OR name LIKE ?)")
                    params += [f"%{q}%", f"%{q}%"]
                if conds:
                    sql += " WHERE " + " AND ".join(conds)
                sql += " ORDER BY name COLLATE NOCASE"
                rows = [dict(r) for r in c.execute(sql, params).fetchall()]

        def esc(s: Any) -> str:
            s = "" if s is None else str(s)
            if any(c in s for c in [",", '"', "\n"]):
                s = '"' + s.replace('"', '""') + '"'
            return s

        header = "sku,name,brand,category,price,quantity,active,barcode\n"
        body = "".join(
            ",".join(esc(x) for x in [
                r["sku"], r["name"], r.get("brand") or "", r.get("category") or "",
                r.get("price") or 0, r.get("quantity") or 0, 1 if r.get("active") else 0, r.get("barcode") or ""
            ]) + "\n"
            for r in rows
        )
        return Response(content=header + body,
                        media_type="text/csv; charset=utf-8",
                        headers={"Content-Disposition": 'attachment; filename="products-db.csv"'})

    # Ручная загрузка (XML/XLSX) → в БД
    @router.post("/manual-upload", dependencies=[Depends(_require_api_key)])
    async def manual_upload(file: UploadFile = File(...)):
        filename = (file.filename or "").lower()
        content = await file.read()

        def _parse_xml(raw: bytes) -> List[Dict[str, Any]]:
            from xml.etree import ElementTree as ET
            try:
                root = ET.fromstring(raw)
            except ET.ParseError as e:
                raise HTTPException(400, f"Некорректный XML: {e}")
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
                name = child_text(off, "model", "name", "title") or code
                brand = child_text(off, "brand")
                # availability
                qty, active = 0, None
                for el in off.iter():
                    if strip(el.tag) == "availability":
                        sc = el.get("stockCount")
                        if sc:
                            try: qty = int(float(sc))
                            except Exception: qty = 0
                        av = (el.get("available") or "").strip().lower()
                        if av in ("yes","true","1"): active = True
                        elif av in ("no","false","0"): active = False
                        break
                # price
                price = 0.0
                for el in off.iter():
                    if strip(el.tag) == "cityprice":
                        txt = (el.text or "").strip()
                        if txt:
                            try: price = float(txt.replace(" ", "").replace(",", "."))
                            except Exception: price = 0.0
                        break
                rows.append({
                    "id": code, "code": code, "sku": code,
                    "name": name, "brand": brand or None, "qty": qty, "price": price, "active": active,
                })
            return rows

        def _parse_excel(raw: bytes) -> List[Dict[str, Any]]:
            if not _OPENPYXL_AVAILABLE:
                raise HTTPException(500, "openpyxl не установлен на сервере.")
            try:
                wb = openpyxl.load_workbook(io.BytesIO(raw), read_only=True, data_only=True)
            except Exception as e:
                raise HTTPException(400, f"Не удалось открыть Excel: {e}")
            ws = wb.active
            headers = [str(c.value or '').strip() for c in ws[1]]
            out: List[Dict[str, Any]] = []
            for row in ws.iter_rows(min_row=2, values_only=True):
                item = {h: v for h, v in zip(headers, row)}
                if any(v not in (None, "", []) for v in item.values()):
                    out.append(item)
            return out

        if filename.endswith(".xml"):
            raw_rows = _parse_xml(content)
        elif filename.endswith(".xlsx") or filename.endswith(".xls"):
            raw_rows = _parse_excel(content)
        else:
            raise HTTPException(400, "Поддерживаются только XML или Excel (.xlsx/.xls).")

        normalized: List[Dict[str, Any]] = []
        for r in raw_rows:
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

    # ──────────────────────────────────────────────────────────────────────
    # Партии (просмотр/добавление/редактирование/удаление)
    # ──────────────────────────────────────────────────────────────────────
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

    @router.post("/db/price-batches/{sku}", dependencies=[Depends(_require_api_key)])
    async def add_batches(sku: str, payload: BatchListIn = Body(...)):
        _ensure_schema()
        with _db() as c:
            for e in payload.entries:
                code = e.batch_code or _gen_batch_code()
                if _USE_PG:
                    c.execute(_q(
                        "INSERT INTO batches(sku,date,qty,unit_cost,note,commission_pct,batch_code,qty_sold) "
                        "VALUES(:sku,:date,:qty,:ucost,:note,:comm,:code,0)"
                    ), {"sku": sku, "date": e.date, "qty": int(e.qty),
                        "ucost": float(e.unit_cost), "note": e.note,
                        "comm": float(e.commission_pct) if e.commission_pct is not None else None,
                        "code": code})
                else:
                    c.execute(
                        "INSERT INTO batches(sku,date,qty,unit_cost,note,commission_pct,batch_code,qty_sold) "
                        "VALUES(?,?,?,?,?,?,?,0)",
                        (sku, e.date, int(e.qty), float(e.unit_cost), e.note,
                         float(e.commission_pct) if e.commission_pct is not None else None, code)
                    )
        avgc = _avg_cost(sku)
        return {"ok": True, "avg_cost": round(avgc, 2) if avgc is not None else None}

    @router.put("/db/price-batches/{sku}/{bid}", dependencies=[Depends(_require_api_key)])
    async def update_batch(sku: str, bid: int, payload: Dict[str, Any] = Body(...)):
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
            return {"ok": True, "status": "noop"}
        _ensure_schema()
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
        return {"ok": True}

    @router.delete("/db/price-batches/{sku}/{bid}", dependencies=[Depends(_require_api_key)])
    async def delete_batch(sku: str, bid: int):
        _ensure_schema()
        with _db() as c:
            # запрет удаления, если уже есть списания
            if _USE_PG:
                r = c.execute(_q("SELECT COALESCE(qty_sold,0) AS s FROM batches WHERE id=:bid AND sku=:sku"),
                              {"bid": bid, "sku": sku}).first()
                if not r:
                    raise HTTPException(404, "Batch not found")
                if int(r._mapping["s"]) > 0:
                    raise HTTPException(400, "Cannot delete: batch has sales")
                c.execute(_q("DELETE FROM batches WHERE id=:bid AND sku=:sku"), {"bid": bid, "sku": sku})
            else:
                r = c.execute("SELECT COALESCE(qty_sold,0) AS s FROM batches WHERE id=? AND sku=?", (bid, sku)).fetchone()
                if not r:
                    raise HTTPException(404, "Batch not found")
                if int(r["s"]) > 0:
                    raise HTTPException(400, "Cannot delete: batch has sales")
                c.execute("DELETE FROM batches WHERE id=? AND sku=?", (bid, sku))
        return {"ok": True}

    # Пересчитать qty_sold из леджера (кнопка «Пересчитать списания (FIFO)»)
    @router.post("/batches/recount-sold", dependencies=[Depends(_require_api_key)])
    async def batches_recount_sold():
        changed = _recount_qty_sold_from_ledger()
        return {"ok": True, "updated_batches": int(changed)}

    # История списаний по SKU (для блока «история под товаром»)
    @router.get("/db/ledger/{sku}")
    async def ledger_by_sku(sku: str, limit: int = Query(200, ge=1, le=2000)):
        _ensure_schema()
        with _db() as c:
            if not _table_exists(c, "profit_fifo_ledger"):
                return {"ok": True, "items": []}
            if _USE_PG:
                rows = c.execute(_q("""
                    SELECT id, order_code, date_utc_ms, line_index, qty, unit_price, total_price,
                           batch_id, unit_cost, commission_pct, commission_amount, cost_amount, profit_amount
                      FROM profit_fifo_ledger
                     WHERE sku=:sku
                     ORDER BY date_utc_ms DESC, id DESC
                     LIMIT :lim
                """), {"sku": sku, "lim": limit}).all()
                rows = _rows_to_dicts(rows)
            else:
                rows = [dict(r) for r in c.execute("""
                    SELECT id, order_code, date_utc_ms, line_index, qty, unit_price, total_price,
                           batch_id, unit_cost, commission_pct, commission_amount, cost_amount, profit_amount
                      FROM profit_fifo_ledger
                     WHERE sku=?
                     ORDER BY date_utc_ms DESC, id DESC
                     LIMIT ?
                """, (sku, limit)).fetchall()]
        return {"ok": True, "items": rows}

    # Партии + продано из леджера
    @router.get("/db/price-batches-with-sold/{sku}")
    async def price_batches_with_sold(sku: str):
        _ensure_schema()
        with _db() as c:
            if not _table_exists(c, "profit_fifo_ledger"):
                if _USE_PG:
                    rows = c.execute(_q(
                        "SELECT id, date, qty, 0 AS sold_qty, unit_cost, note, commission_pct, batch_code "
                        "FROM batches WHERE sku=:s ORDER BY date, id"
                    ), {"s": sku}).all()
                    rows = _rows_to_dicts(rows)
                else:
                    rows = [dict(r) for r in c.execute(
                        "SELECT id, date, qty, 0 AS sold_qty, unit_cost, note, commission_pct, batch_code "
                        "FROM batches WHERE sku=? ORDER BY date, id", (sku,)
                    ).fetchall()]
                return {"ok": True, "sku": sku, "batches": rows}
            if _USE_PG:
                rows = c.execute(_q(
                    "SELECT b.id, b.date, b.qty, COALESCE(l.sold,0) AS sold_qty, "
                    "       b.unit_cost, b.note, b.commission_pct, b.batch_code "
                    "  FROM batches b "
                    "  LEFT JOIN (SELECT batch_id, SUM(qty) AS sold FROM profit_fifo_ledger GROUP BY batch_id) l "
                    "    ON l.batch_id = b.id "
                    " WHERE b.sku=:s "
                    " ORDER BY b.date, b.id"
                ), {"s": sku}).all()
                rows = _rows_to_dicts(rows)
            else:
                rows = [dict(r) for r in c.execute(
                    "SELECT b.id, b.date, b.qty, COALESCE(l.sold,0) AS sold_qty, "
                    "       b.unit_cost, b.note, b.commission_pct, b.batch_code "
                    "  FROM batches b "
                    "  LEFT JOIN (SELECT batch_id, SUM(qty) AS sold FROM profit_fifo_ledger GROUP BY batch_id) l "
                    "    ON l.batch_id = b.id "
                    " WHERE b.sku=? "
                    " ORDER BY b.date, b.id", (sku,)
                ).fetchall()]
        return {"ok": True, "sku": sku, "batches": rows}

    # ──────────────────────────────────────────────────────────────────────
    # Простая ensure-sku (под API вызовы, когда нужно завести SKU «вручную»)
    # ──────────────────────────────────────────────────────────────────────
    @router.post("/db/ensure-sku/{sku}", dependencies=[Depends(_require_api_key)])
    async def ensure_sku(
        sku: str,
        name: str = Query(...),
        price: float = Query(0.0),
        qty: int = Query(0),
        active: int = Query(1),
        brand: Optional[str] = None,
        category: Optional[str] = None,
        barcode: Optional[str] = None,
    ):
        _ensure_schema()
        payload = [{
            "sku": sku, "code": sku, "name": name,
            "price": float(price), "qty": int(qty),
            "active": int(active), "brand": brand,
            "category": category, "barcode": barcode,
        }]
        ins, upd = _upsert_products(payload)
        return {"ok": True, "inserted": ins, "updated": upd}

    # ──────────────────────────────────────────────────────────────────────
    # Синхронизация цен/остатков из Kaspi (новое под твою просьбу)
    # ──────────────────────────────────────────────────────────────────────
    @router.post("/sync/kaspi", dependencies=[Depends(_require_api_key)])
    async def sync_from_kaspi(active_only: int = Query(1), limit: int = Query(0, ge=0, le=20000)):
        if client is None or KaspiClient is None:
            raise HTTPException(501, "KaspiClient не сконфигурирован на сервере.")
        items = _collect_products_from_kaspi(client, active_only=bool(active_only))
        if limit > 0:
            items = items[:limit]
        ins, upd = _upsert_products(items)
        return {"ok": True, "received": len(items), "inserted": ins, "updated": upd}

    # ──────────────────────────────────────────────────────────────────────
    # Бэкап/восстановление (SQLite only — элементы скрываются в UI под PG)
    # ──────────────────────────────────────────────────────────────────────
    @router.get("/db/backup.sqlite3")
    async def backup_db():
        _ensure_schema()
        if _USE_PG:
            raise HTTPException(501, "Backup доступен только для локальной SQLite.")
        fname = os.path.basename(DB_PATH) or "data.sqlite3"
        return FileResponse(DB_PATH, media_type="application/octet-stream", filename=fname)

    @router.post("/db/restore")
    async def restore_db(file: UploadFile = File(...)):
        if _USE_PG:
            raise HTTPException(501, "Restore доступен только для локальной SQLite.")
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
        return {"ok": True, "integrity": ok}

    return router
