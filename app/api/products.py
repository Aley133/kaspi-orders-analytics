from __future__ import annotations
from typing import Callable, Optional, List, Dict, Any, Tuple
from fastapi import APIRouter, HTTPException, Query, UploadFile, File, Body, Depends, Request
from fastapi.responses import Response, FileResponse
from pydantic import BaseModel
import secrets
import io
import os, shutil
import sqlite3
from xml.etree import ElementTree as ET


def require_api_key(req: Request) -> bool:
    api_key = os.getenv("API_KEY")
    if not api_key:        # если ключ не задан, пускать всех (режим dev)
        return True
    # принимаем либо заголовок, либо query param ?api_key=
    sent = req.headers.get("X-API-Key") or req.query_params.get("api_key")
    if sent != api_key:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return True
    
# ===== optional deps for Excel =====
try:
    import openpyxl  # for .xlsx
    _OPENPYXL_AVAILABLE = True
except Exception:
    _OPENPYXL_AVAILABLE = False

# ===== optional imports from kaspi_client =====
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
            normalize_row = None  # fallback ниже

# ====== DB backend switch (PG via SQLAlchemy / fallback SQLite) ======
from contextlib import contextmanager

try:
    from sqlalchemy import create_engine, text, select, func  # type: ignore
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

# Миграция старого файла БД (если был) в /data
_OLD_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data.sqlite3"))
if DB_PATH != _OLD_PATH and os.path.exists(_OLD_PATH) and not os.path.exists(DB_PATH):
    try:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        shutil.copy2(_OLD_PATH, DB_PATH)
    except Exception:
        pass

# SQL helpers
if _USE_PG:
    _engine = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)

@contextmanager
def _db():
    """
    Универсальный контекст:
    - для PG возвращает SQLAlchemy Connection (begin)
    - для SQLite возвращает sqlite3.Connection
    """
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
    """Обёртка: в PG используем text(), в SQLite возвращаем сырой SQL"""
    return text(sql) if _USE_PG else sql

def _rows_to_dicts(rows):
    """Приводит результат rowset к списку dict для обоих бэкендов"""
    if _USE_PG:
        return [dict(r._mapping) for r in rows]
    return [dict(r) for r in rows]

# ===== генерация кода партии для SQLite; в PG генерим на стороне SQL
def _gen_batch_code_sqlite(conn: sqlite3.Connection) -> str:
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    while True:
        code = "".join(secrets.choice(alphabet) for _ in range(6))
        if not conn.execute("SELECT 1 FROM batches WHERE batch_code=?", (code,)).fetchone():
            return code

# ===== создание схемы
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
            # сгенерировать отсутствующие batch_code
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
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(sku) REFERENCES products(sku) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_batches_sku ON batches(sku);

            CREATE TABLE IF NOT EXISTS categories(
                name TEXT PRIMARY KEY,
                base_percent REAL DEFAULT 0.0,
                extra_percent REAL DEFAULT 3.0,
                tax_percent REAL DEFAULT 0.0
            );
            """)
            cols = {r["name"] for r in c.execute("PRAGMA table_info(batches)")}
            if "commission_pct" not in cols:
                c.execute("ALTER TABLE batches ADD COLUMN commission_pct REAL")
            if "batch_code" not in cols:
                c.execute("ALTER TABLE batches ADD COLUMN batch_code TEXT")
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

def _upsert_products(items: List[Dict[str, Any]]) -> Tuple[int,int]:
    """
    Upsert в products (PG или SQLite).
    """
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

# ===== Kaspi client (optional) =====
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

# ====== Pydantic ======
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

# ====== Router factory ======
def get_products_router(client: Optional["KaspiClient"]) -> APIRouter:
    router = APIRouter(tags=["products"])

    # ---- Debug ping (для UI и проверки префикса) ----
    @router.get("/db/ping")
    async def db_ping():
        _ensure_schema()
        if _USE_PG:
            with _db() as c:
                c.execute(_q("SELECT 1"))
            return {"ok": True, "db_path": os.getenv("DATABASE_URL", "")}
        else:
            with _db() as c:
                ok = c.execute("PRAGMA integrity_check").fetchone()[0]
            return {"ok": ok == "ok", "db_path": DB_PATH}

    # ---- Каталог из Kaspi API ----
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
                        r["id"],
                        r["code"],
                        r["name"],
                        r["price"],
                        r["qty"],
                        1 if r["active"] else 0 if r["active"] is False else "",
                        r["brand"],
                        r["category"],
                        r["barcode"],
                    ]
                )
                + "\n"
                for r in items
            ]
        )
        csv = header + body
        return Response(
            content=csv,
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": 'attachment; filename="products.csv"'},
        )

    # ---- Ручная загрузка (XML/Excel) -> в БД ----
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
                        try:
                            qty = int(float(sc))
                        except Exception:
                            qty = 0
                    av = (el.get("available") or "").strip().lower()
                    if av in ("yes", "true", "1"):
                        active = True
                    elif av in ("no", "false", "0"):
                        active = False
                    break
            price = 0.0
            for el in off.iter():
                if strip(el.tag) == "cityprice":
                    txt = (el.text or "").strip()
                    if txt:
                        try:
                            price = float(txt.replace(" ", "").replace(",", "."))
                        except Exception:
                            price = 0.0
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

    # ---- DB: список товаров (+кол-во партий и маржа последней) ----
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

            # batches meta
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
            else:
                bc = {r["sku"]: r["cnt"] for r in c.execute("SELECT sku, COUNT(*) AS cnt FROM batches GROUP BY sku")}
                last = {}
                for r in c.execute("SELECT sku, date, unit_cost, commission_pct FROM batches ORDER BY date"):
                    last[r["sku"]] = (r["unit_cost"], r["commission_pct"])

        items: List[Dict[str, Any]] = []
        for r in rows:
            sku = r["sku"]
            price = float(r.get("price") or 0)
            qty = int(r.get("quantity") or 0)
            cat = r.get("category") or ""
            commissions = cats.get(cat)
            # маржа по последней партии, если есть
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
            })
        return {"count": len(items), "items": items}

    # ---- DB: партии закупок ----
    @router.get("/db/price-batches/{sku}")
    async def get_batches(sku: str):
        _ensure_schema()
        with _db() as c:
            if _USE_PG:
                rows = c.execute(_q(
                    "SELECT id, date, qty, unit_cost, commission_pct, batch_code, note "
                    "FROM batches WHERE sku=:sku ORDER BY date"
                ), {"sku": sku}).all()
                rows = _rows_to_dicts(rows)
            else:
                rows = [dict(r) for r in c.execute(
                    "SELECT id, date, qty, unit_cost, commission_pct, batch_code, note "
                    "FROM batches WHERE sku=? ORDER BY date", (sku,)
                )]
        avgc = _avg_cost(sku)
        return {"batches": rows, "avg_cost": round(avgc, 2) if avgc is not None else None}

    @router.post("/db/price-batches/{sku}")
    async def add_batches(sku: str, payload: BatchListIn = Body(...), _: bool = Depends(require_api_key)):
        _ensure_schema()
        with _db() as c:
            for e in payload.entries:
                if _USE_PG:
                    # генерим код на стороне PG, если не задан
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
                        "INSERT INTO batches(sku,date,qty,unit_cost,note,commission_pct,batch_code) "
                        "VALUES(:sku,:date,:qty,:ucost,:note,:comm,:code)"
                    ), {
                        "sku": sku,
                        "date": e.date,
                        "qty": int(e.qty),
                        "ucost": float(e.unit_cost),
                        "note": e.note,
                        "comm": float(e.commission_pct) if e.commission_pct is not None else None,
                        "code": code
                    })
                else:
                    code = e.batch_code or _gen_batch_code_sqlite(c)
                    c.execute(
                        "INSERT INTO batches(sku,date,qty,unit_cost,note,commission_pct,batch_code) "
                        "VALUES(?,?,?,?,?,?,?)",
                        (sku, e.date, int(e.qty), float(e.unit_cost), e.note,
                         float(e.commission_pct) if e.commission_pct is not None else None,
                         code)
                    )
        avgc = _avg_cost(sku)
        return {"status": "ok", "avg_cost": round(avgc, 2) if avgc is not None else None}

    @router.delete("/db/price-batches/{sku}/{bid}")
    async def delete_batch(sku: str, bid: int, _: bool = Depends(require_api_key)):
        _ensure_schema()
        with _db() as c:
            if _USE_PG:
                c.execute(_q("DELETE FROM batches WHERE id=:bid AND sku=:sku"), {"bid": bid, "sku": sku})
            else:
                c.execute("DELETE FROM batches WHERE id=? AND sku=?", (bid, sku))
        return {"status": "ok"}

    # ---- DB: категории/комиссии ----
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

    # ---- Бэкап/восстановление (актуально только для SQLite) ----
    @router.get("/db/backup.sqlite3")
    async def backup_db():
        _ensure_schema()
        if _USE_PG:
            # на Postgres файлового бэкапа нет — можно скрыть кнопку в UI, но ручку оставим с аккуратным ответом
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
        # контроль целостности
        with _db() as c:
            ok = c.execute("PRAGMA integrity_check").fetchone()[0]
        return {"status": "ok", "integrity": ok}

    return router
