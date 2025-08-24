# products_router.py
from __future__ import annotations
from typing import Callable, Optional, List, Dict, Any, Tuple
from fastapi import APIRouter, HTTPException, Query, UploadFile, File, Body
from fastapi.responses import Response, JSONResponse, FileResponse
from pydantic import BaseModel
import io, os, shutil, sqlite3, random, string
from xml.etree import ElementTree as ET

# ===== optional deps for Excel =====
try:
    import openpyxl  # for .xlsx
    _OPENPYXL_AVAILABLE = True
except Exception:
    _OPENPYXL_AVAILABLE = False

# ===== kaspi_client =====
try:
    from app.kaspi_client import KaspiClient, ProductStock, normalize_row  # type: ignore
except Exception:
    try:
        from ..kaspi_client import KaspiClient, ProductStock, normalize_row  # type: ignore
    except Exception:
        try:
            from kaspi_client import KaspiClient, ProductStock, normalize_row  # type: ignore
        except Exception:
            KaspiClient = None  # type: ignore
            ProductStock = None
            normalize_row = None

# =============================================================================
# DB path (persistent disk first, fallback to local)
# =============================================================================
def _resolve_db_path() -> str:
    target = os.getenv("DB_PATH", "/data/kaspi-orders.sqlite3")
    try:
        os.makedirs(os.path.dirname(target), exist_ok=True)
        if os.access(os.path.dirname(target), os.W_OK):
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

# =============================================================================
# DB helpers / schema
# =============================================================================
def _db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def _ensure_schema():
    with _db() as c:
        c.executescript("""
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS products(
            sku TEXT PRIMARY KEY,
            name TEXT,
            brand TEXT,
            category TEXT,              -- привязка к нашей таблице categories (по code) или произвольный текст
            price REAL,
            quantity INTEGER,
            active INTEGER DEFAULT 1,
            barcode TEXT,
            updated_at TEXT
        );
        CREATE TABLE IF NOT EXISTS batches(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sku TEXT NOT NULL,
            batch_no TEXT NOT NULL,     -- 6-символьный код партии
            date TEXT NOT NULL,
            qty INTEGER NOT NULL,
            unit_cost REAL NOT NULL,
            note TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(sku) REFERENCES products(sku) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_batches_sku ON batches(sku);

        -- категории/комиссии
        CREATE TABLE IF NOT EXISTS categories(
            code TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            commission_percent REAL NOT NULL,  -- комиссия Kaspi для этой категории
            extra_percent REAL NOT NULL DEFAULT 3.0,   -- ваши доп. проценты (НДС, соц.налоги и т.п.)
            delivery_kzt REAL NOT NULL DEFAULT 0.0     -- фикс. доставка/логистика на 1 заказ
        );
        """)

        # seed дефолтных категорий, если пусто
        has_cat = c.execute("SELECT COUNT(*) AS cnt FROM categories").fetchone()["cnt"]
        if not has_cat:
            defaults = [
                ("vitamins", "Витамины/БАДы", 13.0, 3.0, 0.0),
                ("beauty",   "Красота/Уход",   10.0, 3.0, 0.0),
                ("kids",     "Детские товары", 10.0, 3.0, 0.0),
                ("tech",     "Бытовая техника", 5.0,  3.0, 0.0),
            ]
            c.executemany("INSERT INTO categories(code,title,commission_percent,extra_percent,delivery_kzt) VALUES(?,?,?,?,?)", defaults)

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

def _gen_batch_no() -> str:
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=6))

def _upsert_products(items: List[Dict[str, Any]]) -> None:
    _ensure_schema()
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
                active = 1
            c.execute("""
                INSERT INTO products(sku,name,brand,category,price,quantity,active,barcode,updated_at)
                VALUES(?,?,?,?,?,?,?,?,datetime('now'))
                ON CONFLICT(sku) DO UPDATE SET
                    name=excluded.name,
                    brand=excluded.brand,
                    category=COALESCE(NULLIF(products.category,''), excluded.category),
                    price=excluded.price,
                    quantity=excluded.quantity,
                    active=excluded.active,
                    barcode=excluded.barcode,
                    updated_at=excluded.updated_at
            """, (sku, name, brand, category, price, qty, active, barcode))

# =============================================================================
# Parsers (XML/Excel)
# =============================================================================
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
                if av in ("yes","true","1"): active = True
                elif av in ("no","false","0"): active = False
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
            "code": code,
            "name": name or code,
            "brand": brand or None,
            "qty": qty,
            "price": price,
            "active": active,
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

# =============================================================================
# Kaspi helpers
# =============================================================================
def _pick(attrs: Dict[str, Any], *keys: str) -> str:
    for k in keys:
        v = attrs.get(k)
        if v not in (None, ""):
            return str(v)
    return ""

def _normalize_active(val: Any) -> Optional[bool]:
    if val is None or val == "": return None
    if isinstance(val, bool): return val
    s = str(val).strip().lower()
    if s in ("1","true","yes","on","published","active"): return True
    if s in ("0","false","no","off","unpublished","inactive"): return False
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
        raise HTTPException(501, "В kaspi_client нет метода каталога. Нужен iter_products/iter_offers/iter_catalog.")

    items: List[Dict[str, Any]] = []
    seen = set()
    total = 0
    note: Optional[str] = None

    def add_row(item: Dict[str, Any]):
        nonlocal total
        attrs = item.get("attributes", {}) or {}
        pid = item.get("id") or _pick(attrs, "id", "sku", "code", "offerId") or _pick(attrs, "name")
        if not pid or pid in seen: return
        seen.add(pid); total += 1

        code = _pick(attrs, "code", "sku", "offerId", "article", "barcode")
        name = _pick(attrs, "name", "title", "productName", "offerName")
        price = _num(_pick(attrs, "price", "basePrice", "salePrice", "currentPrice", "totalPrice"))
        qty = int(_num(_pick(attrs, "quantity", "availableAmount", "stockQuantity", "qty")))
        brand = _pick(attrs, "brand", "producer", "manufacturer")
        category = _pick(attrs, "category", "categoryName", "group")
        barcode = _pick(attrs, "barcode", "ean")
        active_val = _normalize_active(_pick(attrs, "active", "isActive", "isPublished", "visible", "isVisible", "status"))

        if active_only and active_val is False: return

        items.append({
            "id": pid,
            "code": code,
            "name": name,
            "price": price,
            "qty": qty,
            "active": True if active_val else False if active_val is False else None,
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
        items = []; total = 0

    if not items:
        try:
            for it in client.iter_products_from_orders(days=60):
                add_row(it)
            note = "Каталог по API недоступен, показаны товары из последних заказов (60 дн)."
        except Exception:
            note = "Каталог по API недоступен."
            items, total = [], 0

    return items, total, note

# =============================================================================
# Pydantic
# =============================================================================
class BatchIn(BaseModel):
    date: str
    qty: int
    unit_cost: float
    note: str | None = None

class BatchListIn(BaseModel):
    entries: List[BatchIn]

class CategoryIn(BaseModel):
    code: str
    title: str
    commission_percent: float
    extra_percent: float = 3.0
    delivery_kzt: float = 0.0

# =============================================================================
# Router
# =============================================================================
def get_products_router(client: Optional["KaspiClient"]) -> APIRouter:
    router = APIRouter(prefix="/products", tags=["products"])
    _ensure_schema()

    # ---- API list (прямо из Kaspi, без записи)
    @router.get("/list")
    async def list_products(active: int = Query(1), q: Optional[str] = None,
                            page: int = Query(1, ge=1),
                            page_size: int = Query(500, ge=1, le=2000)):
        if client is None:
            raise HTTPException(500, "KASPI_TOKEN is not set")
        items, _, note = _collect_products(client, active_only=bool(active))
        if q:
            ql = q.strip().lower()
            items = [r for r in items if ql in (r["name"] or "").lower() or ql in (r["code"] or "").lower()]
        start = (page-1)*page_size
        end = start+page_size
        return {"items": items[start:end], "total": len(items), "note": note}

    # ---- Синхронизация в БД
    @router.post("/db/sync")
    async def db_sync(active: int = Query(1)):
        if client is None:
            raise HTTPException(500, "KASPI_TOKEN is not set")
        items, _, note = _collect_products(client, active_only=bool(active))
        normalized = []
        for r in items:
            normalized.append({
                "code": r.get("code") or r.get("id") or "",
                "name": r.get("name"),
                "brand": r.get("brand"),
                "category": r.get("category"),  # первичная категория из каспи (можно перезаписать вручную)
                "price": r.get("price"),
                "qty": r.get("qty"),
                "active": r.get("active"),
                "barcode": r.get("barcode"),
            })
        _upsert_products(normalized)
        return {"status":"ok","saved":len(normalized),"note":note}

    # ---- Ручной импорт (XML/XLSX) -> БД
    @router.post("/manual-upload")
    async def manual_upload(file: UploadFile = File(...)):
        fn = (file.filename or "").lower()
        content = await file.read()
        if fn.endswith(".xml"):
            raw_rows = _parse_xml(content)
        elif fn.endswith(".xlsx") or fn.endswith(".xls"):
            file.file = io.BytesIO(content)
            raw_rows = _parse_excel(file)
        else:
            raise HTTPException(400, "Поддерживаются XML или Excel (.xlsx/.xls).")

        normalized: List[Dict[str, Any]] = []
        for r in raw_rows:
            if normalize_row:
                try:
                    ps = normalize_row(r); d = ps.to_dict()
                except Exception:
                    d = dict(r)
            else:
                d = dict(r)

            d.setdefault("code", (d.get("sku") or d.get("vendorCode") or d.get("barcode") or d.get("id") or ""))
            d["code"] = str(d["code"]).strip()
            d.setdefault("name", d.get("model") or d.get("title") or d.get("Name") or d.get("name") or d["code"])
            d["name"] = str(d["name"]).strip()
            d["qty"] = _to_int(d.get("qty") or d.get("quantity") or d.get("stock"))
            d["price"] = _to_float(d.get("price"))
            d["brand"] = d.get("brand") or d.get("vendor")
            d["barcode"] = d.get("barcode") or d.get("Barcode")
            normalized.append(d)

        _upsert_products(normalized)
        return {"count": len(normalized), "items": normalized}

    # ---- БД: получить список товаров
    @router.get("/db/list")
    async def db_list(active_only: int = 1, search: str = ""):
        _ensure_schema()
        with _db() as c:
            sql = "SELECT * FROM products"
            conds, params = [], []
            if active_only: conds.append("active=1")
            if search:
                conds.append("(sku LIKE ? OR name LIKE ?)")
                params += [f"%{search}%", f"%{search}%"]
            if conds: sql += " WHERE " + " AND ".join(conds)
            sql += " ORDER BY name COLLATE NOCASE"
            rows = [dict(r) for r in c.execute(sql, params)]
        return {"count": len(rows), "items": rows}

    # ---- БД: выставить категорию товару
    @router.post("/db/product-category/{sku}")
    async def set_product_category(sku: str, payload: Dict[str, Any] = Body(...)):
        code = (payload.get("category") or "").strip()
        with _db() as c:
            c.execute("UPDATE products SET category=? WHERE sku=?", (code, sku))
        return {"status":"ok"}

    # ---- БД: категории CRUD (минимум list+upsert)
    @router.get("/categories")
    async def list_categories():
        with _db() as c:
            rows = [dict(r) for r in c.execute("SELECT code,title,commission_percent,extra_percent,delivery_kzt FROM categories ORDER BY title")]
        return {"items": rows}

    @router.post("/categories")
    async def upsert_category(cat: CategoryIn):
        with _db() as c:
            c.execute("""
                INSERT INTO categories(code,title,commission_percent,extra_percent,delivery_kzt)
                VALUES(?,?,?,?,?)
                ON CONFLICT(code) DO UPDATE SET
                    title=excluded.title,
                    commission_percent=excluded.commission_percent,
                    extra_percent=excluded.extra_percent,
                    delivery_kzt=excluded.delivery_kzt
            """, (cat.code, cat.title, float(cat.commission_percent), float(cat.extra_percent), float(cat.delivery_kzt)))
        return {"status":"ok"}

    # ---- БД: партии
    @router.get("/db/price-batches/{sku}")
    async def get_batches(sku: str):
        with _db() as c:
            rows = [dict(r) for r in c.execute(
                "SELECT id, batch_no, date, qty, unit_cost, note FROM batches WHERE sku=? ORDER BY date",
                (sku,)
            )]
        return {"batches": rows}

    @router.post("/db/price-batches/{sku}")
    async def add_batches(sku: str, payload: BatchListIn = Body(...)):
        with _db() as c:
            for e in payload.entries:
                c.execute(
                    "INSERT INTO batches(sku,batch_no,date,qty,unit_cost,note) VALUES(?,?,?,?,?,?)",
                    (sku, _gen_batch_no(), e.date, int(e.qty), float(e.unit_cost), e.note)
                )
        return {"status": "ok"}

    @router.delete("/db/price-batches/{sku}/{bid}")
    async def delete_batch(sku: str, bid: int):
        with _db() as c:
            c.execute("DELETE FROM batches WHERE id=? AND sku=?", (bid, sku))
        return {"status":"ok"}

    # ---- Бэкап / восстановление БД
    @router.get("/db/backup.sqlite3")
    async def backup_db():
        _ensure_schema()
        fname = os.path.basename(DB_PATH) or "data.sqlite3"
        return FileResponse(DB_PATH, media_type="application/octet-stream", filename=fname)

    @router.post("/db/restore")
    async def restore_db(file: UploadFile = File(...)):
        content = await file.read()
        try:
            with _db() as c:
                c.execute("PRAGMA wal_checkpoint(TRUNCATE);")
        except Exception:
            pass
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        with open(DB_PATH, "wb") as f:
            f.write(content)
        _ensure_schema()
        return {"status": "ok"}

    return router
