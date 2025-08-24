from __future__ import annotations
from typing import Callable, Optional, List, Dict, Any, Tuple
from fastapi import APIRouter, HTTPException, Query, UploadFile, File, Body
from fastapi.responses import Response, JSONResponse, FileResponse
from pydantic import BaseModel

import io
import sqlite3, os, shutil
from xml.etree import ElementTree as ET

# ===== optional deps for Excel =====
try:
    import openpyxl  # for .xlsx
    _OPENPYXL_AVAILABLE = True
except Exception:  # pragma: no cover
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
            normalize_row = None  # fallback ниже обрабатывается

# =============================================================================
# DB path (persistent disk first, fallback to local) + auto-migration
# =============================================================================
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

# migrate old file into /data once
_OLD_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data.sqlite3"))
if DB_PATH != _OLD_PATH and os.path.exists(_OLD_PATH) and not os.path.exists(DB_PATH):
    try:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        shutil.copy2(_OLD_PATH, DB_PATH)
    except Exception:
        pass

# =============================================================================
# DB helpers
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
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            base_percent REAL DEFAULT 0,   -- комиссия Kaspi
            extra_percent REAL DEFAULT 3,  -- +3% как просили
            tax_percent REAL DEFAULT 0     -- прочие налоги/соц/пенс.
        );
        """)

def _seed_categories_if_empty():
    _ensure_schema()
    with _db() as c:
        cnt = c.execute("SELECT COUNT(*) FROM categories").fetchone()[0]
        if cnt == 0:
            # Базовый набор — поправите в UI
            defaults = [
                ("Витамины/БАДы", 10.0, 3.0, 0.0),
                ("Сад/освещение", 10.0, 3.0, 0.0),
                ("Товары для дома", 10.0, 3.0, 0.0),
                ("Прочее", 10.0, 3.0, 0.0),
            ]
            c.executemany(
                "INSERT INTO categories(name,base_percent,extra_percent,tax_percent) VALUES(?,?,?,?)",
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
                active = 1 if qty > 0 else 0  # запасной вариант
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

# =============================================================================
# Parsers
# =============================================================================
def _parse_xml(content: bytes) -> List[Dict[str, Any]]:
    """
    Kaspi catalog XML (xmlns="kaspiShopping").
    Берём:
      code/id <- @sku | @shop-sku | @code | @id
      name    <- <model> | <name> | <title>
      brand   <- <brand>
      qty     <- <availabilities>/<availability @stockCount>
      price   <- <cityprices>/<cityprice> (первый)
      active  <- <availability @available="yes">
    """
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
        code = (
            off.get("sku")
            or off.get("shop-sku")
            or off.get("code")
            or off.get("id")
            or ""
        ).strip()

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
            "id": code,          # важно: теперь и id и code одинаковы
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
# Kaspi client
# =============================================================================
try:
    from app.kaspi_client import KaspiClient  # type: ignore
except Exception:
    try:
        from ..kaspi_client import KaspiClient  # type: ignore
    except Exception:
        from kaspi_client import KaspiClient  # type: ignore

# =============================================================================
# Utils to normalize products coming from API
# =============================================================================
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
        raise HTTPException(status_code=501, detail="В kaspi_client нет метода каталога. Ожидается iter_products/iter_offers/iter_catalog.")

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

# =============================================================================
# Pydantic models
# =============================================================================
class BatchIn(BaseModel):
    date: str
    qty: int
    unit_cost: float
    note: str | None = None

class BatchListIn(BaseModel):
    entries: List[BatchIn]

class CategoryIn(BaseModel):
    name: str
    base_percent: float = 0.0
    extra_percent: float = 3.0
    tax_percent: float = 0.0

# =============================================================================
# Router factory
# =============================================================================
def get_products_router(client: Optional["KaspiClient"]) -> APIRouter:
    router = APIRouter(tags=["products"])

    # ---------- Каталог с Kaspi API ----------
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
        page_items = items[start:end]

        return JSONResponse({"items": page_items, "total": len(items), "note": note})

    @router.get("/export.csv")
    async def export_products_csv(active: int = Query(1)):
        if client is None:
            raise HTTPException(status_code=500, detail="KASPI_TOKEN is not set")
        items, _, _ = _collect_products(client, active_only=bool(active))

        def esc(s: str) -> str:
            s = "" if s is None else str(s)
            if any(c in s for c in [",", '"', "\n"]): s = '"' + s.replace('"', '""') + '"'
            return s

        header = "id,code,name,price,qty,active,brand,category,barcode\n"
        body = "".join([",".join(esc(x) for x in [
            r["id"], r["code"], r["name"], r["price"], r["qty"],
            1 if r["active"] else 0 if r["active"] is False else "",
            r["brand"], r["category"], r["barcode"]
        ]) + "\n" for r in items])
        csv = header + body
        return Response(content=csv, media_type="text/csv; charset=utf-8",
                        headers={"Content-Disposition": 'attachment; filename="products.csv"'})

    @router.get("/probe")
    async def probe_products(active: int = Query(1)):
        if client is None:
            raise HTTPException(status_code=500, detail="KASPI_TOKEN is not set")
        try:
            res = client.probe_catalog(sample_size=2, active_only=bool(active))
            return JSONResponse({"attempts": res})
        except Exception as e:
            return JSONResponse({"attempts": [], "error": str(e)})

    # ---------- Ручная загрузка (XML/Excel) с записью в БД ----------
    @router.post("/manual-upload")
    async def manual_upload(file: UploadFile = File(...)):
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
                    ps = normalize_row(r)
                    d = ps.to_dict()
                except Exception:
                    d = dict(r)
            else:
                d = dict(r)

            # Гарантируем поля
            d.setdefault("code", (d.get("sku") or d.get("vendorCode") or d.get("barcode") or d.get("id") or ""))
            d["code"] = str(d["code"]).strip()
            d.setdefault("id", d["code"])  # ключевая строка: id = code
            d.setdefault("name", d.get("model") or d.get("title") or d.get("Name") or d.get("name") or d["code"])
            d["name"] = str(d["name"]).strip()
            d["qty"] = _to_int(d.get("qty") or d.get("quantity") or d.get("stock"))
            d["price"] = _to_float(d.get("price"))
            d["brand"] = d.get("brand") or d.get("vendor")
            d["barcode"] = d.get("barcode") or d.get("Barcode")
            normalized.append(d)

        _upsert_products(normalized)

        normalized.sort(key=lambda x: (x.get("name") or x.get("Name") or x.get("model") or x.get("title") or '').lower())
        return JSONResponse({"count": len(normalized), "items": normalized})

    # ---------- DB: список товаров ----------
    @router.get("/db/list")
    async def db_list(active_only: int = 1, search: str = ""):
        _ensure_schema()
        _seed_categories_if_empty()
        # словарь категорий -> проценты
        with _db() as c:
            cats = {r["name"]: dict(r) for r in c.execute("SELECT * FROM categories")}
            sql = "SELECT * FROM products"
            conds, params = [], []
            if active_only:
                conds.append("active=1")
            if search:
                conds.append("(sku LIKE ? OR name LIKE ?)")
                params += [f"%{search}%", f"%{search}%"]
            if conds:
                sql += " WHERE " + " AND ".join(conds)
            sql += " ORDER BY name COLLATE NOCASE"
            rows = [dict(r) for r in c.execute(sql, params)]

        items: List[Dict[str, Any]] = []
        for r in rows:
            sku = r["sku"]
            price = float(r.get("price") or 0)
            qty = int(r.get("quantity") or 0)
            cat = r.get("category") or ""
            commissions = cats.get(cat)
            total_commission = None
            if commissions:
                total_commission = float(commissions["base_percent"]) + float(commissions["extra_percent"]) + float(commissions["tax_percent"])
            items.append({
                "code": sku,
                "id": sku,
                "name": r.get("name"),
                "brand": r.get("brand"),
                "category": cat,
                "qty": qty,
                "price": price,
                "active": bool(r.get("active")),
                "commission_total": total_commission,
            })
        return {"count": len(items), "items": items}

    # ---------- DB: партии закупок ----------
    @router.get("/db/price-batches/{sku}")
    async def get_batches(sku: str):
        _ensure_schema()
        with _db() as c:
            rows = [dict(r) for r in c.execute(
                "SELECT id, date, qty, unit_cost, note FROM batches WHERE sku=? ORDER BY date,id",
                (sku,)
            )]
        return {"batches": rows}

    @router.post("/db/price-batches/{sku}")
    async def add_batches(sku: str, payload: BatchListIn = Body(...)):
        _ensure_schema()
        with _db() as c:
            for e in payload.entries:
                c.execute(
                    "INSERT INTO batches(sku,date,qty,unit_cost,note) VALUES(?,?,?,?,?)",
                    (sku, e.date, int(e.qty), float(e.unit_cost), e.note)
                )
        return {"status": "ok"}

    @router.delete("/db/price-batches/{sku}/{bid}")
    async def delete_batch(sku: str, bid: int):
        _ensure_schema()
        with _db() as c:
            c.execute("DELETE FROM batches WHERE id=? AND sku=?", (bid, sku))
        return {"status": "ok"}

    # ---------- DB: категории/комиссии ----------
    @router.get("/db/categories")
    async def list_categories():
        _seed_categories_if_empty()
        with _db() as c:
            rows = [dict(r) for r in c.execute("SELECT * FROM categories ORDER BY name")]
        return {"categories": rows}

    @router.post("/db/categories")
    async def save_categories(cats: List[CategoryIn]):
        _ensure_schema()
        with _db() as c:
            for cat in cats:
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
    async def set_product_category(sku: str, payload: Dict[str, Any] = Body(...)):
        _ensure_schema()
        category = (payload.get("category") or "").strip()
        with _db() as c:
            c.execute("UPDATE products SET category=?, updated_at=datetime('now') WHERE sku=?", (category, sku))
        return {"status": "ok", "sku": sku, "category": category}

    # ---------- Бэкап / восстановление БД ----------
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
