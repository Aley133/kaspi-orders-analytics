from __future__ import annotations
from typing import Optional, List, Dict, Any, Tuple
from fastapi import APIRouter, HTTPException, Query, UploadFile, File, Body
from fastapi.responses import Response, JSONResponse, FileResponse
from pydantic import BaseModel
import io, os, shutil, sqlite3, secrets, string

# ---------- Excel optional ----------
try:
    import openpyxl
    _OPENPYXL_AVAILABLE = True
except Exception:
    _OPENPYXL_AVAILABLE = False

# ---------- DB path ----------
def _resolve_db_path() -> str:
    target = os.getenv("DB_PATH", "/data/kaspi-orders.sqlite3")
    d = os.path.dirname(target)
    try:
        os.makedirs(d, exist_ok=True)
        if os.access(d, os.W_OK):
            return target
    except Exception:
        pass
    # fallback рядом с модулем
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data.sqlite3"))

DB_PATH = _resolve_db_path()

_OLD_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data.sqlite3"))
if DB_PATH != _OLD_PATH and os.path.exists(_OLD_PATH) and not os.path.exists(DB_PATH):
    try:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        shutil.copy2(_OLD_PATH, DB_PATH)
    except Exception:
        pass

# ---------- DB helpers ----------
def _db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn

def _gen_batch_code(conn: sqlite3.Connection) -> str:
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    while True:
        code = "".join(secrets.choice(alphabet) for _ in range(6))
        if not conn.execute("SELECT 1 FROM batches WHERE batch_code=?", (code,)).fetchone():
            return code

def _ensure_schema():
    with _db() as c:
        c.executescript("""
        PRAGMA foreign_keys=ON;
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
        # автозаполнение кодов партий
        for r in c.execute("SELECT id FROM batches WHERE batch_code IS NULL OR batch_code=''").fetchall():
            c.execute("UPDATE batches SET batch_code=? WHERE id=?", (_gen_batch_code(c), r["id"]))

def _seed_categories_if_empty():
    _ensure_schema()
    with _db() as c:
        row = c.execute("SELECT COUNT(*) AS c FROM categories").fetchone()
        if row and int(row["c"]) == 0:
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

def _upsert_products(items: List[Dict[str, Any]]) -> Tuple[int,int]:
    _ensure_schema()
    inserted, updated = 0, 0
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
            cur = c.execute("SELECT 1 FROM products WHERE sku=?", (sku,)).fetchone()
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
            if cur: updated += 1
            else: inserted += 1
    return inserted, updated

def _avg_cost(sku: str) -> Optional[float]:
    _ensure_schema()
    with _db() as c:
        r = c.execute(
            "SELECT SUM(qty*unit_cost) AS tc, SUM(qty) AS tq FROM batches WHERE sku=?",
            (sku,)
        ).fetchone()
        if not r or not r["tq"]:
            return None
        return float(r["tc"]) / float(r["tq"])

def _last_batch(conn: sqlite3.Connection, sku: str) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT id, date, qty, unit_cost, commission_pct, batch_code, note "
        "FROM batches WHERE sku=? ORDER BY date DESC, id DESC LIMIT 1", (sku,)
    ).fetchone()

# ---------- Parsers ----------
from xml.etree import ElementTree as ET
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
                if av in ("yes", "true", "1"): active = True
                elif av in ("no", "false", "0"): active = False
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
            "id": code,
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

# ---------- Pydantic ----------
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

# ---------- Router ----------
def get_products_router(client: Optional[Any] = None) -> APIRouter:
    router = APIRouter(prefix="/products", tags=["products"])

    # Импорт из файла с записью в БД
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

        # нормализуем и пишем в products
        normalized: List[Dict[str, Any]] = []
        for d in raw_rows:
            item = dict(d)
            item.setdefault("code", (item.get("sku") or item.get("vendorCode") or item.get("barcode") or item.get("id") or ""))
            item["code"] = str(item["code"]).strip()
            item.setdefault("id", item["code"])
            item.setdefault("name", item.get("model") or item.get("title") or item.get("Name") or item.get("name") or item["code"])
            item["name"] = str(item["name"]).strip()
            item["qty"] = _to_int(item.get("qty") or item.get("quantity") or item.get("stock"))
            item["price"] = _to_float(item.get("price"))
            item["brand"] = item.get("brand") or item.get("vendor")
            item["barcode"] = item.get("barcode") or item.get("Barcode")
            normalized.append(item)

        ins, upd = _upsert_products(normalized)
        normalized.sort(key=lambda x: (x.get("name") or "").lower())
        return JSONResponse({"count": len(normalized), "inserted": ins, "updated": upd, "items": normalized})

    # Массовый апсерт из таблицы
    @router.post("/db/bulk-upsert")
    async def bulk_upsert(rows: List[Dict[str, Any]] = Body(...)):
        items: List[Dict[str, Any]] = []
        for r in rows:
            items.append({
                "code": _sku_of(r),
                "name": r.get("name"),
                "brand": r.get("brand"),
                "category": r.get("category"),
                "price": _to_float(r.get("price")),
                "qty": _to_int(r.get("qty")),
                "active": int(bool(r.get("active"))),
                "barcode": r.get("barcode"),
            })
        ins, upd = _upsert_products(items)
        return {"status":"ok", "inserted": ins, "updated": upd}

    # Список из БД
    @router.get("/db/list")
    async def db_list(active_only: int = 1, search: str = ""):
        _ensure_schema()
        _seed_categories_if_empty()
        with _db() as c:
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
            prod_rows = [dict(r) for r in c.execute(sql, params)]

            # batch_count и last_margin
            # кэш комиссий по категориям
            cats = {r["name"]: dict(r) for r in c.execute("SELECT * FROM categories")}
            items: List[Dict[str, Any]] = []
            for r in prod_rows:
                sku = r["sku"]
                price = float(r.get("price") or 0)
                qty = int(r.get("quantity") or 0)
                # count
                bc = c.execute("SELECT COUNT(*) AS c FROM batches WHERE sku=?", (sku,)).fetchone()["c"]
                last = _last_batch(c, sku)
                last_margin = None
                if last is not None:
                    commission = last["commission_pct"]
                    if commission is None:
                        cat = r.get("category") or ""
                        comms = cats.get(cat)
                        if comms:
                            commission = float(comms["base_percent"]) + float(comms["extra_percent"]) + float(comms["tax_percent"])
                        else:
                            commission = 0.0
                    last_margin = price - (price * (float(commission)/100.0)) - float(last["unit_cost"])
                items.append({
                    "code": sku, "id": sku, "name": r.get("name"),
                    "brand": r.get("brand"), "category": r.get("category") or "",
                    "qty": qty, "price": price, "active": bool(r.get("active")),
                    "batch_count": int(bc), "last_margin": round(last_margin,2) if last_margin is not None else None
                })

        return {"count": len(items), "items": items}

    # Экспорт CSV из БД
    @router.get("/db/export.csv")
    async def export_db_csv(active_only: int = 1, search: str = ""):
        data = await db_list(active_only, search)  # type: ignore
        items = data["items"]  # type: ignore

        def esc(s: Any) -> str:
            s = "" if s is None else str(s)
            if any(c in s for c in [",", '"', "\n"]): s = '"' + s.replace('"', '""') + '"'
            return s

        header = "code,name,brand,category,qty,price,active,batch_count,last_margin\n"
        body = "".join([",".join(esc(x) for x in [
            r["code"], r["name"], r["brand"], r["category"], r["qty"], r["price"],
            1 if r["active"] else 0, r["batch_count"], r["last_margin"]
        ]) + "\n" for r in items])
        return Response(content=header+body, media_type="text/csv; charset=utf-8",
                        headers={"Content-Disposition": 'attachment; filename="products_db.csv"'})
    # Партии
    @router.get("/db/price-batches/{sku}")
    async def get_batches(sku: str):
        _ensure_schema()
        with _db() as c:
            rows = [dict(r) for r in c.execute(
                "SELECT id, date, qty, unit_cost, commission_pct, batch_code, note "
                "FROM batches WHERE sku=? ORDER BY date, id", (sku,)
            )]
        avgc = _avg_cost(sku)
        return {"batches": rows, "avg_cost": round(avgc,2) if avgc is not None else None}

    @router.post("/db/price-batches/{sku}")
    async def add_batches(sku: str, payload: BatchListIn = Body(...)):
        _ensure_schema()
        with _db() as c:
            if not c.execute("SELECT 1 FROM products WHERE sku=?", (sku,)).fetchone():
                raise HTTPException(404, detail="Товар не найден в БД. Сначала сохраните таблицу.")
            for e in payload.entries:
                code = e.batch_code or _gen_batch_code(c)
                c.execute(
                    "INSERT INTO batches(sku,date,qty,unit_cost,note,commission_pct,batch_code) VALUES(?,?,?,?,?,?,?)",
                    (sku, e.date, int(e.qty), float(e.unit_cost), e.note,
                     float(e.commission_pct) if e.commission_pct is not None else None,
                     code)
                )
        avgc = _avg_cost(sku)
        return {"status":"ok", "avg_cost": round(avgc,2) if avgc is not None else None}

    @router.delete("/db/price-batches/{sku}/{bid}")
    async def delete_batch(sku: str, bid: int):
        _ensure_schema()
        with _db() as c:
            c.execute("DELETE FROM batches WHERE id=? AND sku=?", (bid, sku))
        return {"status":"ok"}

    # Категории
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
        return {"status":"ok"}

    # Бэкап/восстановление
    @router.get("/db/backup.sqlite3")
    async def backup_db():
        _ensure_schema()
        fname = os.path.basename(DB_PATH) or "data.sqlite3"
        return FileResponse(DB_PATH, media_type="application/octet-stream", filename=fname)

    @router.post("/db/restore")
    async def restore_db(file: UploadFile = File(...)):
        content = await file.read()
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        with open(DB_PATH, "wb") as f:
            f.write(content)
        # integrity check
        with _db() as c:
            integ = c.execute("PRAGMA integrity_check").fetchone()[0]
            c.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        return {"status":"ok", "integrity": integ}

    # ----- (опционально) заглушки для /list и /probe если KASPI client отсутствует -----
    @router.get("/list")
    async def list_products_stub(*, active: int = 1, q: Optional[str] = None,
                           page: int = 1, page_size: int = 500):
        # возвращаем содержимое из БД как fallback, чтобы фронт всегда работал
        data = await db_list(active_only=active, search=q or "")
        return JSONResponse({"items": data["items"], "total": data["count"], "note": "Catalog API недоступен: показаны данные из БД."})

    @router.get("/probe")
    async def probe_products():
        return JSONResponse({"attempts": [], "error": "kaspi_client не подключён; используется БД."})

    return router
