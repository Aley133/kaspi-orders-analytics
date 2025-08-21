from __future__ import annotations
import sqlite3, httpx, os, json
from typing import List, Dict, Optional, Tuple
from pathlib import Path
from datetime import datetime
from .inventory import _conn as inv_conn

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "app.db"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

def _conn():
    con = sqlite3.connect(DB_PATH)
    con.execute("PRAGMA journal_mode=WAL;")
    return con

def init_db():
    with _conn() as c:
        c.execute("""
        CREATE TABLE IF NOT EXISTS catalog_products (
            code TEXT PRIMARY KEY,
            name TEXT,
            active INTEGER,
            raw_json TEXT,
            updated_at TEXT NOT NULL
        )
        """)
init_db()

BASE = "https://kaspi.kz/shop/api/v2"
TOKEN = os.getenv("KASPI_TOKEN", "")

def _client():
    return httpx.Client(base_url=BASE, headers={
        "X-Auth-Token": TOKEN,
        "Content-Type": "application/vnd.api+json",
    }, timeout=httpx.Timeout(15.0, connect=10.0))

def _try_get(path: str, params: Dict) -> Optional[Dict]:
    with _client() as cl:
        r = cl.get(path, params=params)
        if r.status_code==200:
            try:
                return r.json()
            except Exception:
                return None
        return None

def _iter_collection(path: str, page_size: int = 100, max_pages: int = 300):
    for page in range(0, max_pages):
        js = _try_get(path, {"page[size]": page_size, "page[number]": page})
        if not js or not js.get("data"):
            break
        yield from js["data"]
        if len(js["data"]) < page_size:
            break

def _extract_code_name_active(it: Dict) -> Tuple[Optional[str], str, int]:
    attr = it.get("attributes", {}) if isinstance(it, dict) else {}
    code = attr.get("code") or it.get("id") or attr.get("merchantProductCode") or attr.get("sku")
    name = attr.get("name") or attr.get("title") or ""
    active = int(bool(attr.get("active") or attr.get("isActive") or attr.get("availableForSale")))
    return code, name, active

def sync_products() -> Dict[str,int]:
    """
    Выгружаем из кабинета товары (активные и неактивные).
    Пробуем /products, затем /merchantProducts — у разных аккаунтов по-разному.
    """
    items = []
    for candidate in ("/products", "/merchantProducts"):
        try:
            items = list(_iter_collection(candidate))
            if items:
                break
        except Exception:
            continue
    saved = 0
    now = datetime.utcnow().isoformat()
    with _conn() as c:
        for it in items:
            code, name, active = _extract_code_name_active(it)
            if not code:
                continue
            c.execute("""
                INSERT INTO catalog_products(code,name,active,raw_json,updated_at)
                VALUES(?,?,?,?,?)
                ON CONFLICT(code) DO UPDATE SET name=excluded.name, active=excluded.active, raw_json=excluded.raw_json, updated_at=excluded.updated_at
            """, (code, name, active, json.dumps(it, ensure_ascii=False), now))
            saved += 1
    return {"saved": saved, "source_count": len(items)}

def list_catalog() -> List[Dict]:
    with _conn() as c:
        rows = c.execute("SELECT code,name,active,updated_at FROM catalog_products").fetchall()
    return [{"code": r[0], "name": r[1], "active": int(r[2]), "updated_at": r[3]} for r in rows]

def overview() -> List[Dict]:
    # сопоставляем: каталог vs склад (приходы) vs продажи
    with _conn() as c:
        cat = {r[0]: {"name": r[1], "active": int(r[2])} for r in c.execute("SELECT code,name,active FROM catalog_products")}
    with inv_conn() as c2:
        inv = {r[0]: int(r[1] or 0) for r in c2.execute("SELECT product_code, SUM(qty_in) FROM inventory_batches GROUP BY product_code")}
        sold = {r[0]: int(r[1] or 0) for r in c2.execute("SELECT product_code, qty_sold FROM inventory_sales_cache")}
        thr  = {r[0]: int(r[1] or 0) for r in c2.execute("SELECT product_code, threshold FROM inventory_thresholds")}
    codes = set(cat.keys()) | set(inv.keys()) | set(sold.keys())
    out = []
    for code in sorted(codes):
        name = (cat.get(code) or {}).get("name", "")
        active = (cat.get(code) or {}).get("active", 0)
        qty_in = inv.get(code, 0)
        qty_sold = sold.get(code, 0)
        qty_left = qty_in - qty_sold
        threshold = thr.get(code, 0)
        out.append({
            "code": code, "name": name, "active": active,
            "present_in_management": int(code in cat),
            "present_in_inventory": int(code in inv),
            "qty_left": qty_left,
            "threshold": threshold,
            "low": int(threshold and qty_left <= threshold)
        })
    return out
