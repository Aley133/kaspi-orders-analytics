# app/api/bridge_v2.py
from __future__ import annotations
from typing import List, Dict, Any, Optional, Tuple, DefaultDict
from collections import defaultdict
from pydantic import BaseModel
from fastapi import APIRouter, Body, HTTPException
import os, sqlite3, httpx

router = APIRouter()

# ─────────────────────────────────────────────────────────────────────────────
# Kaspi API (fallback, если в payload нет SKU)
# ─────────────────────────────────────────────────────────────────────────────
KASPI_TOKEN   = os.getenv("KASPI_TOKEN", "").strip()
KASPI_BASEURL = (os.getenv("KASPI_BASE_URL") or "https://kaspi.kz/shop/api/v2").rstrip("/")

def _kaspi_headers():
    if not KASPI_TOKEN:
        raise HTTPException(500, "KASPI_TOKEN is not set")
    return {
        "X-Auth-Token": KASPI_TOKEN,
        "Accept": "application/vnd.api+json",
        "Content-Type": "application/vnd.api+json",
    }

HTTPX_TIMEOUT = httpx.Timeout(connect=10.0, read=40.0, write=15.0, pool=40.0)
HTTPX_LIMITS  = httpx.Limits(max_connections=30, max_keepalive_connections=10)

# ─────────────────────────────────────────────────────────────────────────────
# DB (sqlite по умолчанию; Postgres — через DATABASE_URL/PROFIT_DB_URL)
# ─────────────────────────────────────────────────────────────────────────────
DB_URL = os.getenv("PROFIT_DB_URL") or os.getenv("DATABASE_URL") or "sqlite:///./profit.db"
def _sqlite_path(u:str)->str: return u.split("sqlite:///")[-1]

def _get_conn():
    if DB_URL.startswith("sqlite"):
        c = sqlite3.connect(_sqlite_path(DB_URL))
        c.row_factory = sqlite3.Row
        return c
    import psycopg2
    return psycopg2.connect(DB_URL)

def _init_bridge_sales():
    with _get_conn() as c:
        cur = c.cursor()
        if DB_URL.startswith("sqlite"):
            cur.execute("""
            CREATE TABLE IF NOT EXISTS bridge_sales(
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              order_id TEXT NOT NULL,
              line_index INTEGER NOT NULL,
              order_code TEXT,
              date_utc_ms INTEGER,
              state TEXT,
              sku TEXT NOT NULL,
              title TEXT,
              qty INTEGER NOT NULL,
              unit_price REAL NOT NULL,
              total_price REAL NOT NULL,
              UNIQUE(order_id, line_index)
            )""")
            cur.execute("CREATE INDEX IF NOT EXISTS ix_bridge_sales_date ON bridge_sales(date_utc_ms)")
            cur.execute("CREATE INDEX IF NOT EXISTS ix_bridge_sales_sku  ON bridge_sales(sku)")
        else:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS bridge_sales(
              id SERIAL PRIMARY KEY,
              order_id TEXT NOT NULL,
              line_index INTEGER NOT NULL,
              order_code TEXT,
              date_utc_ms BIGINT,
              state TEXT,
              sku TEXT NOT NULL,
              title TEXT,
              qty INTEGER NOT NULL,
              unit_price DOUBLE PRECISION NOT NULL,
              total_price DOUBLE PRECISION NOT NULL,
              CONSTRAINT bridge_sales_uniq UNIQUE(order_id, line_index)
            )""")
            cur.execute("CREATE INDEX IF NOT EXISTS ix_bridge_sales_date ON bridge_sales(date_utc_ms)")
            cur.execute("CREATE INDEX IF NOT EXISTS ix_bridge_sales_sku  ON bridge_sales(sku)")
        c.commit()

def _upsert_bridge_rows(rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    _init_bridge_sales()
    with _get_conn() as c:
        cur = c.cursor()
        if DB_URL.startswith("sqlite"):
            cur.executemany("""
            INSERT INTO bridge_sales
              (order_id,line_index,order_code,date_utc_ms,state,sku,title,qty,unit_price,total_price)
            VALUES (:order_id,:line_index,:order_code,:date_utc_ms,:state,:sku,:title,:qty,:unit_price,:total_price)
            ON CONFLICT(order_id,line_index) DO UPDATE SET
              order_code=excluded.order_code,
              date_utc_ms=excluded.date_utc_ms,
              state=excluded.state,
              sku=excluded.sku,
              title=excluded.title,
              qty=excluded.qty,
              unit_price=excluded.unit_price,
              total_price=excluded.total_price
            """, rows)
        else:
            cur.executemany("""
            INSERT INTO bridge_sales
              (order_id,line_index,order_code,date_utc_ms,state,sku,title,qty,unit_price,total_price)
            VALUES (%(order_id)s,%(line_index)s,%(order_code)s,%(date_utc_ms)s,%(state)s,%(sku)s,%(title)s,%(qty)s,%(unit_price)s,%(total_price)s)
            ON CONFLICT (order_id,line_index) DO UPDATE SET
              order_code=EXCLUDED.order_code,
              date_utc_ms=EXCLUDED.date_utc_ms,
              state=EXCLUDED.state,
              sku=EXCLUDED.sku,
              title=EXCLUDED.title,
              qty=EXCLUDED.qty,
              unit_price=EXCLUDED.unit_price,
              total_price=EXCLUDED.total_price
            """, rows)
        c.commit()
        return cur.rowcount or 0

# ─────────────────────────────────────────────────────────────────────────────
# Извлечение позиций заказа (fallback к Kaspi)
# ─────────────────────────────────────────────────────────────────────────────
_SKU_KEYS   = ("merchantProductCode","article","sku","code","productCode",
               "offerId","vendorCode","barcode","skuId","id","merchantProductId")
_TITLE_KEYS = ("productName","name","title","itemName","productTitle","merchantProductName")

def _safe_get(d,k):
    return (d or {}).get(k) if isinstance(d,dict) else None

def _index_included(inc_list):
    idx: Dict[Tuple[str,str], dict] = {}
    for it in inc_list or []:
        t, i = it.get("type"), it.get("id")
        if t and i:
            idx[(str(t), str(i))] = it
    return idx

def _rel_id(entry, rel):
    data = ((entry or {}).get("relationships", {}).get(rel, {}) or {}).get("data")
    if isinstance(data, dict):
        return data.get("type"), data.get("id")
    return None, None

def _extract_entry(entry, inc_index) -> Optional[Dict[str,Any]]:
    attrs = entry.get("attributes", {}) if "attributes" in (entry or {}) else (entry or {})
    qty = int(attrs.get("quantity") or 1)
    unit_price = float(attrs.get("unitPrice") or attrs.get("basePrice") or attrs.get("price") or 0.0)

    # первичный SKU из атрибутов
    sku = ""
    for k in _SKU_KEYS:
        v = attrs.get(k)
        if v is not None and str(v).strip():
            sku = str(v).strip()
            break

    # попытка из связей
    def from_rel(rel):
        t, i = _rel_id(entry, rel)
        if not (t and i): return None
        inc_obj = inc_index.get((str(t), str(i))) or {}
        a = inc_obj.get("attributes", {}) if isinstance(inc_obj, dict) else {}
        if "master" in str(t).lower():
            return i or a.get("id") or a.get("code") or a.get("sku") or a.get("productCode")
        return a.get("code") or a.get("sku") or a.get("productCode") or i

    if not sku:
        sku = from_rel("product") or from_rel("merchantProduct") or from_rel("masterProduct") or ""

    # композит
    _pt, pid = _rel_id(entry, "product")
    _mt, mid = _rel_id(entry, "merchantProduct")
    offer_like = attrs.get("offerId") or attrs.get("merchantProductId") or mid
    if (pid or mid) and offer_like and (not sku or str(offer_like) not in sku):
        sku = f"{(pid or mid)}_{offer_like}"

    # цена
    if unit_price <= 0:
        try:
            total = float(attrs.get("totalPrice") or attrs.get("price") or 0)
            unit_price = round(total / max(1, qty), 4) if total else 0.0
        except Exception:
            pass

    if not sku:
        return None

    # название
    titles: Dict[str,str] = {}
    for k in _TITLE_KEYS:
        v = _safe_get(attrs, k)
        if isinstance(v, str) and v.strip():
            titles[k] = v.strip()
    off = _safe_get(attrs, "offer") or {}
    if isinstance(off, dict) and isinstance(off.get("name"), str):
        titles["offer.name"] = off["name"]

    for rel in ("product","merchantProduct","masterProduct"):
        t, i = _rel_id(entry, rel)
        if not (t and i): continue
        inc_obj = inc_index.get((str(t), str(i))) or {}
        a = inc_obj.get("attributes", {}) if isinstance(inc_obj, dict) else {}
        for k in _TITLE_KEYS:
            v = _safe_get(a, k)
            if isinstance(v, str) and v.strip():
                titles[f"{rel}.{k}"] = v.strip()

    title = ""
    for key in ("offer.name","name","productName","title","productTitle"):
        if titles.get(key):
            title = titles[key]; break
    if not title and titles:
        title = next(iter(titles.values()), "")

    best_sku = (_safe_get(attrs,"offer") or {}).get("code") or sku
    total_price = round(unit_price * qty, 4)
    return {"sku": str(best_sku), "title": title, "qty": qty, "unit_price": unit_price, "total_price": total_price}

async def _fetch_entries(order_id:str)->List[Dict[str,Any]]:
    async with httpx.AsyncClient(base_url=KASPI_BASEURL, timeout=HTTPX_TIMEOUT, limits=HTTPX_LIMITS) as cli:
        r = await cli.get(
            f"/orders/{order_id}/entries",
            params={"page[size]":"200","include":"product,merchantProduct,masterProduct"},
            headers=_kaspi_headers(),
        )
        r.raise_for_status()
        j = r.json()
        inc = _index_included(j.get("included", []))
        out = []
        for idx, e in enumerate(j.get("data", []) or []):
            ex = _extract_entry(e, inc)
            if ex:
                ex["__index"] = idx
                out.append(ex)
        return out

# ─────────────────────────────────────────────────────────────────────────────
# Утилиты
# ─────────────────────────────────────────────────────────────────────────────
def _to_ms(x)->Optional[int]:
    if x is None: return None
    try:
        xi=int(x); return xi if xi>10_000_000_000 else xi*1000
    except Exception:
        from datetime import datetime
        try: return int(datetime.fromisoformat(str(x).replace("Z","+00:00")).timestamp()*1000)
        except Exception: return None

# ─────────────────────────────────────────────────────────────────────────────
# Payload модели
# ─────────────────────────────────────────────────────────────────────────────
class SyncItem(BaseModel):
    id: str
    # необязательные — если есть, не будем ходить в Kaspi
    code: Optional[str] = None
    date: Optional[Any] = None              # iso/ms
    state: Optional[str] = None             # KASPI_DELIVERY и т.п.
    sku: Optional[str] = None
    title: Optional[str] = None
    qty: Optional[int] = None               # по умолчанию 1
    unit_price: Optional[float] = None
    total_price: Optional[float] = None
    amount: Optional[float] = None          # синоним total_price
    line_index: Optional[int] = None        # можно не указывать

# ─────────────────────────────────────────────────────────────────────────────
# Роут: синхронизация продаж в мост (главная ручка)
# ─────────────────────────────────────────────────────────────────────────────
@router.post("/bridge/sync-by-ids")
async def bridge_sync_by_ids(items: List[SyncItem] = Body(...)):
    """
    Принимает список заказов:
      - если item.sku присутствует → НЕ идём в Kaspi, используем поля payload
      - если item.sku нет → fallback: тянем позиции /orders/{id}/entries
    Пишем/обновляем bridge_sales (апсерт).
    """
    if not items:
        return {"synced_orders": 0, "items_inserted": 0}

    inserted = 0
    touched_orders: set[str] = set()

    # локальный счётчик line_index по каждому заказу
    counters: DefaultDict[str, int] = defaultdict(int)

    # сначала обрабатываем «офлайновые» элементы со SKU
    offline_rows: List[Dict[str, Any]] = []
    fallback_ids: List[SyncItem] = []

    for it in items:
        if it.sku and str(it.sku).strip():
            oid = it.id
            touched_orders.add(oid)

            idx = it.line_index if it.line_index is not None else counters[oid]
            counters[oid] = idx + 1  # сдвигаем счётчик

            qty = int(it.qty or 1)
            total = it.total_price if it.total_price is not None else (it.amount if it.amount is not None else None)
            if total is None and it.unit_price is not None:
                total = float(it.unit_price) * qty
            if total is None:
                # на худой случай — 0
                total = 0.0
            unit = it.unit_price if it.unit_price is not None else (float(total) / max(1, qty))

            offline_rows.append({
                "order_id": oid,
                "line_index": int(idx),
                "order_code": (it.code or None),
                "date_utc_ms": _to_ms(it.date),
                "state": (it.state or "KASPI_DELIVERY"),
                "sku": str(it.sku).strip(),
                "title": (it.title or None),
                "qty": qty,
                "unit_price": float(unit or 0.0),
                "total_price": float(total or 0.0),
            })
        else:
            fallback_ids.append(it)

    inserted += _upsert_bridge_rows(offline_rows)

    # затем — fallback по тем, где не было SKU
    for ref in fallback_ids:
        try:
            entries = await _fetch_entries(ref.id)
        except httpx.HTTPError:
            # пропускаем таймауты/ошибки сети
            continue
        if not entries:
            continue

        # код/дата/статус возьмём из payload, а если их нет — попробуем подтянуть «шапку»
        order_ms = _to_ms(ref.date)
        order_code = ref.code
        order_state = ref.state

        if (order_ms is None) or (order_code is None) or (order_state is None):
            try:
                async with httpx.AsyncClient(base_url=KASPI_BASEURL, timeout=HTTPX_TIMEOUT, limits=HTTPX_LIMITS) as cli:
                    r = await cli.get(f"/orders/{ref.id}", headers=_kaspi_headers())
                    if r.status_code == 200:
                        a = (r.json().get("data") or {}).get("attributes", {}) or {}
                        order_code  = order_code  or a.get("code") or a.get("orderNumber") or a.get("number")
                        order_state = order_state or a.get("state")
                        if order_ms is None:
                            for df in ("creationDate","approvedDate","signingDate","shipmentDate","deliveryDate","archivedDate"):
                                v = a.get(df)
                                if v:
                                    order_ms = _to_ms(v)
                                    if order_ms: break
            except Exception:
                pass

        rows = []
        for e in entries:
            idx = e.get("__index")
            if idx is None:
                idx = counters[ref.id]
                counters[ref.id] = idx + 1
            rows.append({
                "order_id": ref.id,
                "line_index": int(idx),
                "order_code": order_code,
                "date_utc_ms": order_ms,
                "state": order_state,
                "sku": e["sku"],
                "title": e.get("title"),
                "qty": int(e["qty"]),
                "unit_price": float(e["unit_price"]),
                "total_price": float(e["total_price"]),
            })

        if rows:
            inserted += _upsert_bridge_rows(rows)
            touched_orders.add(ref.id)

    return {"synced_orders": len(touched_orders), "items_inserted": inserted}
