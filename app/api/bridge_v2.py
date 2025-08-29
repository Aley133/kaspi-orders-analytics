# app/api/bridge_v2.py
from __future__ import annotations
from typing import List, Dict, Any, Optional, Tuple
from pydantic import BaseModel
from fastapi import APIRouter, Body, HTTPException
import os, sqlite3, httpx

router = APIRouter()

# --- Kaspi ---
KASPI_TOKEN   = os.getenv("KASPI_TOKEN", "").strip()
KASPI_BASEURL = (os.getenv("KASPI_BASE_URL") or "https://kaspi.kz/shop/api/v2").rstrip("/")
def _kaspi_headers():
    if not KASPI_TOKEN:
        raise HTTPException(500, "KASPI_TOKEN is not set")
    return {"X-Auth-Token": KASPI_TOKEN, "Accept":"application/vnd.api+json", "Content-Type":"application/vnd.api+json"}

HTTPX_TIMEOUT = httpx.Timeout(connect=10.0, read=40.0, write=15.0, pool=40.0)
HTTPX_LIMITS  = httpx.Limits(max_connections=30, max_keepalive_connections=10)

# --- DB (sqlite по умолчанию) ---
DB_URL = os.getenv("PROFIT_DB_URL") or os.getenv("DATABASE_URL") or "sqlite:///./profit.db"
def _sqlite_path(u:str)->str: return u.split("sqlite:///")[-1]
def _get_conn():
    if DB_URL.startswith("sqlite"):
        c = sqlite3.connect(_sqlite_path(DB_URL)); c.row_factory = sqlite3.Row; return c
    import psycopg2; return psycopg2.connect(DB_URL)

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
    if not rows: return 0
    _init_bridge_sales()
    with _get_conn() as c:
        cur = c.cursor()
        if DB_URL.startswith("sqlite"):
            cur.executemany("""
            INSERT INTO bridge_sales
              (order_id,line_index,order_code,date_utc_ms,state,sku,title,qty,unit_price,total_price)
            VALUES (:order_id,:line_index,:order_code,:date_utc_ms,:state,:sku,:title,:qty,:unit_price,:total_price)
            ON CONFLICT(order_id,line_index) DO UPDATE SET
              order_code=excluded.order_code, date_utc_ms=excluded.date_utc_ms, state=excluded.state,
              sku=excluded.sku, title=excluded.title, qty=excluded.qty,
              unit_price=excluded.unit_price, total_price=excluded.total_price
            """, rows)
        else:
            cur.executemany("""
            INSERT INTO bridge_sales
              (order_id,line_index,order_code,date_utc_ms,state,sku,title,qty,unit_price,total_price)
            VALUES (%(order_id)s,%(line_index)s,%(order_code)s,%(date_utc_ms)s,%(state)s,%(sku)s,%(title)s,%(qty)s,%(unit_price)s,%(total_price)s)
            ON CONFLICT (order_id,line_index) DO UPDATE SET
              order_code=EXCLUDED.order_code, date_utc_ms=EXCLUDED.date_utc_ms, state=EXCLUDED.state,
              sku=EXCLUDED.sku, title=EXCLUDED.title, qty=EXCLUDED.qty,
              unit_price=EXCLUDED.unit_price, total_price=EXCLUDED.total_price
            """, rows)
        c.commit()
        return cur.rowcount or 0

# --- helpers для извлечения SKU/названия из позиций заказа ---
_SKU_KEYS   = ("merchantProductCode","article","sku","code","productCode","offerId","vendorCode","barcode","skuId","id","merchantProductId")
_TITLE_KEYS = ("productName","name","title","itemName","productTitle","merchantProductName")
def _safe_get(d,k): return (d or {}).get(k) if isinstance(d,dict) else None
def _index_included(inc): 
    idx={}; 
    for it in inc or []:
        t=it.get("type"); i=it.get("id")
        if t and i: idx[(str(t),str(i))]=it
    return idx
def _rel_id(entry,rel):
    data=((entry or {}).get("relationships",{}).get(rel,{})).get("data")
    return (data or {}).get("type"), (data or {}).get("id")

def _extract_entry(entry, inc) -> Optional[Dict[str,Any]]:
    attrs = entry.get("attributes", {}) if "attributes" in (entry or {}) else (entry or {})
    qty = int(attrs.get("quantity") or 1)
    unit_price = float(attrs.get("unitPrice") or attrs.get("basePrice") or attrs.get("price") or 0.0)

    sku=""
    for k in _SKU_KEYS:
        v = attrs.get(k)
        if v is not None and str(v).strip(): sku=str(v).strip(); break
    def from_rel(rel):
        t,i=_rel_id(entry,rel)
        if not (t and i): return None
        a=_safe_get(_safe_get(inc.get((str(t),str(i))),"attributes"),None) or {}
        if "master" in str(t).lower(): return i or a.get("id") or a.get("code") or a.get("sku") or a.get("productCode")
        return a.get("code") or a.get("sku") or a.get("productCode") or i
    if not sku: sku = from_rel("product") or from_rel("merchantProduct") or from_rel("masterProduct") or ""
    _pt,pid=_rel_id(entry,"product"); _mt,mid=_rel_id(entry,"merchantProduct")
    offer_like = attrs.get("offerId") or attrs.get("merchantProductId") or mid
    if (pid or mid) and offer_like and (not sku or str(offer_like) not in sku):
        sku=f"{(pid or mid)}_{offer_like}"
    if unit_price<=0:
        try:
            total=float(attrs.get("totalPrice") or attrs.get("price") or 0)
            unit_price = round(total/max(1,qty), 4) if total else 0.0
        except Exception: pass
    if not sku: return None
    best = (_safe_get(attrs,"offer") or {}).get("code") or sku
    title=None
    titles={}
    for k in _TITLE_KEYS:
        v=_safe_get(attrs,k)
        if isinstance(v,str) and v.strip(): titles[k]=v.strip()
    off=_safe_get(attrs,"offer") or {}
    if isinstance(off,dict) and isinstance(off.get("name"),str): titles["offer.name"]=off["name"]
    for rel in ("product","merchantProduct","masterProduct"):
        t,i=_rel_id(entry,rel)
        if not (t and i): continue
        a=_safe_get(_safe_get(inc.get((str(t),str(i))),"attributes"),None) or {}
        for k in _TITLE_KEYS:
            v=_safe_get(a,k)
            if isinstance(v,str) and v.strip(): titles[f"{rel}.{k}"]=v.strip()
    for key in ("offer.name","name","productName","title","productTitle"):
        if titles.get(key): title=titles[key]; break
    if not title: title=next(iter(titles.values()), "")
    return {"sku":str(best), "title":title, "qty":qty, "unit_price":unit_price, "total_price": unit_price*qty}

async def _fetch_entries(order_id:str)->List[Dict[str,Any]]:
    async with httpx.AsyncClient(base_url=KASPI_BASEURL, timeout=HTTPX_TIMEOUT, limits=HTTPX_LIMITS) as cli:
        r = await cli.get(f"/orders/{order_id}/entries", params={"page[size]":"200","include":"product,merchantProduct,masterProduct"}, headers=_kaspi_headers())
        r.raise_for_status()
        j=r.json(); inc=_index_included(j.get("included",[]))
        out=[]
        for idx, e in enumerate(j.get("data",[]) or []):
            ex=_extract_entry(e, inc)
            if ex: ex["__index"]=idx; out.append(ex)
        return out

def _to_ms(x)->Optional[int]:
    if x is None: return None
    try:
        xi=int(x); return xi if xi>10_000_000_000 else xi*1000
    except Exception:
        from datetime import datetime
        try: return int(datetime.fromisoformat(str(x).replace("Z","+00:00")).timestamp()*1000)
        except Exception: return None

class OrderRef(BaseModel):
    id: str
    date: Optional[Any] = None

@router.post("/bridge/sync-by-ids")
async def bridge_sync_by_ids(items: List[OrderRef] = Body(...)):
    if not items: return {"synced_orders":0,"items_inserted":0}
    ok=0; ins=0
    for ref in items:
        try:
            entries = await _fetch_entries(ref.id)
        except httpx.HTTPError:
            continue
        if not entries: continue
        order_ms=_to_ms(ref.date); order_code=None; order_state=None
        if order_ms is None or order_code is None:
            try:
                async with httpx.AsyncClient(base_url=KASPI_BASEURL, timeout=HTTPX_TIMEOUT, limits=HTTPX_LIMITS) as cli:
                    r=await cli.get(f"/orders/{ref.id}", headers=_kaspi_headers())
                    if r.status_code==200:
                        a=(r.json().get("data") or {}).get("attributes",{}) or {}
                        order_code  = a.get("code") or a.get("orderNumber") or a.get("number")
                        order_state = a.get("state")
                        for df in ("creationDate","approvedDate","signingDate","shipmentDate","deliveryDate","archivedDate"):
                            if order_ms: break
                            order_ms=_to_ms(a.get(df))
            except Exception:
                pass
        rows=[{
            "order_id": ref.id, "line_index": e["__index"], "order_code": order_code,
            "date_utc_ms": order_ms, "state": order_state, "sku": e["sku"],
            "title": e.get("title"), "qty": e["qty"], "unit_price": e["unit_price"], "total_price": e["total_price"],
        } for e in entries]
        ins += _upsert_bridge_rows(rows); ok += 1
    return {"synced_orders": ok, "items_inserted": ins}
