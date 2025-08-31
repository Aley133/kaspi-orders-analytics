# app/api/bridge_v2.py
from __future__ import annotations

from typing import List, Dict, Any, Optional, Tuple
from pydantic import BaseModel, Field, ValidationError
from fastapi import APIRouter, Body, HTTPException, Query
import os, sqlite3, httpx, math

router = APIRouter()

# ─────────────────────────────────────────────────────────────────────────────
# Конфигурация
# ─────────────────────────────────────────────────────────────────────────────
BRIDGE_ONLY_STATE = (os.getenv("BRIDGE_ONLY_STATE") or "KASPI_DELIVERY").strip() or None

KASPI_TOKEN    = (os.getenv("KASPI_TOKEN") or "").strip()
KASPI_BASE_URL = (os.getenv("KASPI_BASE_URL") or "https://kaspi.kz/shop/api/v2").rstrip("/")
KASPI_FALLBACK_ENABLED = (os.getenv("BRIDGE_KASPI_FALLBACK", "1").lower() in ("1","true","yes","on"))

HTTPX_TIMEOUT = httpx.Timeout(connect=10.0, read=40.0, write=15.0, pool=40.0)
HTTPX_LIMITS  = httpx.Limits(max_connections=30, max_keepalive_connections=10)

DB_URL = os.getenv("PROFIT_DB_URL") or os.getenv("DATABASE_URL") or "sqlite:///./profit.db"

def _sqlite_path(u: str) -> str:
    return u.split("sqlite:///")[-1]

def _get_conn():
    if DB_URL.startswith("sqlite"):
        c = sqlite3.connect(_sqlite_path(DB_URL))
        c.row_factory = sqlite3.Row
        return c
    import psycopg2
    return psycopg2.connect(DB_URL)

def _driver() -> str:
    return "sqlite" if DB_URL.startswith("sqlite") else "pg"

# ─────────────────────────────────────────────────────────────────────────────
# База: таблица с «мостовыми» продажами (каждая строка = позиция заказа)
# ─────────────────────────────────────────────────────────────────────────────
def _init_bridge_sales():
    with _get_conn() as c:
        cur = c.cursor()
        if _driver() == "sqlite":
            cur.execute("""
            CREATE TABLE IF NOT EXISTS bridge_sales(
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              order_id   TEXT NOT NULL,
              line_index INTEGER NOT NULL,
              order_code TEXT,
              date_utc_ms INTEGER,
              state TEXT,
              sku TEXT,
              title TEXT,
              qty INTEGER NOT NULL,
              unit_price DOUBLE PRECISION NOT NULL,
              total_price DOUBLE PRECISION NOT NULL,
              CONSTRAINT bridge_sales_uniq UNIQUE(order_id, line_index)
            )""")
            cur.execute("CREATE INDEX IF NOT EXISTS ix_bridge_sales_date ON bridge_sales(date_utc_ms)")
            cur.execute("CREATE INDEX IF NOT EXISTS ix_bridge_sales_sku  ON bridge_sales(sku)")
        else:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS bridge_sales(
              id SERIAL PRIMARY KEY,
              order_id   TEXT NOT NULL,
              line_index INTEGER NOT NULL,
              order_code TEXT,
              date_utc_ms BIGINT,
              state TEXT,
              sku TEXT,
              title TEXT,
              qty INTEGER NOT NULL,
              unit_price DOUBLE PRECISION NOT NULL,
              total_price DOUBLE PRECISION NOT NULL,
              CONSTRAINT bridge_sales_uniq UNIQUE(order_id, line_index)
            )""")
            cur.execute("CREATE INDEX IF NOT EXISTS ix_bridge_sales_date ON bridge_sales(date_utc_ms)")
            cur.execute("CREATE INDEX IF NOT EXISTS ix_bridge_sales_sku  ON bridge_sales(sku)")
        c.commit()

def _upsert_rows(rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    _init_bridge_sales()
    total = 0
    with _get_conn() as c:
        cur = c.cursor()
        if _driver() == "sqlite":
            sql = """
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
            """
            for i in range(0, len(rows), 500):
                chunk = rows[i:i+500]
                cur.executemany(sql, chunk)
                total += cur.rowcount or 0
        else:
            sql = """
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
            """
            for i in range(0, len(rows), 500):
                chunk = rows[i:i+500]
                cur.executemany(sql, chunk)
                total += cur.rowcount or 0
        c.commit()
    return int(total)

# ─────────────────────────────────────────────────────────────────────────────
# Хелперы
# ─────────────────────────────────────────────────────────────────────────────
def _to_ms(dt_like) -> Optional[int]:
    if dt_like is None:
        return None
    try:
        if isinstance(dt_like, (int,float)) and dt_like > 10_000_000_000:
            return int(dt_like)  # уже миллисекунды
        if isinstance(dt_like, (int,float)):
            return int(float(dt_like) * 1000.0)  # секунды
        # ISO
        from datetime import datetime, timezone
        return int(datetime.fromisoformat(str(dt_like).replace("Z","+00:00")).timestamp() * 1000)
    except Exception:
        return None

def _safe_float(v, d=0.0):
    try:
        return float(v)
    except Exception:
        return float(d)

def _norm_str(s: Optional[str], maxlen: int = 255) -> Optional[str]:
    if s is None:
        return None
    s = str(s)
    return s[:maxlen] if len(s) > maxlen else s

def _kaspi_headers() -> Dict[str,str]:
    if not KASPI_TOKEN:
        raise HTTPException(status_code=500, detail="KASPI_TOKEN not configured")
    return {
        "X-Auth-Token": KASPI_TOKEN,
        "Accept": "application/vnd.api+json",
        "Content-Type": "application/vnd.api+json",
        "User-Agent": "Mozilla/5.0",
    }

async def _kaspi_entries(order_id: str) -> List[Dict[str,Any]]:
    """Получить все позиции заказа из Kaspi."""
    async with httpx.AsyncClient(base_url=KASPI_BASE_URL, timeout=HTTPX_TIMEOUT, limits=HTTPX_LIMITS) as cli:
        r = await cli.get(f"/orders/{order_id}/entries", params={"page[size]": "200", "include": "product,merchantProduct,masterProduct"}, headers=_kaspi_headers())
        r.raise_for_status()
        j = r.json()
        data   = (j.get("data") or [])
        inc    = { (it["type"], it["id"]): it for it in (j.get("included") or []) }
        out: List[Dict[str,Any]] = []
        for idx, e in enumerate(data):
            attrs = (e.get("attributes") or {})
            qty   = int(attrs.get("quantity") or 1)
            total = _safe_float(attrs.get("totalPrice") or attrs.get("basePrice") or attrs.get("price"), 0.0)
            unit  = _safe_float(attrs.get("unitPrice") or attrs.get("basePrice") or attrs.get("price"), None)
            # sku кандидаты
            sku = None
            for k in ("merchantProduct.code","product.code","offer.code","code","sku"):
                val = None
                if "." in k:
                    a,b = k.split(".",1)
                    val = ((attrs.get(a) or {}) or {}).get(b)
                else:
                    val = attrs.get(k)
                if val:
                    sku = str(val)
                    break
            if not sku:
                # из include
                for rel in ("product","merchantProduct","masterProduct"):
                    rel_data = ((e.get("relationships") or {}).get(rel) or {}).get("data")
                    if isinstance(rel_data, dict):
                        ref = inc.get((rel_data.get("type"), rel_data.get("id"))) or {}
                        code = (ref.get("attributes") or {}).get("code")
                        if code:
                            sku = str(code); break
            title = (attrs.get("offer") or {}).get("name")
            out.append({
                "line_index": idx,
                "sku": sku or "",
                "title": title,
                "qty": qty,
                "unit_price": float(unit) if unit is not None else (float(total)/max(1,qty)),
                "total_price": float(total)
            })
        return out

# ─────────────────────────────────────────────────────────────────────────────
# Pydantic-модели payload
# ─────────────────────────────────────────────────────────────────────────────
class SyncItem(BaseModel):
    id: str = Field(..., description="Kaspi order_id")
    code: Optional[str] = Field(None, description="Номер заказа для логов")
    date: Optional[Any] = Field(None, description="Дата (iso/ms/sec)")
    state: Optional[str] = Field(None, description="Состояние заказа")
    # либо готовые поля:
    sku: Optional[str] = None
    title: Optional[str] = None
    qty: Optional[int] = 1
    unit_price: Optional[float] = None
    total_price: Optional[float] = None
    amount: Optional[float] = Field(None, description="синоним total_price")
    line_index: Optional[int] = Field(None, description="Индекс позиции (если известен)")

# ─────────────────────────────────────────────────────────────────────────────
# Диагностика
# ─────────────────────────────────────────────────────────────────────────────
@router.get("/db/ping")
def db_ping():
    _init_bridge_sales()
    info: Dict[str,Any] = {"ok": True, "driver": _driver()}
    if _driver() == "sqlite":
        info["db_path"] = _sqlite_path(DB_URL)
    return info

# ─────────────────────────────────────────────────────────────────────────────
# Запись в мост: офлайн + fallback в Kaspi
# ─────────────────────────────────────────────────────────────────────────────
@router.post("/bridge/sync-by-ids")
async def sync_by_ids(payload: List[SyncItem] = Body(...)):
    """
    Принимает список заказов/позиций.
    Если пришёл готовый SKU — записывает строку (order_id, line_index) с суммой/ценами.
    Если SKU нет и включён fallback — тянет список позиций из Kaspi.
    Защита от дублей: уникальный ключ (order_id, line_index).
    """
    if not isinstance(payload, list) or not payload:
        return {"synced_orders": 0, "items_inserted": 0, "skipped_no_sku": 0, "skipped_by_state": 0, "fallback_used": False, "errors": []}

    touched_orders = set()
    inserted = 0
    skipped_no_sku = 0
    skipped_by_state = 0
    fallback_used = False
    errors: List[str] = []

    offline_rows: List[Dict[str,Any]] = []
    fallback_map: Dict[str, SyncItem] = {}  # order_id -> last meta (date/state/code)

    for raw in payload:
        try:
            it = SyncItem.model_validate(raw)
        except ValidationError as e:
            errors.append("validate_error")
            continue

        if BRIDGE_ONLY_STATE and it.state and str(it.state).upper() != str(BRIDGE_ONLY_STATE).upper():
            skipped_by_state += 1
            continue

        touched_orders.add(it.id)

        # есть готовая строка (SKU уже известен)
        if it.sku:
            qty = int(it.qty or 1)
            total = it.total_price if it.total_price is not None else (it.amount if it.amount is not None else it.unit_price)
            total = _safe_float(total, 0.0)
            unit = it.unit_price if it.unit_price is not None else (float(total)/max(1,qty))
            offline_rows.append({
                "order_id":   it.id,
                "line_index": int(it.line_index if it.line_index is not None else 0),
                "order_code": _norm_str(it.code, 64),
                "date_utc_ms": _to_ms(it.date),
                "state": _norm_str(it.state, 64),
                "sku": str(it.sku).strip(),
                "title": _norm_str(it.title, 512),
                "qty": qty,
                "unit_price": _safe_float(unit, 0.0),
                "total_price": _safe_float(total, 0.0),
            })
        else:
            # потребуется fallback по всем позициям этого заказа
            if KASPI_FALLBACK_ENABLED and KASPI_TOKEN:
                fallback_map[it.id] = it
            else:
                skipped_no_sku += 1

    # офлайн-строки
    if offline_rows:
        try:
            inserted += _upsert_rows(offline_rows)
        except Exception:
            errors.append("db_upsert_error")

    # fallback к Kaspi — берём все позиции каждого заказа
    if fallback_map:
        fallback_used = True
        async with httpx.AsyncClient(timeout=HTTPX_TIMEOUT, limits=HTTPX_LIMITS) as _:
            for oid, meta in fallback_map.items():
                try:
                    entries = await _kaspi_entries(oid)
                    for e in entries:
                        if not e.get("sku"):
                            continue
                        offline_rows.append({
                            "order_id":   oid,
                            "line_index": int(e["line_index"]),
                            "order_code": _norm_str(meta.code, 64),
                            "date_utc_ms": _to_ms(meta.date),
                            "state": _norm_str(meta.state, 64),
                            "sku": str(e["sku"]),
                            "title": _norm_str(e.get("title"), 512),
                            "qty": int(e.get("qty") or 1),
                            "unit_price": _safe_float(e.get("unit_price"), 0.0),
                            "total_price": _safe_float(e.get("total_price"), 0.0),
                        })
                except httpx.HTTPStatusError as ex:
                    errors.append(f"kaspi_http_{ex.response.status_code}")
                except httpx.HTTPError:
                    errors.append("kaspi_http_error")
                except Exception:
                    errors.append("kaspi_parse_error")
        if offline_rows:
            try:
                inserted += _upsert_rows(offline_rows)
            except Exception:
                errors.append("db_upsert_error")

    return {
        "synced_orders": len(touched_orders),
        "items_inserted": int(inserted),
        "skipped_no_sku": int(skipped_no_sku),
        "skipped_by_state": int(skipped_by_state),
        "fallback_used": bool(fallback_used),
        "errors": errors,
    }

# ─────────────────────────────────────────────────────────────────────────────
# НОВОЕ: Выдача строк моста «номер заказа + SKU»
# ─────────────────────────────────────────────────────────────────────────────
@router.get("/bridge/list")
def bridge_list(
    date_from: Optional[str] = Query(None),
    date_to: Optional[str]   = Query(None),
    order_id: Optional[str]  = Query(None),
    order_code: Optional[str]= Query(None),
    sku: Optional[str]       = Query(None),
    state: Optional[str]     = Query(None),
    limit: int               = Query(1000, ge=1, le=100000),
    offset: int              = Query(0, ge=0),
):
    """
    Построчная выдача содержимого bridge_sales.
    Это «как списывать со склада»: каждая строка = одна позиция заказа (order_code + sku).
    """
    _init_bridge_sales()
    where = []
    params: Dict[str, Any] = {}
    if date_from:
        where.append("date_utc_ms >= :df"); params["df"] = _to_ms(date_from) or 0
    if date_to:
        where.append("date_utc_ms <= :dt"); params["dt"] = _to_ms(date_to) or 9_999_999_999_999
    if order_id:
        where.append("order_id = :oid"); params["oid"] = order_id
    if order_code:
        where.append("order_code = :ocode"); params["ocode"] = order_code
    if sku:
        where.append("sku = :sku"); params["sku"] = sku
    if state:
        where.append("state = :st"); params["st"] = state

    sql = "SELECT order_id,order_code,sku,title,qty,unit_price,total_price,state,date_utc_ms,line_index FROM bridge_sales"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY date_utc_ms ASC, order_id ASC, line_index ASC LIMIT :lim OFFSET :off"
    params["lim"] = int(limit)
    params["off"] = int(offset)

    with _get_conn() as c:
        cur = c.cursor()
        cur.execute(sql, params) if _driver()=="sqlite" else cur.execute(sql.replace(":", "%(").replace(" ", ")s ", 1), params)  # pg/psycopg2 style
        rows = [dict(r) for r in cur.fetchall()]

    return {"rows": rows, "count": len(rows)}

@router.get("/bridge/by-order-sku")
def bridge_by_order_sku(
    date_from: Optional[str] = Query(None),
    date_to: Optional[str]   = Query(None),
    order_id: Optional[str]  = Query(None),
    order_code: Optional[str]= Query(None),
    sku: Optional[str]       = Query(None),
    state: Optional[str]     = Query(None),
    group_by_id: int         = Query(0, description="1 — группировать по order_id+sku вместо order_code+sku"),
    limit: int               = Query(1000, ge=1, le=100000),
    offset: int              = Query(0, ge=0),
):
    """
    Агрегация по паре (order_code, sku) — или (order_id, sku) если group_by_id=1.
    Удобно для сверки со складом и защиты от повторного списания.
    """
    _init_bridge_sales()
    where = []
    params: Dict[str, Any] = {}
    if date_from:
        where.append("date_utc_ms >= :df"); params["df"] = _to_ms(date_from) or 0
    if date_to:
        where.append("date_utc_ms <= :dt"); params["dt"] = _to_ms(date_to) or 9_999_999_999_999
    if order_id:
        where.append("order_id = :oid"); params["oid"] = order_id
    if order_code:
        where.append("order_code = :ocode"); params["ocode"] = order_code
    if sku:
        where.append("sku = :sku"); params["sku"] = sku
    if state:
        where.append("state = :st"); params["st"] = state

    key1 = "order_id" if group_by_id else "order_code"
    sql = f"""
    SELECT {key1} as key, sku,
           SUM(qty)            AS qty,
           SUM(total_price)    AS total,
           MIN(date_utc_ms)    AS first_ms,
           MAX(date_utc_ms)    AS last_ms,
           COUNT(*)            AS lines
      FROM bridge_sales
    """
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += f" GROUP BY {key1}, sku ORDER BY first_ms ASC LIMIT :lim OFFSET :off"
    params["lim"] = int(limit)
    params["off"] = int(offset)

    with _get_conn() as c:
        cur = c.cursor()
        cur.execute(sql, params) if _driver()=="sqlite" else cur.execute(sql.replace(":", "%(").replace(" ", ")s ", 1), params)
        rows = [dict(r) for r in cur.fetchall()]

    return {"rows": rows, "count": len(rows), "group_key": ("order_id" if group_by_id else "order_code")}

@router.get("/bridge/exists")
def bridge_exists(
    order_code: Optional[str] = Query(None),
    sku: Optional[str]        = Query(None),
    order_id: Optional[str]   = Query(None),
    line_index: Optional[int] = Query(None),
):
    """
    Быстрая проверка наличия строки в мосте.
    Варианты:
      • order_id + line_index — точная проверка строки заказа
      • order_code + sku      — проверка по номеру и SKU
    """
    _init_bridge_sales()
    with _get_conn() as c:
        cur = c.cursor()
        if order_id is not None and line_index is not None:
            cur.execute("SELECT 1 FROM bridge_sales WHERE order_id = ? AND line_index = ? LIMIT 1", (order_id, int(line_index))) if _driver()=="sqlite" \
                else cur.execute("SELECT 1 FROM bridge_sales WHERE order_id = %(oid)s AND line_index = %(li)s LIMIT 1", {"oid":order_id, "li":int(line_index)})
        elif order_code and sku:
            cur.execute("SELECT 1 FROM bridge_sales WHERE order_code = ? AND sku = ? LIMIT 1", (order_code, sku)) if _driver()=="sqlite" \
                else cur.execute("SELECT 1 FROM bridge_sales WHERE order_code = %(oc)s AND sku = %(sku)s LIMIT 1", {"oc":order_code, "sku":sku})
        else:
            raise HTTPException(status_code=400, detail="Укажите (order_id & line_index) ИЛИ (order_code & sku)")
        row = cur.fetchone()
    return {"exists": bool(row)}
