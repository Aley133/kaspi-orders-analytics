# app/api/bridge_v2.py
from __future__ import annotations

from typing import List, Dict, Any, Optional, Tuple, DefaultDict
from collections import defaultdict
from pydantic import BaseModel, Field, ValidationError
from fastapi import APIRouter, Body, HTTPException
import os, sqlite3, math, httpx

router = APIRouter()

# ─────────────────────────────────────────────────────────────────────────────
# Конфигурация
# ─────────────────────────────────────────────────────────────────────────────
# Считаем только этот статус (по умолчанию — KASPI_DELIVERY). Пусто => не фильтровать.
BRIDGE_ONLY_STATE = (os.getenv("BRIDGE_ONLY_STATE") or "KASPI_DELIVERY").strip() or None

# Fallback в Kaspi, если в payload нет SKU. По умолчанию ВЫКЛЮЧЕН, чтобы не ловить таймауты.
KASPI_FALLBACK_ENABLED = (os.getenv("KASPI_FALLBACK_ENABLED", "0").lower() in ("1", "true", "yes"))

# База Kaspi API (только если fallback включён и есть токен)
KASPI_TOKEN   = os.getenv("KASPI_TOKEN", "").strip()
KASPI_BASEURL = (os.getenv("KASPI_BASE_URL") or "https://kaspi.kz/shop/api/v2").rstrip("/")

def _kaspi_headers() -> Dict[str, str]:
    if not KASPI_TOKEN:
        raise HTTPException(500, "KASPI_TOKEN is not set")
    return {
        "X-Auth-Token": KASPI_TOKEN,
        "Accept": "application/vnd.api+json",
        "Content-Type": "application/vnd.api+json",
    }

HTTPX_TIMEOUT = httpx.Timeout(connect=10.0, read=40.0, write=15.0, pool=40.0)
HTTPX_LIMITS  = httpx.Limits(max_connections=30, max_keepalive_connections=10)

# Куда шлём FIFO-списания (router из products.py)
PRODUCTS_BASE_URL = (os.getenv("PRODUCTS_BASE_URL") or "http://localhost:8000/products").rstrip("/")
# API-ключ для products.require_api_key
PRODUCTS_API_KEY = os.getenv("PRODUCTS_API_KEY") or os.getenv("API_KEY")

# ─────────────────────────────────────────────────────────────────────────────
# База данных
# ─────────────────────────────────────────────────────────────────────────────
DB_URL = os.getenv("PROFIT_DB_URL") or os.getenv("DATABASE_URL") or "sqlite:///./profit.db"
def _sqlite_path(u: str) -> str: return u.split("sqlite:///")[-1]

def _get_conn():
    if DB_URL.startswith("sqlite"):
        c = sqlite3.connect(_sqlite_path(DB_URL))
        c.row_factory = sqlite3.Row
        return c
    import psycopg2
    return psycopg2.connect(DB_URL)

def _driver_name() -> str:
    return "sqlite" if DB_URL.startswith("sqlite") else "pg"

def _init_bridge_sales() -> None:
    with _get_conn() as c:
        cur = c.cursor()
        if _driver_name() == "sqlite":
            cur.execute("""
            CREATE TABLE IF NOT EXISTS bridge_sales(
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              order_id   TEXT NOT NULL,
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
              order_id   TEXT NOT NULL,
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

def _chunked(iterable: List[Dict[str, Any]], size: int = 500):
    for i in range(0, len(iterable), size):
        yield iterable[i:i+size]

def _upsert_bridge_rows(rows: List[Dict[str, Any]]) -> int:
    """Пишем батчами, чтобы не ронять соединение большими executemany."""
    if not rows:
        return 0
    _init_bridge_sales()
    total = 0
    with _get_conn() as c:
        cur = c.cursor()
        if _driver_name() == "sqlite":
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
            for chunk in _chunked(rows):
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
            for chunk in _chunked(rows):
                cur.executemany(sql, chunk)
                total += cur.rowcount or 0
        c.commit()
    return int(total or 0)

# ─────────────────────────────────────────────────────────────────────────────
# Утилиты
# ─────────────────────────────────────────────────────────────────────────────
def _to_ms(x) -> Optional[int]:
    """Любую дату (ms / sec / iso) → UTC ms."""
    if x is None:
        return None
    try:
        xi = int(x)
        # если это похоже на секунды — домножим
        return xi if xi > 10_000_000_000 else xi * 1000
    except Exception:
        from datetime import datetime
        try:
            return int(datetime.fromisoformat(str(x).replace("Z", "+00:00")).timestamp() * 1000)
        except Exception:
            return None

def _norm_str(s: Optional[str], maxlen: int = 512) -> Optional[str]:
    if not s:
        return None
    s = str(s)
    if len(s) > maxlen:
        s = s[:maxlen]
    return s

def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default

# ─────────────────────────────────────────────────────────────────────────────
# Fallback в Kaspi (включается переменной окружения)
# ─────────────────────────────────────────────────────────────────────────────
_SKU_KEYS   = ("merchantProductCode","article","sku","code","productCode",
               "offerId","vendorCode","barcode","skuId","id","merchantProductId")
_TITLE_KEYS = ("productName","name","title","itemName","productTitle","merchantProductName")

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
    unit_price = _safe_float(attrs.get("unitPrice") or attrs.get("basePrice") or attrs.get("price"), 0.0)

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
        total = _safe_float(attrs.get("totalPrice") or attrs.get("price"), 0.0)
        unit_price = round(total / max(1, qty), 4) if total else 0.0

    if not sku:
        return None

    # название
    titles: Dict[str,str] = {}
    for k in _TITLE_KEYS:
        v = attrs.get(k)
        if isinstance(v, str) and v.strip():
            titles[k] = v.strip()
    off = attrs.get("offer") or {}
    if isinstance(off, dict) and isinstance(off.get("name"), str):
        titles["offer.name"] = off["name"]

    for rel in ("product","merchantProduct","masterProduct"):
        t, i = _rel_id(entry, rel)
        if not (t and i): continue
        inc_obj = inc_index.get((str(t), str(i))) or {}
        a = inc_obj.get("attributes", {}) if isinstance(inc_obj, dict) else {}
        for k in _TITLE_KEYS:
            v = a.get(k)
            if isinstance(v, str) and v.strip():
                titles[f"{rel}.{k}"] = v.strip()

    title = ""
    for key in ("offer.name","name","productName","title","productTitle"):
        if titles.get(key):
            title = titles[key]; break
    if not title and titles:
        title = next(iter(titles.values()), "")
    best_sku = (attrs.get("offer") or {}).get("code") or sku
    total_price = round(unit_price * qty, 4)
    return {
        "sku": str(best_sku),
        "title": title,
        "qty": qty,
        "unit_price": unit_price,
        "total_price": total_price
    }

async def _fetch_entries_fallback(order_id: str) -> List[Dict[str, Any]]:
    """Ходим в Kaspi только если KASPI_FALLBACK_ENABLED и есть токен."""
    if not KASPI_FALLBACK_ENABLED or not KASPI_TOKEN:
        return []
    async with httpx.AsyncClient(base_url=KASPI_BASEURL, timeout=HTTPX_TIMEOUT, limits=HTTPX_LIMITS) as cli:
        r = await cli.get(
            f"/orders/{order_id}/entries",
            params={"page[size]": "200", "include": "product,merchantProduct,masterProduct"},
            headers=_kaspi_headers(),
        )
        r.raise_for_status()
        j = r.json()
        inc = _index_included(j.get("included", []))
        out: List[Dict[str, Any]] = []
        for idx, e in enumerate(j.get("data", []) or []):
            ex = _extract_entry(e, inc)
            if ex:
                ex["__index"] = idx
                out.append(ex)
        return out

# ─────────────────────────────────────────────────────────────────────────────
# Модели payload
# ─────────────────────────────────────────────────────────────────────────────
class SyncItem(BaseModel):
    id: str = Field(..., description="Kaspi order_id (обязателен)")
    code: Optional[str] = Field(None, description="Номер заказа (для логов)")
    date: Optional[Any] = Field(None, description="Дата (iso/ms/sec)")
    state: Optional[str] = Field(None, description="Состояние заказа")
    sku: Optional[str] = Field(None, description="Готовый SKU/offer.code — если есть, в Kaspi не ходим")
    title: Optional[str] = None
    qty: Optional[int] = 1
    unit_price: Optional[float] = None
    total_price: Optional[float] = None
    amount: Optional[float] = Field(None, description="Синоним total_price")
    line_index: Optional[int] = Field(None, description="Индекс позиции (если известен)")

# ─────────────────────────────────────────────────────────────────────────────
# Пинги/диагностика
# ─────────────────────────────────────────────────────────────────────────────
@router.get("/db/ping")
def db_ping():
    """Для UI: показывает, что база доступна, и тип драйвера."""
    info: Dict[str, Any] = {"ok": True, "driver": _driver_name()}
    if _driver_name() == "sqlite":
        info["db_path"] = _sqlite_path(DB_URL)
    else:
        info["db_path"] = DB_URL
    info["fallback_enabled"] = bool(KASPI_FALLBACK_ENABLED)
    info["only_state"] = BRIDGE_ONLY_STATE
    return info

# ─────────────────────────────────────────────────────────────────────────────
# Хелпер: отправка батчевого FIFO-списания на products
# ─────────────────────────────────────────────────────────────────────────────
async def _send_fifo_writeoffs_bulk(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Отправляет массив {order_id, sku, qty, note?, ts_ms?} в /products/db/writeoff/by-order/bulk.
    Возвращает JSON-ответ ручки products.
    """
    if not rows:
        return {"results": []}
    url = f"{PRODUCTS_BASE_URL}/db/writeoff/by-order/bulk"
    headers = {"Content-Type": "application/json"}
    if PRODUCTS_API_KEY:
        headers["X-API-Key"] = PRODUCTS_API_KEY

    async with httpx.AsyncClient(timeout=HTTPX_TIMEOUT, limits=HTTPX_LIMITS) as cli:
        r = await cli.post(url, json={"rows": rows}, headers=headers)
        r.raise_for_status()
        return r.json()

# ─────────────────────────────────────────────────────────────────────────────
# Главная ручка: офлайновая синхронизация с опциональным fallback + FIFO
# ─────────────────────────────────────────────────────────────────────────────
@router.post("/bridge/sync-by-ids")
async def bridge_sync_by_ids(items: List[SyncItem] = Body(...)):
    """
    Принимает список заказов/позиций.
    Логика:
      1) Если item.sku есть — используем офлайн-путь (без сетевых запросов).
      2) Если sku нет — пробуем fallback в Kaspi (если включён и есть токен).
    Всегда пишем в таблицу bridge_sales (UPSERT).
    Фильтрация по состоянию: BRIDGE_ONLY_STATE (по умолчанию KASPI_DELIVERY).
    Дополнительно: агрегируем (order_id, sku) → qty и отправляем единый батч FIFO-списания
    на сервис products: POST {PRODUCTS_BASE_URL}/db/writeoff/by-order/bulk.
    """
    if not items:
        return {
            "synced_orders": 0, "items_inserted": 0,
            "skipped_no_sku": 0, "skipped_by_state": 0,
            "fallback_used": 0,
            "writeoff_sent": 0, "writeoff_ok": 0, "writeoff_skipped": 0, "writeoff_failed": 0,
            "errors": []
        }

    inserted = 0
    touched_orders: set[str] = set()
    skipped_no_sku = 0
    skipped_by_state = 0
    fallback_used = 0
    errors: List[str] = []

    # локальный счётчик line_index по каждому заказу (если не передан)
    counters: DefaultDict[str, int] = defaultdict(int)

    # агрегатор для FIFO (order_id, sku) → {order_id, sku, qty, ts_ms?, note?}
    to_fifo_map: DefaultDict[Tuple[str, str], Dict[str, Any]] = defaultdict(dict)

    # 1) офлайновые записи (со SKU)
    offline_rows: List[Dict[str, Any]] = []
    # 2) кто без SKU — попробуем fallback
    fallback_refs: List[SyncItem] = []

    for it in items:
        try:
            # фильтрация по состоянию (если требуемый статус задан)
            if BRIDGE_ONLY_STATE and it.state and it.state != BRIDGE_ONLY_STATE:
                skipped_by_state += 1
                continue

            # Готовый SKU → офлайн
            if it.sku and str(it.sku).strip():
                oid = it.id
                touched_orders.add(oid)

                idx = it.line_index if it.line_index is not None else counters[oid]
                counters[oid] = int(idx) + 1

                qty = int(it.qty or 1)
                # total_price предпочитаем из total/amount; если нет — из unit*qty; дальше дефолты
                total = it.total_price if it.total_price is not None else (it.amount if it.amount is not None else None)
                if total is None and it.unit_price is not None:
                    total = _safe_float(it.unit_price) * qty
                if total is None:
                    total = 0.0
                unit = it.unit_price if it.unit_price is not None else (float(total) / max(1, qty))

                offline_rows.append({
                    "order_id":   oid,
                    "line_index": int(idx),
                    "order_code": _norm_str(it.code, 64),
                    "date_utc_ms": _to_ms(it.date),
                    "state": _norm_str(it.state, 64),
                    "sku": str(it.sku).strip(),
                    "title": _norm_str(it.title, 512),
                    "qty": qty,
                    "unit_price": _safe_float(unit, 0.0),
                    "total_price": _safe_float(total, 0.0),
                })

                # ——— агрегируем списание по (order_id, sku)
                _oid = oid
                _sku = str(it.sku).strip()
                _qty = qty
                _ts = _to_ms(it.date)

                note_bits = []
                if it.code: note_bits.append(f"#{it.code}")
                if it.unit_price is not None: note_bits.append(f"unit={_safe_float(it.unit_price):g}")
                if it.total_price is not None: note_bits.append(f"total={_safe_float(it.total_price):g}")
                _note = " ".join(note_bits) or None

                key = (_oid, _sku)
                acc = to_fifo_map.get(key)
                if not acc:
                    acc = {"order_id": _oid, "sku": _sku, "qty": 0}
                    if _ts: acc["ts_ms"] = _ts
                    if _note: acc["note"] = _note
                acc["qty"] = int(acc.get("qty", 0)) + _qty
                if _ts and not acc.get("ts_ms"):
                    acc["ts_ms"] = _ts
                to_fifo_map[key] = acc

            else:
                # без SKU — пробуем fallback (если включён)
                if KASPI_FALLBACK_ENABLED and KASPI_TOKEN:
                    fallback_refs.append(it)
                else:
                    skipped_no_sku += 1

        except Exception as e:
            errors.append(f"payload_item_error:{type(e).__name__}")
            continue

    # Записываем офлайновые
    try:
        inserted += _upsert_bridge_rows(offline_rows)
    except Exception as e:
        errors.append(f"upsert_offline_error:{type(e).__name__}")

    # Fallback: запрос к Kaspi только для тех, у кого не было SKU
    for ref in fallback_refs:
        try:
            if BRIDGE_ONLY_STATE and ref.state and ref.state != BRIDGE_ONLY_STATE:
                skipped_by_state += 1
                continue

            entries = await _fetch_entries_fallback(ref.id)
            if not entries:
                skipped_no_sku += 1
                continue

            fallback_used += 1
            # код/дата/статус берём из payload, если есть
            order_ms = _to_ms(ref.date)
            order_code = _norm_str(ref.code, 64)
            order_state = _norm_str(ref.state, 64)

            # если пусто — не ломаемся: вставим пустые/None, FIFO всё равно по SKU
            rows: List[Dict[str, Any]] = []
            for e in entries:
                idx = e.get("__index")
                if idx is None:
                    idx = counters[ref.id]
                    counters[ref.id] = int(idx) + 1
                rows.append({
                    "order_id": ref.id,
                    "line_index": int(idx),
                    "order_code": order_code,
                    "date_utc_ms": order_ms,
                    "state": order_state,
                    "sku": e["sku"],
                    "title": _norm_str(e.get("title"), 512),
                    "qty": int(e["qty"]),
                    "unit_price": _safe_float(e["unit_price"], 0.0),
                    "total_price": _safe_float(e["total_price"], 0.0),
                })

                # ——— для каждой fallback-строки также добавляем в to_fifo_map
                _sku = str(e["sku"]).strip()
                _qty = int(e["qty"])
                key = (ref.id, _sku)

                note_bits = []
                if order_code: note_bits.append(f"#{order_code}")
                if e.get("unit_price") is not None: note_bits.append(f"unit={_safe_float(e.get('unit_price')):g}")
                if e.get("total_price") is not None: note_bits.append(f"total={_safe_float(e.get('total_price')):g}")
                _note = " ".join(note_bits) or None

                acc = to_fifo_map.get(key)
                if not acc:
                    acc = {"order_id": ref.id, "sku": _sku, "qty": 0}
                    if order_ms: acc["ts_ms"] = order_ms
                    if _note:    acc["note"]  = _note
                acc["qty"] = int(acc.get("qty", 0)) + _qty
                if order_ms and not acc.get("ts_ms"):
                    acc["ts_ms"] = order_ms
                to_fifo_map[key] = acc

            if rows:
                inserted += _upsert_bridge_rows(rows)
                touched_orders.add(ref.id)

        except httpx.TimeoutException:
            errors.append("kaspi_timeout")
        except httpx.HTTPStatusError as e:
            errors.append(f"kaspi_http_{e.response.status_code}")
        except httpx.HTTPError as e:
            errors.append(f"kaspi_http_{type(e).__name__}")
        except Exception as e:
            errors.append(f"fallback_error:{type(e).__name__}")

    # ——— отправляем списание FIFO по заказам (bulk)
    fifo_payload = list(to_fifo_map.values())
    writeoff_result = {"results": []}
    writeoff_ok = writeoff_skipped = writeoff_failed = 0
    try:
        writeoff_result = await _send_fifo_writeoffs_bulk(fifo_payload)
        for r in (writeoff_result.get("results") or []):
            if r.get("ok") and r.get("skipped"):
                writeoff_skipped += 1
            elif r.get("ok"):
                writeoff_ok += 1
            else:
                writeoff_failed += 1
    except httpx.HTTPError as e:
        errors.append(f"fifo_http_{type(e).__name__}")
    except Exception as e:
        errors.append(f"fifo_error:{type(e).__name__}")

    return {
        "synced_orders": len(touched_orders),
        "items_inserted": inserted,
        "skipped_no_sku": skipped_no_sku,
        "skipped_by_state": skipped_by_state,
        "fallback_used": fallback_used,
        "writeoff_sent": len(fifo_payload),
        "writeoff_ok": writeoff_ok,
        "writeoff_skipped": writeoff_skipped,  # уже были списаны (идемпотентность)
        "writeoff_failed": writeoff_failed,
        "errors": errors,
    }
