# app/api/profit_bridge.py
from __future__ import annotations
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone
import os, sqlite3

# Берём ровно ту же конфигурацию, что и в profit_fifo.py
DEFAULT_TZ = os.getenv("TZ", "Asia/Almaty")
try:
    from sqlalchemy import create_engine, text
    _SQLA_OK = True
except Exception:
    _SQLA_OK = False

DATABASE_URL = os.getenv("DATABASE_URL")
_USE_PG = bool(DATABASE_URL and _SQLA_OK)
if _USE_PG:
    _engine = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)

def _resolve_db_path() -> str:
    target = os.getenv("DB_PATH", "/data/kaspi-orders.sqlite3")
    os.makedirs(os.path.dirname(target), exist_ok=True)
    return target

DB_PATH = _resolve_db_path()

from contextlib import contextmanager
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

def _q(sql: str): return text(sql) if _USE_PG else sql
def _rows(rows): return [dict(r._mapping) for r in rows] if _USE_PG else [dict(r) for r in rows]

# Протект как в products/profit
def require_api_key(req: Request) -> bool:
    key = os.getenv("API_KEY")
    if not key:
        return True
    sent = req.headers.get("X-API-Key") or req.query_params.get("api_key")
    if sent != key:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return True

# Убедимся, что нужные таблицы есть (та же схема, что в profit_fifo)
def _ensure_schema():
    with _db() as c:
        if _USE_PG:
            c.execute(_q("""CREATE TABLE IF NOT EXISTS orders(
                id TEXT PRIMARY KEY, date TIMESTAMP NOT NULL, customer TEXT)"""))
            c.execute(_q("""CREATE TABLE IF NOT EXISTS order_items(
                id SERIAL PRIMARY KEY,
                order_id TEXT NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
                sku TEXT NOT NULL, qty INTEGER NOT NULL,
                unit_price DOUBLE PRECISION NOT NULL,
                commission_pct DOUBLE PRECISION)"""))
            c.execute(_q("CREATE INDEX IF NOT EXISTS idx_order_items_sku ON order_items(sku)"))
            c.execute(_q("CREATE INDEX IF NOT EXISTS idx_orders_date ON orders(date)"))
        else:
            c.executescript("""
            CREATE TABLE IF NOT EXISTS orders(
                id TEXT PRIMARY KEY, date TEXT NOT NULL, customer TEXT);
            CREATE TABLE IF NOT EXISTS order_items(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id TEXT NOT NULL,
                sku TEXT NOT NULL, qty INTEGER NOT NULL,
                unit_price REAL NOT NULL,
                commission_pct REAL,
                FOREIGN KEY(order_id) REFERENCES orders(id) ON DELETE CASCADE);
            CREATE INDEX IF NOT EXISTS idx_order_items_sku ON order_items(sku);
            CREATE INDEX IF NOT EXISTS idx_orders_date ON orders(date);
            """)

# ==== Доступ к твоему клиенту Kaspi из main.py ====
# В main.py уже есть объект client (KaspiClient). Мы аккуратно импортируем его фабрику.
# ВНИМАНИЕ: импорт ниже не тащит FastAPI-приложение, только ссылку на client/session.
try:
    # путь может отличаться; поправь, если модуль в другом месте
    from app.main import client  # type: ignore
except Exception:
    client = None  # чтобы было явное сообщение об ошибке

# Универсальный «нормализатор» SKU/кол-ва/цены из ответа по заказу
def _extract_items_from_order(o: Dict[str, Any]) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []

    # Популярные варианты полей в ответах Kaspi (на разных эндпоинтах структура слегка отличается)
    # 1) o["entries"] / o["basket"]["entries"]
    candidate_lists = []
    if isinstance(o.get("entries"), list): candidate_lists.append(o["entries"])
    if isinstance(o.get("basket"), dict) and isinstance(o["basket"].get("entries"), list):
        candidate_lists.append(o["basket"]["entries"])
    if isinstance(o.get("positions"), list): candidate_lists.append(o["positions"])

    for arr in candidate_lists:
        for it in arr:
            sku = (
                it.get("sku")
                or (it.get("offer") or {}).get("id")
                or (it.get("merchantProduct") or {}).get("code")
                or it.get("article")
                or it.get("code")
            )
            qty = it.get("quantity") or it.get("qty") or it.get("count") or 1
            price = (
                it.get("price")
                or it.get("unitPrice")
                or it.get("unit_price")
                or it.get("totalPrice")  # на всякий случай
            )
            # На некоторых ответах цена приходит общей суммой — тогда делим на qty
            try:
                q = int(qty) if qty is not None else 1
                p = float(price) if price is not None else 0.0
                if q > 1 and it.get("unitPrice") is None and it.get("unit_price") is None and it.get("price") is None:
                    p = p / q
            except Exception:
                q = 1; p = 0.0

            if not sku:
                # без SKU позиция нам бесполезна
                continue

            items.append({
                "sku": str(sku),
                "qty": int(q),
                "unit_price": float(p),
                "commission_pct": None,  # можно проставить позже из категорий
            })

    return items

# Получение детальной информации по одному заказу из Kaspi
async def _fetch_order_with_items(order_id: str) -> Tuple[datetime, List[Dict[str, Any]], Optional[str]]:
    if client is None:
        raise HTTPException(500, detail="Kaspi client is not available for FIFO sync. Import failed.")
    # У клиента в main.py уже есть запросы к /shop/api/v2.
    # Практика показывает, что работает GET /shop/api/v2/orders/{id}
    data = await client.get_json(f"/shop/api/v2/orders/{order_id}")  # <-- если у тебя другой метод, поменяй здесь
    # Дата (лучше брать creationDate/approvedDate в зависимости от выбранного поля; здесь используем creationDate)
    dt_raw = data.get("creationDate") or data.get("approvedDate") or data.get("date")
    if isinstance(dt_raw, str):
        # гарантируем timezone-aware
        try:
            dt = datetime.fromisoformat(dt_raw.replace("Z", "+00:00"))
        except Exception:
            dt = datetime.now(timezone.utc)
    else:
        dt = datetime.now(timezone.utc)

    items = _extract_items_from_order(data)
    customer = (data.get("customer") or {}).get("name")
    return dt, items, customer

# Вспомогательная вставка в БД FIFO (idempotent)
def _upsert_orders_bulk(orders: List[Dict[str, Any]]) -> Dict[str, int]:
    _ensure_schema()
    ins_o = upd_o = ins_i = 0
    with _db() as c:
        for o in orders:
            oid = o["id"]
            if _USE_PG:
                existed = c.execute(_q("SELECT 1 FROM orders WHERE id=:id"), {"id": oid}).first()
                c.execute(_q("""
                    INSERT INTO orders(id,date,customer)
                    VALUES(:id,:date,:customer)
                    ON CONFLICT (id) DO UPDATE SET date=EXCLUDED.date, customer=EXCLUDED.customer
                """), {"id": oid, "date": o["date"], "customer": o.get("customer")})
            else:
                existed = c.execute("SELECT 1 FROM orders WHERE id=?", (oid,)).fetchone()
                c.execute("""
                    INSERT INTO orders(id,date,customer) VALUES(?,?,?)
                    ON CONFLICT(id) DO UPDATE SET date=excluded.date, customer=excluded.customer
                """, (oid, o["date"], o.get("customer")))
            upd_o += 1 if existed else 0
            ins_o += 0 if existed else 1

            # пересобираем позиции
            if _USE_PG:
                c.execute(_q("DELETE FROM order_items WHERE order_id=:id"), {"id": oid})
                for it in o["items"]:
                    c.execute(_q("""
                        INSERT INTO order_items(order_id,sku,qty,unit_price,commission_pct)
                        VALUES(:oid,:sku,:qty,:p,:comm)
                    """), {"oid": oid, "sku": it["sku"], "qty": int(it["qty"]),
                           "p": float(it["unit_price"]),
                           "comm": float(it["commission_pct"]) if it.get("commission_pct") is not None else None})
                    ins_i += 1
            else:
                c.execute("DELETE FROM order_items WHERE order_id=?", (oid,))
                for it in o["items"]:
                    c.execute("""
                        INSERT INTO order_items(order_id,sku,qty,unit_price,commission_pct)
                        VALUES(?,?,?,?,?)
                    """, (oid, it["sku"], int(it["qty"]), float(it["unit_price"]),
                          float(it["commission_pct"]) if it.get("commission_pct") is not None else None))
                    ins_i += 1
    return {"orders_inserted": ins_o, "orders_updated": upd_o, "items_inserted": ins_i}

def get_profit_bridge_router() -> APIRouter:
    r = APIRouter(tags=["profit-bridge"])

    @r.post("/sync", summary="Синхронизировать заказы (с позициями) в БД FIFO")
    async def sync_fifo(
        request: Request,
        ids: Optional[List[str]] = Query(None, description="Список конкретных orderId; если не заданы — берём по периоду"),
        start: Optional[str] = Query(None, description="YYYY-MM-DD"),
        end: Optional[str]   = Query(None, description="YYYY-MM-DD"),
        tz: str = Query(DEFAULT_TZ),
        date_field: str = Query("creationDate"),
        states: Optional[str] = Query(None, description="перечень статусов через запятую"),
        exclude_states: Optional[str] = Query(None, description="перечень статусов через запятую для исключения"),
        use_bd: int = Query(1),
        business_day_start: Optional[str] = Query("20:00"),
        _auth: bool = Depends(require_api_key),
    ):
        if client is None:
            raise HTTPException(500, detail="Kaspi client not available inside app for FIFO sync (import failed).")

        # 1) Определим списки ID, если их не передали
        order_ids: List[str] = []
        if ids:
            order_ids = [str(x) for x in ids]
        else:
            # используем уже готовый сборщик диапазона из main.py
            try:
                from app.main import _collect_range  # type: ignore
            except Exception:
                raise HTTPException(500, detail="Cannot import _collect_range from main.py")
            result = await _collect_range(
                tz=tz, date_field=date_field, states=states,
                exclude_states=exclude_states, use_bd=bool(int(use_bd)),
                bd_start=business_day_start or "20:00",
                start=start, end=end, limit=100000, order="asc", grouped=False
            )
            order_ids = [str(it["id"]) for it in (result.get("items") or [])]

        if not order_ids:
            return {"orders": 0, "items": 0, "note": "no orders in range"}

        # 2) Тянем по каждому заказу позиции и нормализуем
        normalized: List[Dict[str, Any]] = []
        for oid in order_ids:
            dt, items, customer = await _fetch_order_with_items(oid)
            if not items:
                continue
            normalized.append({
                "id": oid,
                "date": dt.isoformat(),
                "customer": customer,
                "items": items,
            })

        if not normalized:
            return {"orders": 0, "items": 0, "note": "orders have no items (check client mapping)"}

        # 3) Записываем в БД FIFO
        stats = _upsert_orders_bulk(normalized)
        return {"status": "ok", **stats, "synced_orders": len(normalized)}

    return r
