# app/api/profit_bridge.py
from __future__ import annotations
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, HTTPException, Query, Depends
import os
from datetime import datetime

# Берём общий доступ к БД/схеме/ключу из уже существующего модуля FIFO
from app.api.profit_fifo import _db, _q, _ensure_schema, require_api_key

# Тот же клиент, что используется в проекте
try:
    from app.kaspi_client import KaspiClient  # type: ignore
except Exception:
    from kaspi_client import KaspiClient  # type: ignore


router = APIRouter(tags=["profit"])

def _pick(d: Dict[str, Any], *keys: str, default: Any = None):
    for k in keys:
        v = d.get(k)
        if v not in (None, "", []):
            return v
    return default

def _as_iso(dt: Any) -> str:
    """Безопасно приводим дату/время к ISO-строке."""
    if isinstance(dt, str):
        return dt
    try:
        return dt.isoformat()
    except Exception:
        return str(dt)

def _parse_items(entry: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Унифицируем формат одной позиции заказа.
    Ожидаем поля наподобие: shopSku / sku / code / offerId, quantity, unitPrice/totalPrice.
    """
    # разные SDK и версии отдают чуть разные названия
    attrs = entry.get("attributes", {}) or {}
    sku = _pick(entry, "shopSku", "sku", "code", "offerId") \
          or _pick(attrs, "shopSku", "sku", "code", "offerId")
    qty = _pick(entry, "quantity", "qty", default=0) or _pick(attrs, "quantity", "qty", default=0)
    qty = int(float(qty or 0))
    unit_price = _pick(entry, "unitPrice", "price", default=None)
    if unit_price is None:
        total = _pick(entry, "totalPrice", default=0) or _pick(attrs, "totalPrice", default=0)
        unit_price = (float(total) / qty) if qty else 0.0
    else:
        unit_price = float(unit_price)

    comm = _pick(entry, "commission_pct", "commissionPercent", default=None)
    try:
        comm = float(comm) if comm is not None else None
    except Exception:
        comm = None

    if not sku:
        return []
    return [{
        "sku": str(sku).strip(),
        "qty": qty,
        "unit_price": unit_price,
        "commission_pct": comm
    }]

def _iter_order_items(o: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Достаём список позиций из заказа (entries / positions / items)."""
    entries = (
        o.get("entries") or
        o.get("positions") or
        o.get("items") or
        o.get("lines") or
        []
    )
    out: List[Dict[str, Any]] = []
    for e in entries:
        out += _parse_items(e) or []
    return out


@router.post("/profit/ingest-from-kaspi")
async def ingest_from_kaspi(
    start: str = Query(..., description="YYYY-MM-DD"),
    end: str   = Query(..., description="YYYY-MM-DD"),
    tz: str = Query("Asia/Almaty"),
    date_field: str = Query("creationDate"),
    states: Optional[str] = Query(None, description="CSV: NEW,DELIVERED,..."),
    exclude_states: Optional[str] = Query(None, description="CSV: CANCELED,..."),
    _: bool = Depends(require_api_key),
):
    """
    Тянем заказы из Kaspi за период, нормализуем позиции и
    складываем в таблицы orders / order_items для FIFO.
    """
    token = os.getenv("KASPI_TOKEN")
    if not token:
        raise HTTPException(500, "KASPI_TOKEN is not set")

    _ensure_schema()
    cli = KaspiClient(token=token)

    # пробуем найти итератор заказов «по месту»
    it = None
    for name in ("iter_orders", "iterOrders", "orders_iter"):
        if hasattr(cli, name):
            it = getattr(cli, name)
            break
    if it is None:
        raise HTTPException(501, "KaspiClient: метод обхода заказов не найден (ожидается iter_orders)")

    # собираем фильтры (максимально совместимо)
    kw: Dict[str, Any] = dict(
        start=start, end=end, tz=tz, date_field=date_field,
        states=(states.split(",") if states else None),
        exclude_states=(exclude_states.split(",") if exclude_states else None),
        order="asc",
    )

    inserted_orders = 0
    upserted_orders = 0
    inserted_items = 0

    with _db() as c:
        for o in it(**kw):
            attrs = o.get("attributes", {}) or {}
            oid = str(_pick(o, "id", "number", default=_pick(attrs, "id", "number")))
            if not oid:
                continue
            odt = _pick(o, date_field, default=_pick(attrs, date_field)) \
                  or _pick(o, "date", default=_pick(attrs, "date"))
            odt = _as_iso(odt)

            # UPSERT заказа
            existed = None
            if c.dialect.name == "postgresql":  # SQLAlchemy даёт name
                existed = c.execute(_q("SELECT 1 FROM orders WHERE id=:id"), {"id": oid}).first()
                c.execute(_q("""
                    INSERT INTO orders(id, date, customer)
                    VALUES(:id, :dt, :customer)
                    ON CONFLICT (id) DO UPDATE SET date=EXCLUDED.date, customer=EXCLUDED.customer
                """), {"id": oid, "dt": odt, "customer": _pick(attrs, "customer", default=None)})
            else:
                existed = c.execute("SELECT 1 FROM orders WHERE id=?", (oid,)).fetchone()
                c.execute("""
                    INSERT INTO orders(id, date, customer) VALUES(?,?,?)
                    ON CONFLICT(id) DO UPDATE SET date=excluded.date, customer=excluded.customer
                """, (oid, odt, _pick(attrs, "customer", default=None)))
            upserted_orders += 1 if existed else 0
            inserted_orders += 0 if existed else 1

            # перезаписываем позиции заказа
            if c.dialect.name == "postgresql":
                c.execute(_q("DELETE FROM order_items WHERE order_id=:id"), {"id": oid})
            else:
                c.execute("DELETE FROM order_items WHERE order_id=?", (oid,))

            for itrow in _iter_order_items(o):
                if c.dialect.name == "postgresql":
                    c.execute(_q("""
                        INSERT INTO order_items(order_id, sku, qty, unit_price, commission_pct)
                        VALUES(:oid, :sku, :qty, :p, :comm)
                    """), {"oid": oid, "sku": itrow["sku"], "qty": int(itrow["qty"]),
                           "p": float(itrow["unit_price"]),
                           "comm": float(itrow["commission_pct"]) if itrow["commission_pct"] is not None else None})
                else:
                    c.execute("""
                        INSERT INTO order_items(order_id, sku, qty, unit_price, commission_pct)
                        VALUES(?,?,?,?,?)
                    """, (oid, itrow["sku"], int(itrow["qty"]),
                          float(itrow["unit_price"]),
                          float(itrow["commission_pct"]) if itrow["commission_pct"] is not None else None))
                inserted_items += 1

    return {
        "status": "ok",
        "orders_inserted": inserted_orders,
        "orders_upserted": upserted_orders,
        "items_inserted": inserted_items,
    }

