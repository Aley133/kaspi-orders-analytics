# app/services/kaspi_sync.py
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

# HTTP клиент: requests по-умолчанию
try:
    import requests
except Exception:  # pragma: no cover
    requests = None  # если модуль недоступен, REST/фид работать не будет

# ──────────────────────────────────────────────────────────────────────────────
# Конфиг через ENV
# ──────────────────────────────────────────────────────────────────────────────
KASPI_CITY_ID          = os.getenv("KASPI_CITY_ID", "196220100")
KASPI_PRICE_XML_URL    = os.getenv("KASPI_PRICE_XML_URL")  # если задан — берём XML фид
KASPI_API_BASE         = os.getenv("KASPI_API_BASE")       # база REST API (если используем REST)
KASPI_MERCHANT_ID      = os.getenv("KASPI_MERCHANT_ID")
KASPI_TOKEN            = os.getenv("KASPI_TOKEN")          # Bearer/X-Auth-Token (всё равно)
KASPI_ENDPOINT_OFFERS  = os.getenv("KASPI_ENDPOINT_OFFERS",  "/v2/offers")
KASPI_ENDPOINT_PRICES  = os.getenv("KASPI_ENDPOINT_PRICES",  "/v2/prices")
KASPI_ENDPOINT_STOCKS  = os.getenv("KASPI_ENDPOINT_STOCKS",  "/v2/stocks")

# Авто-ценообразование
AUTO_REPRICE       = bool(int(os.getenv("KASPI_AUTO_REPRICE", "0")))
MIN_MARGIN_PCT     = float(os.getenv("KASPI_MIN_MARGIN_PCT", "10"))
UNDERCUT_DELTA_PCT = float(os.getenv("KASPI_REPRICE_UNDERCUT", "0"))

# ──────────────────────────────────────────────────────────────────────────────
# Утилиты (локальные, чтобы не импортировать products)
# ──────────────────────────────────────────────────────────────────────────────
def _maybe_float(v: Any) -> Optional[float]:
    try:
        if v is None or v == "":
            return None
        return float(str(v).replace(" ", "").replace(",", "."))
    except Exception:
        return None

def _maybe_int(v: Any) -> Optional[int]:
    try:
        if v is None or v == "":
            return None
        return int(float(str(v).replace(" ", "").replace(",", ".")))
    except Exception:
        return None

def _norm_sku(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s or None

# XML → список словарей с полями, близкими к REST
def _parse_xml_smart(xml_bytes: bytes, city_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Универсальный парсер витринного XML-фида Kaspi.
    Ищет <offer> и вынимает основные поля: vendorCode/sku, name, brand, price, quantity/stock, barcode.
    """
    import xml.etree.ElementTree as ET

    def strip(tag: str) -> str:
        return tag.split("}", 1)[-1] if "}" in tag else tag

    root = ET.fromstring(xml_bytes)
    items: List[Dict[str, Any]] = []

    for off in [el for el in root.iter() if strip(el.tag) == "offer"]:
        # атрибуты оффера
        sku = _norm_sku(off.get("sku") or off.get("id") or off.get("code") or off.get("vendorCode"))
        available = off.get("available")
        active: Optional[bool] = None
        if isinstance(available, str):
            s = available.strip().lower()
            if s in ("true", "1", "yes", "да", "+"):
                active = True
            elif s in ("false", "0", "no", "нет", "-"):
                active = False

        row: Dict[str, Any] = {
            "sku": sku,
            "name": None,
            "brand": None,
            "category": None,
            "price": None,
            "qty": None,
            "active": active,
            "barcode": None,
        }

        # дочерние элементы
        for ch in off:
            tag = strip(ch.tag).lower()
            val = (ch.text or "").strip()
            if not val:
                continue

            if tag in ("name", "title", "model"):
                row["name"] = val
            elif tag in ("vendor", "brand"):
                row["brand"] = val
            elif tag in ("barcode", "ean"):
                row["barcode"] = val
            elif tag in ("price", "saleprice", "currentprice"):
                row["price"] = _maybe_float(val)
            elif tag in ("quantity", "qty", "stock", "stockcount"):
                row["qty"] = _maybe_int(val)
            elif tag in ("category", "categoryname"):
                row["category"] = val

        items.append(row)

    return items

# ──────────────────────────────────────────────────────────────────────────────
# DTO
# ──────────────────────────────────────────────────────────────────────────────
@dataclass
class Offer:
    sku: str
    name: Optional[str]
    brand: Optional[str]
    category: Optional[str]
    price: Optional[float]
    qty: Optional[int]
    active: Optional[bool]
    barcode: Optional[str] = None
    competitor_min_price: Optional[float] = None  # если REST возвращает минимум рынка

# ──────────────────────────────────────────────────────────────────────────────
# Клиент Kaspi (XML/REST)
# ──────────────────────────────────────────────────────────────────────────────
class KaspiClient:
    def __init__(self) -> None:
        self.session = None
        if KASPI_API_BASE and requests:
            self.session = requests.Session()
            self.session.headers.update(self._auth_headers())

    def _auth_headers(self) -> Dict[str, str]:
        headers = {"User-Agent": "kaspi-sync/1.0"}
        if KASPI_TOKEN:
            headers["Authorization"] = f"Bearer {KASPI_TOKEN}"
            headers["X-Auth-Token"] = KASPI_TOKEN
        if KASPI_MERCHANT_ID:
            headers["X-Merchant-Id"] = KASPI_MERCHANT_ID
        return headers

    # XML-фид
    def fetch_via_xml_feed(self) -> List[Offer]:
        if not KASPI_PRICE_XML_URL:
            raise RuntimeError("KASPI_PRICE_XML_URL не задан")
        if not requests:
            raise RuntimeError("requests недоступен для HTTP-загрузки XML")
        r = requests.get(KASPI_PRICE_XML_URL, timeout=60)
        r.raise_for_status()
        items = _parse_xml_smart(r.content, city_id=KASPI_CITY_ID)
        return [self._norm_item(it) for it in items]

    # REST
    def fetch_via_rest(self) -> List[Offer]:
        if not (KASPI_API_BASE and self.session):
            raise RuntimeError("REST-параметры не заданы (KASPI_API_BASE/KASPI_TOKEN)")
        offers: Dict[str, Offer] = {}
        page, size = 0, 100
        while True:
            url = f"{KASPI_API_BASE}{KASPI_ENDPOINT_OFFERS}"
            params = {"page": page, "size": size}
            if KASPI_MERCHANT_ID:
                params["merchantId"] = KASPI_MERCHANT_ID
            resp = self.session.get(url, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json() if resp.content else {}
            items = data.get("content") or data.get("items") or data.get("offers") or []
            for raw in items:
                it = self._norm_item(self._map_offer_json(raw))
                if it.sku:
                    offers[it.sku] = it
            total_pages = int(data.get("totalPages") or data.get("pages") or 1)
            page += 1
            if page >= total_pages:
                break

        if not offers:
            return []

        skus = list(offers.keys())

        # цены
        try:
            price_map = self._fetch_prices_map(skus)
            for sku, p in price_map.items():
                if sku in offers and p is not None:
                    offers[sku].price = p
        except Exception:
            pass

        # остатки/активность
        try:
            stock_map = self._fetch_stocks_map(skus)
            for sku, st in stock_map.items():
                if sku not in offers:
                    continue
                qty, active = st
                if qty is not None:
                    offers[sku].qty = qty
                if active is not None:
                    offers[sku].active = active
        except Exception:
            pass

        return list(offers.values())

    def _fetch_prices_map(self, skus: List[str]) -> Dict[str, Optional[float]]:
        if not KASPI_ENDPOINT_PRICES:
            return {}
        out: Dict[str, Optional[float]] = {}
        for i in range(0, len(skus), 200):
            payload = {"skus": skus[i:i+200]}
            url = f"{KASPI_API_BASE}{KASPI_ENDPOINT_PRICES}"
            r = self.session.post(url, json=payload, timeout=60)
            r.raise_for_status()
            data = r.json() if r.content else {}
            rows = data if isinstance(data, list) else data.get("items", [])
            for row in rows:
                sku = _norm_sku(row.get("sku"))
                price = _maybe_float(row.get("price"))
                if sku:
                    out[sku] = price
        return out

    def _fetch_stocks_map(self, skus: List[str]) -> Dict[str, Tuple[Optional[int], Optional[bool]]]:
        if not KASPI_ENDPOINT_STOCKS:
            return {}
        out: Dict[str, Tuple[Optional[int], Optional[bool]]] = {}
        for i in range(0, len(skus), 200):
            payload = {"skus": skus[i:i+200]}
            url = f"{KASPI_API_BASE}{KASPI_ENDPOINT_STOCKS}"
            r = self.session.post(url, json=payload, timeout=60)
            r.raise_for_status()
            data = r.json() if r.content else {}
            rows = data if isinstance(data, list) else data.get("items", [])
            for row in rows:
                sku = _norm_sku(row.get("sku"))
                qty = _maybe_int(row.get("stock"))
                if "active" in row:
                    active = bool(row.get("active"))
                else:
                    active = True if (qty is not None and qty > 0) else None
                if sku:
                    out[sku] = (qty, active)
        return out

    @staticmethod
    def _map_offer_json(raw: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "sku": raw.get("sku") or raw.get("code") or raw.get("vendorCode") or raw.get("id"),
            "name": raw.get("name") or raw.get("title") or raw.get("model"),
            "brand": raw.get("brand") or raw.get("vendor"),
            "category": raw.get("category") or raw.get("categoryName"),
            "price": raw.get("price") or raw.get("salePrice") or raw.get("currentPrice"),
            "qty": raw.get("qty") or raw.get("stock") or raw.get("stockCount"),
            "active": raw.get("active") if "active" in raw else raw.get("published"),
            "barcode": raw.get("barcode") or raw.get("ean"),
            "competitor_min_price": raw.get("minPrice") or raw.get("minimalPrice"),
        }

    @staticmethod
    def _norm_item(it: Dict[str, Any]) -> Offer:
        sku = _norm_sku(it.get("sku"))
        if not sku:
            return Offer(sku="", name=None, brand=None, category=None, price=None, qty=None, active=None)
        active = it.get("active")
        if isinstance(active, str):
            s = active.strip().lower()
            active = True if s in ("1","true","yes","on","да","+","published","visible") else \
                     False if s in ("0","false","no","off","нет","-","hidden") else None
        price = _maybe_float(it.get("price"))
        qty   = _maybe_int(it.get("qty"))
        if active is None and qty is not None:
            active = True if qty > 0 else None
        return Offer(
            sku=sku,
            name=it.get("name"),
            brand=it.get("brand"),
            category=it.get("category"),
            price=price,
            qty=qty,
            active=active,
            barcode=it.get("barcode"),
            competitor_min_price=_maybe_float(it.get("competitor_min_price")),
        )

    # публичная точка входа
    def load_offers(self) -> List[Offer]:
        if KASPI_PRICE_XML_URL:
            return self.fetch_via_xml_feed()
        return self.fetch_via_rest()

# ──────────────────────────────────────────────────────────────────────────────
# Repricing: аккуратно, с ограничением минимальной маржи
# ──────────────────────────────────────────────────────────────────────────────
def _apply_repricing_if_needed(offers: List[Offer]) -> None:
    if not AUTO_REPRICE or UNDERCUT_DELTA_PCT <= 0:
        return

    # ленивый импорт инфраструктуры БД из products — БЕЗ циклического импорта
    from app.api import products as api
    _db = api._db
    _USE_PG = getattr(api, "_USE_PG", False)
    _q = getattr(api, "_q", lambda s: s)

    # подтянем последнюю себестоимость по sku из batches
    last_cost: Dict[str, float] = {}
    with _db() as c:
        if _USE_PG:
            rows = c.execute(_q("""
                SELECT DISTINCT ON (sku) sku, unit_cost
                  FROM batches
              ORDER BY sku, date DESC, id DESC
            """)).all()
            for r in rows:
                last_cost[r._mapping["sku"]] = float(r._mapping["unit_cost"] or 0)
        else:
            seen = set()
            for r in c.execute("SELECT sku, unit_cost, date, id FROM batches ORDER BY sku, date DESC, id DESC"):
                sku = r["sku"]
                if sku in seen:
                    continue
                last_cost[sku] = float(r["unit_cost"] or 0)
                seen.add(sku)

    for off in offers:
        if off.competitor_min_price is None:
            continue
        cost = last_cost.get(off.sku)
        if not cost or cost <= 0:
            continue

        target = off.competitor_min_price * (1.0 - UNDERCUT_DELTA_PCT/100.0)
        min_allowed = cost * (1.0 + MIN_MARGIN_PCT/100.0)
        if target < min_allowed:
            target = min_allowed

        # если изменение >1% — обновим локально
        if off.price is None or abs(off.price - target) / max(off.price or 1, 1) > 0.01:
            off.price = round(target, 2)

# ──────────────────────────────────────────────────────────────────────────────
# Синхронизация в локальную БД
# ──────────────────────────────────────────────────────────────────────────────
@dataclass
class SyncResult:
    items_in_kaspi: int
    inserted: int
    updated: int
    deactivated: int
    deleted: int
    in_sale: int
    removed: int

def kaspi_sync_run(
    *,
    mode: str = "merge",
    price_only: bool = True,
    hard_delete_missing: bool = False
) -> SyncResult:
    """
    - тянем ассортимент из Kaspi (XML или REST),
    - опционально репрайсим,
    - апсертим в БД,
    - при mode='replace' снимаем/удаляем то, чего нет в Kaspi.
    """
    # ленивый импорт, чтобы не образовать цикл
    from app.api import products as api
    _ensure_schema = api._ensure_schema
    _db = api._db
    _USE_PG = getattr(api, "_USE_PG", False)
    _q = getattr(api, "_q", lambda s: s)
    bulk_upsert_products = api.bulk_upsert_products

    _ensure_schema()

    client = KaspiClient()
    offers = client.load_offers()

    _apply_repricing_if_needed(offers)

    # готовим payload (используем 'code' как ключ)
    payload: List[Dict[str, Any]] = []
    keep_codes: List[str] = []
    in_sale = 0
    removed = 0

    for o in offers:
        code = o.sku or getattr(o, "code", None)
        if not code:
            continue
        keep_codes.append(code)
        if o.active is True:
            in_sale += 1
        elif o.active is False:
            removed += 1
        payload.append({
            "code": code,
            "name": o.name,
            "brand": o.brand,
            "category": o.category,
            "price": o.price,
            "qty": o.qty,
            "active": o.active,
            "barcode": o.barcode,
        })

    # апсерт
    upsert_res = bulk_upsert_products(payload, price_only=price_only)
    inserted = upsert_res.get("inserted", 0) if isinstance(upsert_res, dict) else (upsert_res[0] if upsert_res else 0)
    updated  = upsert_res.get("updated", 0)  if isinstance(upsert_res, dict) else (upsert_res[1] if upsert_res else 0)

    deactivated = 0
    deleted = 0

    if mode.lower() == "replace" and keep_codes:
        with _db() as c:
            if hard_delete_missing:
                if _USE_PG:
                    placeholders = ", ".join([f":c{i}" for i in range(len(keep_codes))])
                    sql = _q(f"DELETE FROM products WHERE code NOT IN ({placeholders})")
                    params = {f"c{i}": v for i, v in enumerate(keep_codes)}
                    r = c.execute(sql, params)
                else:
                    placeholders = ", ".join(["?"] * len(keep_codes))
                    r = c.execute(f"DELETE FROM products WHERE code NOT IN ({placeholders})", keep_codes)
                deleted = getattr(r, "rowcount", 0) or 0
            else:
                if _USE_PG:
                    placeholders = ", ".join([f":c{i}" for i in range(len(keep_codes))])
                    sql = _q(f"UPDATE products SET active=0 WHERE code NOT IN ({placeholders}) AND active<>0")
                    params = {f"c{i}": v for i, v in enumerate(keep_codes)}
                    r = c.execute(sql, params)
                else:
                    placeholders = ", ".join(["?"] * len(keep_codes))
                    r = c.execute(
                        f"UPDATE products SET active=0 WHERE code NOT IN ({placeholders}) AND active<>0",
                        keep_codes
                    )
                deactivated = getattr(r, "rowcount", 0) or 0

    return SyncResult(
        items_in_kaspi=len(offers),
        inserted=inserted,
        updated=updated,
        deactivated=deactivated,
        deleted=deleted,
        in_sale=in_sale,
        removed=removed,
    )

# ──────────────────────────────────────────────────────────────────────────────
# Утилита «список из БД» (для вкладок в UI)
# ──────────────────────────────────────────────────────────────────────────────
def list_from_db(
    *,
    active: Optional[bool],
    limit: int = 1000,
    offset: int = 0,
    search: str = ""
) -> List[Dict[str, Any]]:
    """
    Вернёт товары из локальной БД c полями:
      code, name, brand, category, price, qty, active, barcode
    """
    from app.api import products as api
    _ensure_schema = api._ensure_schema
    _db = api._db
    _USE_PG = getattr(api, "_USE_PG", False)
    _q = getattr(api, "_q", lambda s: s)

    _ensure_schema()
    with _db() as c:
        if _USE_PG:
            sql = """
              SELECT code, name, brand, category, price, qty, active, barcode
                FROM products
            """
            conds, params = [], {}
            if active is True:
                conds.append("active=1")
            elif active is False:
                conds.append("COALESCE(active,0)=0")
            if search:
                conds.append("(code ILIKE :q OR name ILIKE :q)")
                params["q"] = f"%{search}%"
            if conds:
                sql += " WHERE " + " AND ".join(conds)
            sql += " ORDER BY name LIMIT :lim OFFSET :off"
            params.update({"lim": limit, "off": offset})
            rows = c.execute(_q(sql), params).all()
            return [dict(r._mapping) for r in rows]
        else:
            sql = """
              SELECT code, name, brand, category, price, qty, active, barcode
                FROM products
            """
            conds, params = [], []
            if active is True:
                conds.append("active=1")
            elif active is False:
                conds.append("COALESCE(active,0)=0")
            if search:
                conds.append("(code LIKE ? OR name LIKE ?)")
                params += [f"%{search}%", f"%{search}%"]
            if conds:
                sql += " WHERE " + " AND ".join(conds)
            sql += " ORDER BY name COLLATE NOCASE LIMIT ? OFFSET ?"
            params += [limit, offset]
            rows = [dict(r) for r in c.execute(sql, params).fetchall()]
            return rows
