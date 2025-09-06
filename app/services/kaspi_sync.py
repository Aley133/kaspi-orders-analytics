# app/services/kaspi_sync.py
from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass

# HTTP клиент: requests (опционально)
try:
    import requests
except Exception:  # pragma: no cover
    requests = None

import xml.etree.ElementTree as ET

# ──────────────────────────────────────────────────────────────────────────────
# Конфиг через ENV
# ──────────────────────────────────────────────────────────────────────────────
KASPI_CITY_ID   = os.getenv("KASPI_CITY_ID", "196220100")

# XML-фид (приоритетный и самый стабильный)
KASPI_PRICE_XML_URL = os.getenv("KASPI_PRICE_XML_URL")

# REST (если нужен)
KASPI_API_BASE        = os.getenv("KASPI_API_BASE")
KASPI_MERCHANT_ID     = os.getenv("KASPI_MERCHANT_ID")
KASPI_TOKEN           = os.getenv("KASPI_TOKEN")
KASPI_ENDPOINT_OFFERS = os.getenv("KASPI_ENDPOINT_OFFERS", "/v2/offers")
KASPI_ENDPOINT_PRICES = os.getenv("KASPI_ENDPOINT_PRICES", "/v2/prices")
KASPI_ENDPOINT_STOCKS = os.getenv("KASPI_ENDPOINT_STOCKS", "/v2/stocks")

# Авто-ценообразование
AUTO_REPRICE       = bool(int(os.getenv("KASPI_AUTO_REPRICE", "0")))
MIN_MARGIN_PCT     = float(os.getenv("KASPI_MIN_MARGIN_PCT", "10"))
UNDERCUT_DELTA_PCT = float(os.getenv("KASPI_REPRICE_UNDERCUT", "0"))

# Поведение по умолчанию, если флаг активности не пришёл
KASPI_DEFAULT_ACTIVE = bool(int(os.getenv("KASPI_DEFAULT_ACTIVE", "1")))

# Страховка для режима replace: если пришло < N% от локального каталога — не деактивируем
KASPI_REPLACE_SAFETY_MIN_RATIO = float(os.getenv("KASPI_REPLACE_SAFETY_MIN_RATIO", "0.5"))

# ──────────────────────────────────────────────────────────────────────────────
# Утилиты (локальные, чтобы не импортировать products на уровне модуля)
# ──────────────────────────────────────────────────────────────────────────────
def _norm_sku(s: Any) -> Optional[str]:
    if s is None:
        return None
    sku = str(s).strip()
    return sku or None

def _maybe_int(v: Any) -> Optional[int]:
    try:
        if v is None or v == "":
            return None
        return int(float(str(v).replace(" ", "").replace(",", ".")))
    except Exception:
        return None

def _maybe_float(v: Any) -> Optional[float]:
    try:
        if v is None or v == "":
            return None
        return float(str(v).replace(" ", "").replace(",", "."))
    except Exception:
        return None

def _text(el: Optional[ET.Element]) -> Optional[str]:
    if el is None:
        return None
    t = (el.text or "").strip()
    return t or None

def _parse_xml_smart(content: bytes, city_id: str) -> List[Dict[str, Any]]:
    """
    Универсальный парсер xml-фида Kaspi (и похожих) на best-effort основе.
    Ищем <offer> и собираем самые распространённые поля.
    """
    strip = lambda t: t.split("}")[-1] if "}" in t else t

    root = ET.fromstring(content)
    offers: List[Dict[str, Any]] = []

    # Бывает корень <yml_catalog> -> <shop> -> <offers>
    # но мы просто пройдёмся по всем <offer>
    for off in [el for el in root.iter() if strip(el.tag).lower() == "offer"]:
        # id/code/vendorCode/sku
        sku = (
            off.get("id") or off.get("sku") or off.get("code") or off.get("vendorCode")
        )
        if not sku:
            # бывает как <barcode> вместо sku (не идеально, но fallback)
            sku = _text(off.find(".//barcode")) or _text(off.find(".//bar-code"))
        sku = _norm_sku(sku)
        if not sku:
            continue

        # name/title/model
        name = _text(off.find(".//name")) or _text(off.find(".//title")) or _text(off.find(".//model"))

        # brand/vendor/manufacturer
        brand = _text(off.find(".//brand")) or _text(off.find(".//vendor")) or _text(off.find(".//manufacturer"))

        # category
        category = _text(off.find(".//category")) or _text(off.find(".//categoryName"))

        # barcode
        barcode = _text(off.find(".//barcode")) or _text(off.find(".//bar-code"))

        # price (или блок prices с привязкой к городу)
        price = _maybe_float(_text(off.find(".//price")))

        if price is None:
            # <prices><price cityId="...">123</price>...</prices>
            for p in off.iter():
                if strip(p.tag).lower() == "price":
                    # город может быть в cityId, city-id, id
                    cid = p.get("cityId") or p.get("city-id") or p.get("id")
                    if cid and str(cid) == str(city_id):
                        price = _maybe_float(_text(p))
                        if price is not None:
                            break

        # qty/stock/quantity
        qty = _maybe_int(_text(off.find(".//qty"))) or _maybe_int(_text(off.find(".//stock"))) \
              or _maybe_int(_text(off.find(".//quantity")))

        # active/available/published
        active: Optional[bool] = None
        aval = _text(off.find(".//available")) or _text(off.find(".//isAvailable")) or _text(off.find(".//published"))
        if aval is not None:
            s = aval.strip().lower()
            if s in ("1","true","yes","on","+","да","available","published","visible"):
                active = True
            elif s in ("0","false","no","off","-","нет","hidden","unavailable"):
                active = False

        offers.append({
            "sku": sku,
            "name": name,
            "brand": brand,
            "category": category,
            "price": price,
            "qty": qty,
            "active": active,
            "barcode": barcode,
        })

    return offers

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
    competitor_min_price: Optional[float] = None  # для REST-варианта

# ──────────────────────────────────────────────────────────────────────────────
# Клиент Kaspi (XML/REST)
# ──────────────────────────────────────────────────────────────────────────────
class KaspiClient:
    def __init__(self):
        self.session = None
        if KASPI_API_BASE and requests:
            self.session = requests.Session()
            self.session.headers.update(self._auth_headers())

    def _auth_headers(self) -> Dict[str, str]:
        hdrs = {"User-Agent": "kaspi-sync/1.0"}
        if KASPI_TOKEN:
            hdrs["Authorization"] = f"Bearer {KASPI_TOKEN}"
            hdrs["X-Auth-Token"]  = KASPI_TOKEN
        if KASPI_MERCHANT_ID:
            hdrs["X-Merchant-Id"] = KASPI_MERCHANT_ID
        return hdrs

    # XML feed
    def fetch_via_xml_feed(self) -> List[Offer]:
        if not KASPI_PRICE_XML_URL:
            raise RuntimeError("KASPI_PRICE_XML_URL не задан")
        if not requests:
            raise RuntimeError("requests недоступен для загрузки XML")
        r = requests.get(KASPI_PRICE_XML_URL, timeout=60)
        r.raise_for_status()
        items = _parse_xml_smart(r.content, city_id=KASPI_CITY_ID)
        return [self._norm_item(it) for it in items]

    # REST
    def fetch_via_rest(self) -> List[Offer]:
        if not (KASPI_API_BASE and self.session):
            raise RuntimeError("REST не сконфигурирован (KASPI_API_BASE/KASPI_TOKEN)")
        offers: Dict[str, Offer] = {}

        # 1) офферы, постранично
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

        # 2) подтянуть цены (если есть ручка)
        try:
            for i in range(0, len(skus), 200):
                sl = skus[i:i+200]
                url = f"{KASPI_API_BASE}{KASPI_ENDPOINT_PRICES}"
                r = self.session.post(url, json={"skus": sl}, timeout=60)
                r.raise_for_status()
                data = r.json() if r.content else {}
                rows = data if isinstance(data, list) else data.get("items", [])
                for row in rows:
                    sku = _norm_sku(row.get("sku"))
                    price = _maybe_float(row.get("price"))
                    if sku and sku in offers and price is not None:
                        offers[sku].price = price
        except Exception:
            pass

        # 3) подтянуть остатки/активность
        try:
            for i in range(0, len(skus), 200):
                sl = skus[i:i+200]
                url = f"{KASPI_API_BASE}{KASPI_ENDPOINT_STOCKS}"
                r = self.session.post(url, json={"skus": sl}, timeout=60)
                r.raise_for_status()
                data = r.json() if r.content else {}
                rows = data if isinstance(data, list) else data.get("items", [])
                for row in rows:
                    sku = _norm_sku(row.get("sku"))
                    qty = _maybe_int(row.get("stock"))
                    active = None
                    if "active" in row:
                        active = bool(row.get("active"))
                    elif qty is not None and qty > 0:
                        active = True
                    if sku and sku in offers:
                        if qty is not None:
                            offers[sku].qty = qty
                        if active is not None:
                            offers[sku].active = active
        except Exception:
            pass

        return list(offers.values())

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

        if active is None:
            if qty is not None and qty > 0:
                active = True
            else:
                active = bool(KASPI_DEFAULT_ACTIVE)

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

    def load_offers(self) -> List[Offer]:
        if KASPI_PRICE_XML_URL:
            return self.fetch_via_xml_feed()
        return self.fetch_via_rest()

# ──────────────────────────────────────────────────────────────────────────────
# Repricing (lazy-import DB из products, чтобы не было цикла)
# ──────────────────────────────────────────────────────────────────────────────
def _apply_repricing_if_needed(offers: List[Offer]) -> None:
    if not AUTO_REPRICE or UNDERCUT_DELTA_PCT <= 0:
        return

    from app.api import products as api  # lazy import
    _db = api._db
    _USE_PG = getattr(api, "_USE_PG", False)
    _q = getattr(api, "_q", lambda s: s)

    # последняя себестоимость по sku
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
                s = r["sku"]
                if s in seen:
                    continue
                last_cost[s] = float(r["unit_cost"] or 0)
                seen.add(s)

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
    1) тянем ассортимент из Kaspi (XML/REST)
    2) опционально применяем автоцену
    3) апсертим в БД
    4) в режиме replace деактивируем/удаляем отсутствующие (со страховкой)
    """
    from app.api import products as api  # lazy import для разрыва циклов
    api._ensure_schema()

    _db = api._db
    _USE_PG = getattr(api, "_USE_PG", False)
    _q = getattr(api, "_q", lambda s: s)
    bulk_upsert_products = api.bulk_upsert_products

    client = KaspiClient()
    offers = client.load_offers()

    _apply_repricing_if_needed(offers)

    payload: List[Dict[str, Any]] = []
    keep_codes: List[str] = []
    in_sale = removed = 0

    for o in offers:
        code = o.sku or getattr(o, "code", None)
        if not code:
            continue
        keep_codes.append(code)

        # активен по умолчанию (только явный False => снят)
        active_final = False if (o.active is False) else True
        if active_final:
            in_sale += 1
        else:
            removed += 1

        payload.append({
            "code": code,
            "name": o.name,
            "brand": o.brand,
            "category": o.category,
            "price": o.price,
            "qty": o.qty,
            "active": active_final,
            "barcode": o.barcode,
        })

    # апсерт
    upsert_res = bulk_upsert_products(payload, price_only=price_only)
    inserted = upsert_res.get("inserted", 0) if isinstance(upsert_res, dict) else (upsert_res[0] if upsert_res else 0)
    updated  = upsert_res.get("updated", 0)  if isinstance(upsert_res, dict) else (upsert_res[1] if upsert_res else 0)

    deactivated = deleted = 0

    # страховка деактивации
    skip_deactivation = False
    with _db() as c:
        if _USE_PG:
            db_count = c.execute(_q("SELECT COUNT(*) AS n FROM products")).one()[0]
        else:
            db_count = c.execute("SELECT COUNT(*) AS n FROM products").fetchone()[0]
    if db_count and len(keep_codes) / max(db_count, 1) < KASPI_REPLACE_SAFETY_MIN_RATIO:
        skip_deactivation = True

    if mode.lower() == "replace" and keep_codes and not skip_deactivation:
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
