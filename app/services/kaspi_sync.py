# app/services/kaspi_sync.py
from __future__ import annotations
import os
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Tuple

try:
    import requests
except Exception:
    requests = None  # HTTP недоступен — отдадим ошибку при попытке REST

# ──────────────────────────────────────────────────────────────────────────────
# ENV
# ──────────────────────────────────────────────────────────────────────────────
KASPI_CITY_ID   = os.getenv("KASPI_CITY_ID", "196220100")
KASPI_PRICE_XML_URL = os.getenv("KASPI_PRICE_XML_URL")       # если задан — используем XML-фид
KASPI_API_BASE  = os.getenv("KASPI_API_BASE")                # если нужен REST
KASPI_MERCHANT_ID = os.getenv("KASPI_MERCHANT_ID")
KASPI_TOKEN     = os.getenv("KASPI_TOKEN")

KASPI_ENDPOINT_OFFERS = os.getenv("KASPI_ENDPOINT_OFFERS", "/v2/offers")
KASPI_ENDPOINT_PRICES = os.getenv("KASPI_ENDPOINT_PRICES", "/v2/prices")
KASPI_ENDPOINT_STOCKS = os.getenv("KASPI_ENDPOINT_STOCKS", "/v2/stocks")

# Поведение активностии, репрайс, защита replace
KASPI_DEFAULT_ACTIVE = bool(int(os.getenv("KASPI_DEFAULT_ACTIVE", "1")))  # если нет явного active и qty, считаем активным
AUTO_REPRICE         = bool(int(os.getenv("KASPI_AUTO_REPRICE", "0")))
MIN_MARGIN_PCT       = float(os.getenv("KASPI_MIN_MARGIN_PCT", "10"))
UNDERCUT_DELTA_PCT   = float(os.getenv("KASPI_REPRICE_UNDERCUT", "0"))
REPLACE_SAFETY_RATIO = float(os.getenv("KASPI_REPLACE_SAFETY_MIN_RATIO", "0.2"))  # минимум 20% от текущих позиций в БД

# ──────────────────────────────────────────────────────────────────────────────
# Импорт БД-утилит лениво (чтобы исключить циклический импорт)
# ──────────────────────────────────────────────────────────────────────────────
def _db_tools():
    from app.api import products as api
    return api

# ──────────────────────────────────────────────────────────────────────────────
# Хелперы парсинга
# ──────────────────────────────────────────────────────────────────────────────
def _strip_tag(t: Any) -> str:
    s = str(t or "")
    if "}" in s:  # {ns}tag
        s = s.split("}", 1)[1]
    return s.lower()

def _maybe_float(x: Any) -> Optional[float]:
    try:
        if x is None: return None
        s = str(x).strip().replace(" ", "").replace(",", ".")
        return float(s) if s else None
    except Exception:
        return None

def _maybe_int(x: Any) -> Optional[int]:
    try:
        if x is None: return None
        s = str(x).strip().replace(" ", "")
        return int(float(s)) if s else None
    except Exception:
        return None

def _norm_sku(x: Any) -> str:
    return (str(x or "")).strip()

def _parse_xml_smart(buf: bytes, *, city_id: str) -> List[Dict[str, Any]]:
    import xml.etree.ElementTree as ET
    root = ET.fromstring(buf)
    strip = _strip_tag
    def _txt(el):
        return None if el is None else (el.text or "").strip()

    offers: List[Dict[str, Any]] = []
    for off in [el for el in root.iter() if strip(el.tag) == "offer"]:
        sku = _norm_sku(off.get("sku") or off.get("code") or off.get("vendorCode") or off.get("id"))
        if not sku:
            continue
        name = _txt(off.find(".//name"))
        brand = _txt(off.find(".//brand"))
        category = _txt(off.find(".//category"))
        barcode = _txt(off.find(".//barcode")) or _txt(off.find(".//ean"))

        price = _maybe_float(_txt(off.find(".//price")))
        if price is None:
            # <prices><price cityId="...">...</price> — берём для нужного города
            for p in off.iter():
                if strip(p.tag) == "price":
                    cid = p.get("cityId") or p.get("city-id") or p.get("id")
                    if cid and str(cid) == str(city_id):
                        price = _maybe_float(_txt(p))
                        if price is not None:
                            break

        qty = _maybe_int(_txt(off.find(".//qty"))) or _maybe_int(_txt(off.find(".//stock"))) \
              or _maybe_int(_txt(off.find(".//quantity")))

        active = None
        aval = _txt(off.find(".//available")) or _txt(off.find(".//isAvailable")) or _txt(off.find(".//published"))
        if aval is not None:
            s = aval.strip().lower()
            if s in ("1","true","yes","on","+","да","available","published","visible"):
                active = True
            elif s in ("0","false","no","off","-","нет","hidden","unavailable"):
                active = False

        offers.append({
            "sku": sku, "name": name, "brand": brand, "category": category,
            "price": price, "qty": qty, "active": active, "barcode": barcode,
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
    competitor_min_price: Optional[float] = None

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
        h = {"User-Agent": "kaspi-sync/1.0"}
        if KASPI_TOKEN:
            h["Authorization"] = f"Bearer {KASPI_TOKEN}"
            h["X-Auth-Token"] = KASPI_TOKEN
        if KASPI_MERCHANT_ID:
            h["X-Merchant-Id"] = KASPI_MERCHANT_ID
        return h

    def load_offers(self) -> List[Offer]:
        if KASPI_PRICE_XML_URL:
            return self._fetch_via_xml_feed()
        return self._fetch_via_rest()

    def _fetch_via_xml_feed(self) -> List[Offer]:
        if not requests:
            raise RuntimeError("requests недоступен; для XML нужен HTTP клиент")
        if not KASPI_PRICE_XML_URL:
            return []
        r = requests.get(KASPI_PRICE_XML_URL, timeout=60)
        r.raise_for_status()
        rows = _parse_xml_smart(r.content, city_id=KASPI_CITY_ID)
        return [self._norm_row(x) for x in rows]

    def _fetch_via_rest(self) -> List[Offer]:
        if not (KASPI_API_BASE and self.session):
            return []
        page, size = 0, 200
        offers: Dict[str, Offer] = {}
        while True:
            u = f"{KASPI_API_BASE}{KASPI_ENDPOINT_OFFERS}"
            params = {"page": page, "size": size}
            if KASPI_MERCHANT_ID:
                params["merchantId"] = KASPI_MERCHANT_ID
            resp = self.session.get(u, params=params, timeout=60)
            resp.raise_for_status()
            j = resp.json() if resp.content else {}
            items = j.get("content") or j.get("items") or j.get("offers") or []
            for raw in items:
                it = self._map_offer_json(raw)
                off = self._norm_row(it)
                if off.sku:
                    offers[off.sku] = off
            total_pages = int(j.get("totalPages") or j.get("pages") or 1)
            page += 1
            if page >= total_pages:
                break

        if not offers:
            return []
        skus = list(offers.keys())

        # цены
        if KASPI_ENDPOINT_PRICES:
            for i in range(0, len(skus), 200):
                part = skus[i:i+200]
                u = f"{KASPI_API_BASE}{KASPI_ENDPOINT_PRICES}"
                r = self.session.post(u, json={"skus": part}, timeout=60)
                r.raise_for_status()
                data = r.json() if r.content else []
                arr = data if isinstance(data, list) else (data.get("items") or [])
                for row in arr:
                    sku = _norm_sku(row.get("sku"))
                    p = _maybe_float(row.get("price"))
                    if sku in offers and p is not None:
                        offers[sku].price = p

        # остатки/активность
        if KASPI_ENDPOINT_STOCKS:
            for i in range(0, len(skus), 200):
                part = skus[i:i+200]
                u = f"{KASPI_API_BASE}{KASPI_ENDPOINT_STOCKS}"
                r = self.session.post(u, json={"skus": part}, timeout=60)
                r.raise_for_status()
                data = r.json() if r.content else []
                arr = data if isinstance(data, list) else (data.get("items") or [])
                for row in arr:
                    sku = _norm_sku(row.get("sku"))
                    qty = _maybe_int(row.get("stock"))
                    active = None if "active" not in row else bool(row.get("active"))
                    if sku in offers:
                        if qty is not None:
                            offers[sku].qty = qty
                        if active is not None:
                            offers[sku].active = active

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
    def _norm_row(it: Dict[str, Any]) -> Offer:
        sku = _norm_sku(it.get("sku"))
        if not sku:
            return Offer(sku="", name=None, brand=None, category=None, price=None, qty=None, active=None)
        # active нормализуем, но окончательное решение примем в sync_run()
        active = it.get("active")
        if isinstance(active, str):
            s = active.strip().lower()
            active = True if s in ("1","true","yes","on","+","да","available","published","visible") \
                     else False if s in ("0","false","no","off","-","нет","hidden","unavailable") \
                     else None
        return Offer(
            sku=sku,
            name=it.get("name"), brand=it.get("brand"), category=it.get("category"),
            price=_maybe_float(it.get("price")), qty=_maybe_int(it.get("qty")),
            active=active, barcode=it.get("barcode"),
            competitor_min_price=_maybe_float(it.get("competitor_min_price")),
        )

# ──────────────────────────────────────────────────────────────────────────────
# Repricing
# ──────────────────────────────────────────────────────────────────────────────
def _apply_repricing_if_needed(offers: List[Offer]) -> None:
    if not AUTO_REPRICE or UNDERCUT_DELTA_PCT <= 0:
        return
    api = _db_tools()
    # последняя себестоимость по SKU
    last_cost: Dict[str, float] = {}
    with api._db() as c:
        if api._USE_PG:
            rows = c.execute(api._q("""
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
        if off.price is None or abs((off.price or 0) - target) / max(off.price or 1, 1) > 0.01:
            off.price = round(target, 2)

# ──────────────────────────────────────────────────────────────────────────────
# Результат
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

# ──────────────────────────────────────────────────────────────────────────────
# Основной сценарий
# ──────────────────────────────────────────────────────────────────────────────
def kaspi_sync_run(*, mode: str = "merge", price_only: bool = False, hard_delete_missing: bool = False) -> SyncResult:
    """
    - тянем ассортимент из Kaspi (XML/REST),
    - опционально авто-репрайс,
    - апсертим в локальную БД,
    - 'replace': деактивируем/удаляем отсутствующие (с защитой от факапов),
    - возвращаем статистику.
    """
    api = _db_tools()
    api._ensure_schema()

    client = KaspiClient()
    offers = client.load_offers()
    _apply_repricing_if_needed(offers)

    payload: List[Dict[str, Any]] = []
    keep_skus: List[str] = []
    in_sale = removed = 0

    for o in offers:
        if not o.sku:
            continue
        # Правильная трактовка активности
        active_final: Optional[bool]
        if o.active is False:
            active_final = False
        elif o.active is True:
            active_final = True
        elif o.qty is not None:
            active_final = True if int(o.qty) > 0 else False
        else:
            active_final = KASPI_DEFAULT_ACTIVE

        if active_final:
            in_sale += 1
        else:
            removed += 1

        keep_skus.append(o.sku)
        payload.append({
            "sku": o.sku, "code": o.sku,     # и sku, и code — чтобы обе ветки БД-апсерта были счастливы
            "name": o.name, "brand": o.brand, "category": o.category,
            "price": o.price, "qty": o.qty, "active": active_final,
            "barcode": o.barcode,
        })

    # Апсерт
    up = api.bulk_upsert_products(payload, price_only=price_only)
    inserted = (up.get("inserted", 0) if isinstance(up, dict) else (up[0] if up else 0))
    updated  = (up.get("updated",  0) if isinstance(up, dict) else (up[1] if up else 0))

    deactivated = deleted = 0

    # Безопасный REPLACE
    if mode.lower() == "replace" and keep_skus:
        # Защита: если из Каспи пришло слишком мало позиций, не трогаем остальное
        with api._db() as c:
            if api._USE_PG:
                total_db = c.execute(api._q("SELECT COUNT(*) AS n FROM products")).scalar() or 0
            else:
                total_db = c.execute("SELECT COUNT(*) FROM products").fetchone()[0] or 0
        if total_db and len(keep_skus) < int(total_db * REPLACE_SAFETY_RATIO):
            # притормозим — возможно, Каспи отдал урезанный список/ошибка фильтра
            pass
        else:
            with api._db() as c:
                if hard_delete_missing:
                    if api._USE_PG:
                        ph = ", ".join([f":s{i}" for i in range(len(keep_skus))])
                        r = c.execute(api._q(f"DELETE FROM products WHERE sku NOT IN ({ph})"),
                                      {f"s{i}": s for i, s in enumerate(keep_skus)})
                    else:
                        ph = ", ".join(["?"] * len(keep_skus))
                        r = c.execute(f"DELETE FROM products WHERE sku NOT IN ({ph})", keep_skus)
                    deleted = getattr(r, "rowcount", 0) or 0
                else:
                    if api._USE_PG:
                        ph = ", ".join([f":s{i}" for i in range(len(keep_skus))])
                        r = c.execute(api._q(f"UPDATE products SET active=0 WHERE sku NOT IN ({ph}) AND active<>0"),
                                      {f"s{i}": s for i, s in enumerate(keep_skus)})
                    else:
                        ph = ", ".join(["?"] * len(keep_skus))
                        r = c.execute(f"UPDATE products SET active=0 WHERE sku NOT IN ({ph}) AND active<>0", keep_skus)
                    deactivated = getattr(r, "rowcount", 0) or 0

    return SyncResult(
        items_in_kaspi=len(offers),
        inserted=inserted, updated=updated,
        deactivated=deactivated, deleted=deleted,
        in_sale=in_sale, removed=removed,
    )
