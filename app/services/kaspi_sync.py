# app/services/kaspi_sync.py
from __future__ import annotations

import os
import math
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple
from dataclasses import dataclass

# HTTP клиент: requests по-умолчанию (можно заменить на httpx)
try:
    import requests
except Exception:  # pragma: no cover
    requests = None  # поднимем ошибку при использовании REST

# Используем готовые хелперы/схему из вашего products.py
# ВАЖНО: здесь НЕТ циклического импорта (products не импортирует этот модуль).
from app.api.products import (
    _db, _USE_PG, _q, _ensure_schema, _upsert_products,
    _maybe_float, _maybe_int, _norm_sku, _parse_xml_smart,
)

# ──────────────────────────────────────────────────────────────────────────────
# Конфиг через ENV
# ──────────────────────────────────────────────────────────────────────────────
KASPI_CITY_ID          = os.getenv("KASPI_CITY_ID", "196220100")
KASPI_PRICE_XML_URL    = os.getenv("KASPI_PRICE_XML_URL")  # если задан, возьмём фид (быстро и надёжно)
KASPI_API_BASE         = os.getenv("KASPI_API_BASE")       # если нужен REST, указать базу, напр.: "https://kaspi.kz/shop/api"
KASPI_MERCHANT_ID      = os.getenv("KASPI_MERCHANT_ID")
KASPI_TOKEN            = os.getenv("KASPI_TOKEN")          # Bearer / X-Auth-Token — зависит от вашего шлюза
# Кастомные пути REST (оставлены гибкими — у разных интеграций похоже, но не идентично)
KASPI_ENDPOINT_OFFERS  = os.getenv("KASPI_ENDPOINT_OFFERS",  "/v2/offers")   # список витрины (пагинация)
KASPI_ENDPOINT_PRICES  = os.getenv("KASPI_ENDPOINT_PRICES",  "/v2/prices")   # цены по sku
KASPI_ENDPOINT_STOCKS  = os.getenv("KASPI_ENDPOINT_STOCKS",  "/v2/stocks")   # остатки по sku

# Авто-ценообразование (простое правило)
AUTO_REPRICE            = bool(int(os.getenv("KASPI_AUTO_REPRICE", "0")))
MIN_MARGIN_PCT          = float(os.getenv("KASPI_MIN_MARGIN_PCT", "10"))      # не падать ниже этой маржи
UNDERCUT_DELTA_PCT      = float(os.getenv("KASPI_REPRICE_UNDERCUT", "0"))     # % на сколько «подрезать» конкурентную цену (0 = выкл)

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
    # Доп. инфо, если есть в REST:
    competitor_min_price: Optional[float] = None

# ──────────────────────────────────────────────────────────────────────────────
# Клиент Каспи
# ──────────────────────────────────────────────────────────────────────────────
class KaspiClient:
    """
    Универсальный клиент: умеет забирать товары либо из XML-фида, либо из REST.
    Выбор стратегии — автоматически по ENV (фид в приоритете: он стабильнее).
    """
    def __init__(self):
        self.session = None
        if KASPI_API_BASE and requests:
            self.session = requests.Session()
            self.session.headers.update(self._auth_headers())

    def _auth_headers(self) -> Dict[str, str]:
        # У разных инсталляций шапка бывает Bearer / X-Auth-Token — оставляем оба варианта.
        hdrs = {"User-Agent": "kaspi-sync/1.0"}
        if KASPI_TOKEN:
            hdrs["Authorization"]  = f"Bearer {KASPI_TOKEN}"
            hdrs["X-Auth-Token"]   = KASPI_TOKEN
        if KASPI_MERCHANT_ID:
            hdrs["X-Merchant-Id"]  = KASPI_MERCHANT_ID
        return hdrs

    # ───────────── ФИД: XML (быстро и просто) ─────────────
    def fetch_via_xml_feed(self) -> List[Offer]:
        if not KASPI_PRICE_XML_URL:
            raise RuntimeError("KASPI_PRICE_XML_URL не задан")
        if not requests:
            raise RuntimeError("Модуль requests недоступен для HTTP-загрузки XML")
        r = requests.get(KASPI_PRICE_XML_URL, timeout=60)
        r.raise_for_status()
        items = _parse_xml_smart(r.content, city_id=KASPI_CITY_ID)
        return [self._norm_item(it) for it in items]

    # ───────────── REST: офферы/цены/остатки ─────────────
    def fetch_via_rest(self) -> List[Offer]:
        if not (KASPI_API_BASE and self.session):
            raise RuntimeError("REST-параметры не заданы (KASPI_API_BASE/KASPI_TOKEN)")
        # 1) тянем офферы постранично
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
            total_pages = (data.get("totalPages") or data.get("pages") or 1)
            page += 1
            if page >= int(total_pages):
                break

        if not offers:
            return []

        skus = list(offers.keys())

        # 2) подтягиваем цены (если есть отдельная ручка)
        try:
            price_map = self._fetch_prices_map(skus)
            for sku, p in price_map.items():
                if sku in offers and p is not None:
                    offers[sku].price = p
        except Exception:
            pass

        # 3) подтягиваем остатки/активность (если есть отдельная ручка)
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
        # батчами по 200
        for i in range(0, len(skus), 200):
            slice_ = skus[i:i+200]
            url = f"{KASPI_API_BASE}{KASPI_ENDPOINT_PRICES}"
            payload = {"skus": slice_}
            r = self.session.post(url, json=payload, timeout=60)
            r.raise_for_status()
            data = r.json() if r.content else {}
            # ожидаем формат: [{"sku":"xxx","price":123.45}, ...]
            for row in data if isinstance(data, list) else data.get("items", []):
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
            slice_ = skus[i:i+200]
            url = f"{KASPI_API_BASE}{KASPI_ENDPOINT_STOCKS}"
            payload = {"skus": slice_}
            r = self.session.post(url, json=payload, timeout=60)
            r.raise_for_status()
            data = r.json() if r.content else {}
            # ожидаем формат: [{"sku":"xxx","stock":5,"active":true}, ...]
            for row in data if isinstance(data, list) else data.get("items", []):
                sku = _norm_sku(row.get("sku"))
                qty = _maybe_int(row.get("stock"))
                active = None
                if "active" in row:
                    active = bool(row.get("active"))
                elif qty is not None:
                    # эвристика: >0 => потенциально в продаже
                    active = True if int(qty) > 0 else None
                if sku:
                    out[sku] = (qty, active)
        return out

    # ───────────── нормализация ─────────────
    @staticmethod
    def _map_offer_json(raw: Dict[str, Any]) -> Dict[str, Any]:
        """
        У разных интеграций поля могут называться по-разному — сминаем в единое.
        """
        return {
            "sku": raw.get("sku") or raw.get("code") or raw.get("vendorCode") or raw.get("id"),
            "name": raw.get("name") or raw.get("title") or raw.get("model"),
            "brand": raw.get("brand") or raw.get("vendor"),
            "category": raw.get("category") or raw.get("categoryName"),
            "price": raw.get("price") or raw.get("salePrice") or raw.get("currentPrice"),
            "qty": raw.get("qty") or raw.get("stock") or raw.get("stockCount"),
            "active": raw.get("active") if "active" in raw else raw.get("published"),
            "barcode": raw.get("barcode") or raw.get("ean"),
            # иногда REST возвращает ещё минимальную цену по рынку:
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
# Repricing (очень аккуратное, в одну строчку логики)
# ──────────────────────────────────────────────────────────────────────────────
def _apply_repricing_if_needed(offers: List[Offer]) -> None:
    """
    Простое правило:
      - если есть competitor_min_price, попытаемся быть ниже на UNDERCUT_DELTA_PCT,
      - но не опускаться ниже минимальной маржи MIN_MARGIN_PCT относительно нашей себестоимости (по последней партии).
    Если себестоимость неизвестна — цену не трогаем.
    """
    if not AUTO_REPRICE or UNDERCUT_DELTA_PCT <= 0:
        return

    # Подтянем последнюю себестоимость из batches
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
        # контроль маржи
        min_allowed = cost * (1.0 + MIN_MARGIN_PCT/100.0)
        if target < min_allowed:
            target = min_allowed

        # если target сильно отличается — обновим локально (в Каспи выкладка цены остаётся на вашей ответственности)
        if off.price is None or abs(off.price - target) / max(off.price or 1, 1) > 0.01:
            off.price = round(target, 2)

# ──────────────────────────────────────────────────────────────────────────────
# Синхронизация в БД «Мой склад»
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

def kaspi_sync_run(*, mode: str = "merge", price_only: bool = True,
                   hard_delete_missing: bool = False) -> SyncResult:
    """
    Основной сценарий:
      - тянем ассортимент из Каспи (XML или REST),
      - опционально применяем авто-цену,
      - апсертим в локальную БД,
      - при mode='replace' деактивируем/удаляем то, чего нет в Каспи,
      - возвращаем разбиение «в продаже» vs «сняты».
    """
    _ensure_schema()

    client = KaspiClient()
    offers = client.load_offers()

    # auto pricing (опционально)
    _apply_repricing_if_needed(offers)

    # Преобразуем в формат апсерта products.py
    payload: List[Dict[str, Any]] = []
    keep_skus: List[str] = []
    in_sale = removed = 0
    for o in offers:
        if not o.sku:
            continue
        keep_skus.append(o.sku)
        if o.active is True:
            in_sale += 1
        elif o.active is False:
            removed += 1
        payload.append({
            "sku": o.sku,
            "name": o.name,
            "brand": o.brand,
            "category": o.category,
            "price": o.price,
            "qty": o.qty,
            "active": o.active,
            "barcode": o.barcode,
        })

    # Апсерт
    inserted, updated = _upsert_products(payload, price_only=price_only)

    # Деактивация/удаление отсутствующих — только в режиме replace
    deactivated = deleted = 0
    if mode.lower() == "replace" and keep_skus:
        # локально выполняем UPDATE/DELETE (без импорта приватных функций, чтобы не плодить зависимостей)
        with _db() as c:
            if hard_delete_missing:
                if _USE_PG:
                    placeholders = ", ".join([f":s{i}" for i in range(len(keep_skus))])
                    sql = _q(f"DELETE FROM products WHERE sku NOT IN ({placeholders})")
                    params = {f"s{i}": s for i, s in enumerate(keep_skus)}
                    r = c.execute(sql, params)
                    deleted = r.rowcount or 0
                else:
                    placeholders = ", ".join(["?"] * len(keep_skus))
                    r = c.execute(f"DELETE FROM products WHERE sku NOT IN ({placeholders})", keep_skus)
                    deleted = r.rowcount or 0
            else:
                if _USE_PG:
                    placeholders = ", ".join([f":s{i}" for i in range(len(keep_skus))])
                    sql = _q(f"UPDATE products SET active=0 WHERE sku NOT IN ({placeholders}) AND active<>0")
                    params = {f"s{i}": s for i, s in enumerate(keep_skus)}
                    r = c.execute(sql, params)
                    deactivated = r.rowcount or 0
                else:
                    placeholders = ", ".join(["?"] * len(keep_skus))
                    r = c.execute(f"UPDATE products SET active=0 WHERE sku NOT IN ({placeholders}) AND active<>0", keep_skus)
                    deactivated = r.rowcount or 0

    return SyncResult(
        items_in_kaspi=len(offers),
        inserted=inserted, updated=updated,
        deactivated=deactivated, deleted=deleted,
        in_sale=in_sale, removed=removed,
    )

# ──────────────────────────────────────────────────────────────────────────────
# Утилиты для чтения «списков» из локальной БД (для двух вкладок в UI)
# ──────────────────────────────────────────────────────────────────────────────
def list_from_db(*, active: Optional[bool], limit: int = 1000, offset: int = 0,
                 search: str = "") -> List[Dict[str, Any]]:
    """
    Вернуть товары из локальной БД, отфильтрованные по активности.
    Это удобный источник для вкладок «В продаже» / «Сняты с продажи».
    """
    _ensure_schema()
    with _db() as c:
        if _USE_PG:
            sql = "SELECT sku,name,brand,category,price,quantity,active FROM products"
            conds, params = [], {}
            if active is True:
                conds.append("active=1")
            elif active is False:
                conds.append("COALESCE(active,0)=0")
            if search:
                conds.append("(sku ILIKE :q OR name ILIKE :q)")
                params["q"] = f"%{search}%"
            if conds:
                sql += " WHERE " + " AND ".join(conds)
            sql += " ORDER BY name LIMIT :lim OFFSET :off"
            params.update({"lim": limit, "off": offset})
            rows = c.execute(_q(sql), params).all()
            return [dict(r._mapping) for r in rows]
        else:
            sql = "SELECT sku,name,brand,category,price,quantity,active FROM products"
            conds, params = [], []
            if active is True:
                conds.append("active=1")
            elif active is False:
                conds.append("COALESCE(active,0)=0")
            if search:
                conds.append("(sku LIKE ? OR name LIKE ?)")
                params += [f"%{search}%", f"%{search}%"]
            if conds:
                sql += " WHERE " + " AND ".join(conds)
            sql += " ORDER BY name COLLATE NOCASE LIMIT ? OFFSET ?"
            params += [limit, offset]
            rows = [dict(r) for r in c.execute(sql, params).fetchall()]
            return rows

