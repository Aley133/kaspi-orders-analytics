from __future__ import annotations
from datetime import datetime
from typing import Dict, Generator, Optional, Any
import httpx
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

BASE_URL = "https://kaspi.kz/shop/api/v2"


class KaspiClient:
    def __init__(
        self,
        token: str,
        base_url: str = BASE_URL,
        timeout_connect: float = 10.0,
        timeout_read: float = 20.0,
    ):
        self.base_url = base_url.rstrip("/")
        self.headers = {
            "Accept": "application/vnd.api+json",
            "Content-Type": "application/vnd.api+json",
            "X-Auth-Token": token,
            "User-Agent": "kaspi-orders-service/0.4.6",
        }
        self.timeout = httpx.Timeout(
            connect=timeout_connect, read=timeout_read, write=timeout_read, pool=timeout_read
        )

    # -------------------- low-level --------------------

    @retry(
        wait=wait_exponential(multiplier=1, min=1, max=8),
        stop=stop_after_attempt(3),
        reraise=True,
        retry=retry_if_exception_type((httpx.RequestError, httpx.HTTPStatusError)),
    )
    def _get(self, path: str, params: Dict[str, object]) -> Dict:
        url = f"{self.base_url}/{path.lstrip('/')}"
        with httpx.Client(headers=self.headers, timeout=self.timeout) as client:
            resp = client.get(url, params=params)
            resp.raise_for_status()
            return resp.json()

    @staticmethod
    def _to_ms(dt: datetime) -> int:
        if dt.tzinfo is None:
            import time

            return int(time.mktime(dt.timetuple()) * 1000)
        return int(dt.timestamp() * 1000)

    # -------------------- orders --------------------

    def iter_orders(
        self,
        start: datetime,
        end: datetime,
        page_size: int = 50,
        filter_field: str = "creationDate",
        state: Optional[str] = None,
    ) -> Generator[Dict, None, None]:
        if page_size > 100:
            page_size = 100
        params: Dict[str, object] = {
            "page[number]": 0,
            "page[size]": page_size,
            f"filter[orders][{filter_field}][$ge]": self._to_ms(start),
            f"filter[orders][{filter_field}][$le]": self._to_ms(end),
        }
        if state:
            params["filter[orders][state]"] = state
        while True:
            data = self._get("orders", params)
            items = data.get("data", [])
            for it in items:
                yield it
            meta = data.get("meta", {})
            page_count = meta.get("pageCount")
            current = int(params["page[number]"])
            if page_count is not None and current + 1 >= int(page_count):
                break
            if not items:
                break
            params["page[number]"] = current + 1

    # -------------------- products/offers/catalog --------------------
    # Универсальный JSON:API-итератор с поддержкой links.next

    def _iter_jsonapi(self, rel_url: str, params: Optional[Dict[str, Any]] = None) -> Generator[Dict, None, None]:
        """
        Итерирует по JSON:API спискам, следуя links.next.
        Возвращает элементы (обычно словари) из массива data или сам объект, если API возвращает не JSON:API.
        """
        url = f"{self.base_url}/{rel_url.lstrip('/')}"
        with httpx.Client(headers=self.headers, timeout=self.timeout) as client:
            while True:
                resp = client.get(url, params=params)
                resp.raise_for_status()
                js = resp.json()

                data = js.get("data") if isinstance(js, dict) else js
                if isinstance(data, list):
                    for item in data:
                        yield item
                elif data:
                    # одиночный объект
                    yield data

                # пагинация по JSON:API
                next_link = js.get("links", {}).get("next") if isinstance(js, dict) else None
                if not next_link:
                    break
                url = next_link
                params = None  # next уже включает querystring

    @staticmethod
    def _wrap_product_item(item: Dict[str, Any]) -> Dict[str, Any]:
        """
        Нормализация под формат {'id': ..., 'attributes': {...}},
        чтобы совпадало с тем, что ожидает остальной код/export.
        """
        if not isinstance(item, dict):
            return {"id": str(item), "attributes": {"raw": item}}

        attrs = item.get("attributes")
        if not isinstance(attrs, dict):
            # не JSON:API — соберём attrs из плоского объекта, исключая служебные ключи
            attrs = {k: v for k, v in item.items() if k not in ("id", "type", "links", "relationships")}

        pid = (
            item.get("id")
            or attrs.get("id")
            or attrs.get("sku")
            or attrs.get("code")
            or attrs.get("offerId")
            or attrs.get("article")
        )
        return {"id": pid, "attributes": attrs}

    def iter_products(self, active_only: bool = True, page_size: int = 100) -> Generator[Dict, None, None]:
        """
        Итерация по товарам/офферам. Пробуем несколько возможных эндпоинтов,
        чтобы не зависеть от версии/типа аккаунта. Возвращаем нормализованные элементы.

        Возвращает элементы вида: {'id': ..., 'attributes': {...}}.
        """
        if page_size > 200:
            page_size = 200

        # набор потенциальных эндпоинтов с фильтрами активности
        variants = [
            # Вариант 1: товары
            ("merchant/products", {"page[size]": page_size, "filter[products][active]": "true" if active_only else None}),
            # Вариант 2: офферы каталога
            ("catalog/offers", {"page[size]": page_size, "filter[offers][active]": "true" if active_only else None}),
            # Вариант 3: короткий путь offers
            ("offers", {"page[size]": page_size, "filter[offers][active]": "true" if active_only else None}),
            # Вариант 4: singular 'offer'
            ("merchant/offer", {"page[size]": page_size, "filter[offer][active]": "true" if active_only else None}),
        ]

        last_err: Optional[Exception] = None

        for rel, params in variants:
            # убираем None, если фильтр активности не нужен/не поддерживается
            q = {k: v for k, v in (params or {}).items() if v is not None}
            try:
                any_yielded = False
                for raw in self._iter_jsonapi(rel, params=q):
                    any_yielded = True
                    yield self._wrap_product_item(raw)
                if any_yielded:
                    # этот эндпоинт сработал — завершаем
                    return
            except httpx.HTTPStatusError as e:
                # 404/403 — пробуем следующий вариант; другие статусы — пробрасываем
                if e.response.status_code in (404, 403):
                    last_err = e
                    continue
                raise
            except Exception as e:
                # запомним и попробуем следующий
                last_err = e
                continue

        # если ни один вариант не сработал — поднимем последнюю ошибку или общую
        if last_err:
            raise last_err
        raise RuntimeError("No known products endpoint responded")

    # Совместимость: алиасы
    def iter_offers(self, active_only: bool = True, page_size: int = 100) -> Generator[Dict, None, None]:
        """Алиас к iter_products (offers == products для нашей выгрузки)."""
        yield from self.iter_products(active_only=active_only, page_size=page_size)

    def iter_catalog(self, active_only: bool = True, page_size: int = 100) -> Generator[Dict, None, None]:
        """Алиас к iter_products."""
        yield from self.iter_products(active_only=active_only, page_size=page_size)
