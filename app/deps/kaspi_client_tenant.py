# app/deps/kaspi_client_tenant.py
from __future__ import annotations

import os
from datetime import datetime, timedelta, date
from typing import Dict, Iterable, Union

import httpx

from .auth import get_current_kaspi_token

# Базовый URL Kaspi API (можно переопределить через переменные окружения)
KASPI_BASE_URL = (os.getenv("KASPI_BASE_URL") or "https://kaspi.kz/shop/api/v2").rstrip("/")


# -------- utils --------
def _coerce_date(d: Union[str, datetime, date]) -> datetime:
    """
    Приводит строку 'YYYY-MM-DD' или date к datetime (локальное «00:00:00»),
    datetime возвращает как есть.
    """
    if isinstance(d, datetime):
        return d
    if isinstance(d, date):
        return datetime(d.year, d.month, d.day)
    if isinstance(d, str):
        # допускаем 'YYYY-MM-DD'
        dt = date.fromisoformat(d)
        return datetime(dt.year, dt.month, dt.day)
    raise TypeError(f"Unsupported date type: {type(d)!r}")


def _to_ms(d: Union[str, datetime, date]) -> int:
    return int(_coerce_date(d).timestamp() * 1000)


# -------- client --------
class KaspiClient:
    """
    Минимальный tenant-aware клиент Kaspi.
    Токен подтягивается из текущего tenant-контекста (см. get_current_kaspi_token()).
    """

    _ALLOWED_FIELDS = (
        "creationDate",          # создание заказа
        "plannedShipmentDate",   # план передачи
        "shipmentDate",          # фактическая передача
        "deliveryDate",          # фактическая доставка
    )

    def __init__(self, token: str | None = None, base_url: str | None = None):
        # token параметр оставлен для совместимости, но фактически не используется:
        # мы берём токен из middleware через get_current_kaspi_token()
        self.base_url = (base_url or KASPI_BASE_URL).rstrip("/")

    def _headers(self) -> Dict[str, str]:
        token = get_current_kaspi_token()
        if not token:
            # жёстко: без персонального токена — 401
            raise RuntimeError("Kaspi token is not set for this tenant")
        return {
            "X-Auth-Token": token,
            "Accept": "application/vnd.api+json",
            "Content-Type": "application/vnd.api+json",
            "User-Agent": "leo-analytics/1.0",
        }

    def iter_orders(
        self,
        *,
        start: Union[str, datetime, date],
        end: Union[str, datetime, date],
        filter_field: str = "creationDate",
    ) -> Iterable[dict]:
        """
        Синхронный генератор (поверх httpx). Возвращает элементы из `data` ответа /orders.
        Диапазон дат задаётся через filter[orders][date] + операторы $ge/$le,
        а какое именно поле используется для сравнения — в filter[orders][by].

        Важно: Kaspi ожидает page[number], поэтому стартуем с 1 и далее
        ходим по ссылкам из "links.next".
        """
        # включаем конец дня: [start; end 23:59:59.999]
        start_ms = _to_ms(start)
        end_ms = _to_ms(end + timedelta(days=1)) - 1

        # гарантируем валидное поле
        field = (filter_field or "creationDate").strip()
        if field not in self._ALLOWED_FIELDS:
            field = "creationDate"

        params: Dict[str, object] = {
            "include": "entries",
            "page[size]": 200,
            "page[number]": 1,
            "filter[orders][by]": field,            # ← какое поле учитывать
            # ВАЖНО: диапазон всегда в [orders][date] с операторами $ge/$le
            "filter[orders][date][$ge]": start_ms,
            "filter[orders][date][$le]": end_ms,
        }

        with httpx.Client(base_url=self.base_url, timeout=60.0) as cli:
            url = "/orders"
            while True:
                r = cli.get(url, params=params, headers=self._headers())
                try:
                    r.raise_for_status()
                except httpx.HTTPStatusError as e:
                    body = r.text
                    raise RuntimeError(f"Kaspi API {r.status_code}: {body or e}") from e

                j = r.json()
                for it in (j.get("data") or []):
                    yield it

                nxt = (j.get("links") or {}).get("next")
                if not nxt:
                    break
                # next уже включает page[number], поэтому дальше ходим «как есть»
                url = nxt
                params = {}
