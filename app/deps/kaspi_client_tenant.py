# app/deps/kaspi_client_tenant.py
from __future__ import annotations

import os
from datetime import datetime, timedelta, date
from typing import Dict, Iterable, Optional

import httpx

# Берём токен из middleware (пер-тенант)
from .auth import get_current_kaspi_token

KASPI_BASE_URL = (os.getenv("KASPI_BASE_URL") or "https://kaspi.kz/shop/api/v2").rstrip("/")

# Разрешённые поля дат (поддерживает сам Kaspi)
ALLOWED_DATE_FIELDS = (
    "creationDate",
    "plannedShipmentDate",
    "plannedDeliveryDate",
    "shipmentDate",
    "deliveryDate",
)


def _to_ms(d: datetime | date) -> int:
    """UTC milliseconds from date/datetime (локальное время берём как есть)."""
    if isinstance(d, date) and not isinstance(d, datetime):
        d = datetime(d.year, d.month, d.day)
    return int(d.timestamp() * 1000)


class KaspiClient:
    """
    Пер-tenant клиент.

    ВАЖНО про фильтры Kaspi:
      - когда by=creationDate, диапазон ДОЛЖЕН быть в filter[orders][date][$ge/$le]
      - для остальных полей by=<field> диапазон ожидают в filter[orders][<field>][$ge/$le]
    """

    def __init__(self, *, base_url: Optional[str] = None):
        self.base_url = (base_url or KASPI_BASE_URL).rstrip("/")

    def _headers(self) -> Dict[str, str]:
        token = get_current_kaspi_token()
        if not token:
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
        start: date | datetime,
        end: date | datetime,
        filter_field: str = "creationDate",
    ) -> Iterable[dict]:
        """
        Итератор по заказам за [start; end 23:59:59.999] по указанному полю дат.
        """
        # Границы включительно
        start_ms = _to_ms(start)
        end_ms = _to_ms(end + timedelta(days=1)) - 1

        field = (filter_field or "creationDate").strip()
        if field not in ALLOWED_DATE_FIELDS:
            field = "creationDate"

        # Базовые параметры
        params: Dict[str, object] = {
            "include": "entries",
            "page[size]": 200,
            "page[number]": 1,
            "filter[orders][by]": field,
        }

        # --- КЛЮЧЕВОЙ МОМЕНТ ---
        # creationDate → диапазон в [date]
        # остальные поля → диапазон в [<field>]
        if field == "creationDate":
            params["filter[orders][date][$ge]"] = start_ms
            params["filter[orders][date][$le]"] = end_ms
        else:
            params[f"filter[orders][{field}][$ge]"] = start_ms
            params[f"filter[orders][{field}][$le]"] = end_ms

        with httpx.Client(base_url=self.base_url, timeout=60.0) as cli:
            url = "/orders"
            while True:
                r = cli.get(url, params=params, headers=self._headers())
                try:
                    r.raise_for_status()
                except httpx.HTTPStatusError as e:
                    # пробрасываем тело ошибки, чтобы в логах было видно причину от Kaspi
                    raise RuntimeError(f"Kaspi API {r.status_code}: {r.text or e}") from e

                j = r.json()
                for it in (j.get("data") or []):
                    yield it

                nxt = (j.get("links") or {}).get("next")
                if not nxt:
                    break

                # next может быть абсолютным URL — в таком случае параметры уже включены в ссылку
                url = nxt
                params = {}
