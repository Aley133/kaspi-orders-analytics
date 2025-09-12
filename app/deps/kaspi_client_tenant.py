# deps/app/kaspi_client.py
from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional

import httpx

from .auth import get_current_kaspi_token

KASPI_BASE_URL = (os.getenv("KASPI_BASE_URL") or "https://kaspi.kz/shop/api/v2").rstrip("/")

_ALLOWED_BY = {"creationDate", "shipmentDate", "deliveryDate"}


def _to_ms(v: Any) -> int:
    """
    Преобразует значение в миллисекунды Unix (UTC).
    Поддерживает int/float, строки-числа, ISO-дату/время и 'YYYY-MM-DD'.
    """
    if isinstance(v, (int, float)):
        return int(v)

    s = str(v).strip()
    if s.isdigit():  # уже миллисекунды/секунды как строка
        # Не пытаемся угадать, сек это или мс — бэкенд выше передаёт корректно.
        return int(s)

    # Попытка ISO с временем (включая 'Z')
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        # Формат 'YYYY-MM-DD'
        dt = datetime.strptime(s, "%Y-%m-%d")
        dt = dt.replace(tzinfo=timezone.utc)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return int(dt.timestamp() * 1000)


class KaspiClient:
    """
    Мини-клиент для Kaspi API v2: заказы с фильтром по диапазону дат.
    Используется синхронно (как ожидает main.py).
    """

    def __init__(self, token: Optional[str] = None, base_url: Optional[str] = None) -> None:
        self.base_url = (base_url or KASPI_BASE_URL).rstrip("/")
        self._token_override = (token or "").strip() or None

    def _headers(self) -> Dict[str, str]:
        token = self._token_override or get_current_kaspi_token()
        if not token:
            raise RuntimeError("Kaspi token is not set for this tenant")

        return {
            "X-Auth-Token": token,
            "Accept": "application/vnd.api+json",
            "Content-Type": "application/vnd.api+json",
            "User-Agent": "KaspiAnalytics/1.0",
        }

    @staticmethod
    def _normalize_by(by: Optional[str]) -> str:
        by = (by or "creationDate").strip()
        return by if by in _ALLOWED_BY else "creationDate"

    def iter_orders(self, *, start: Any, end: Any, filter_field: str = "creationDate") -> Iterable[dict]:
        """
        Генератор заказов за указанный период.
        :param start: начало интервала (поддерживает ms/int, ISO, 'YYYY-MM-DD')
        :param end: конец интервала
        :param filter_field: creationDate | shipmentDate | deliveryDate
        :yield: элементы из data[]
        """
        start_ms = _to_ms(start)
        end_ms = _to_ms(end)
        by = self._normalize_by(filter_field)

        params: Dict[str, Any] = {
            "page[size]": 200,
            "filter[orders][by]": by,
            "filter[orders][date][ge]": start_ms,
            "filter[orders][date][le]": end_ms,
            "include": "entries",
        }

        with httpx.Client(base_url=self.base_url, timeout=60.0) as cli:
            url: str = "/orders"
            while True:
                r = cli.get(url, params=params, headers=self._headers())
                if r.status_code == 400:
                    # Покажем исходный текст ошибки Kaspi, чтобы не терять контекст
                    raise RuntimeError(f"Kaspi API 400: {r.text}")
                r.raise_for_status()

                j = r.json()
                data = j.get("data") or []
                for it in data:
                    yield it

                next_link = (j.get("links") or {}).get("next")
                if not next_link:
                    break

                # `links.next` может быть абсолютным — httpx корректно обработает.
                url = next_link
                params = {}  # при переходе по next не передаём старые params

    # Опционально: утилита на случай, если где-то нужен прямой GET
    def get(self, path_or_url: str, *, params: Optional[Dict[str, Any]] = None) -> httpx.Response:
        with httpx.Client(base_url=self.base_url, timeout=60.0) as cli:
            r = cli.get(path_or_url, params=params or {}, headers=self._headers())
            r.raise_for_status()
            return r
