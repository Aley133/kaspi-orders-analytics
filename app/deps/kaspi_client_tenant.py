# app/deps/kaspi_client_tenant.py
import os
import httpx
from datetime import datetime, date
from typing import Dict, Iterable, Any

from .auth import get_current_kaspi_token

KASPI_BASE_URL = (os.getenv("KASPI_BASE_URL") or "https://kaspi.kz/shop/api/v2").rstrip("/")


def _to_ms(v: Any) -> int:
    """Приводим вход (int|float|str|datetime|date) к миллисекундам epoch."""
    if v is None:
        return 0
    if isinstance(v, (int, float)):
        n = int(v)
        # если похоже на миллисекунды — оставляем, иначе секунды -> мс
        return n if n > 10_000_000_000 else n * 1000
    if isinstance(v, datetime):
        # tz-aware -> UTC timestamp; naive — как есть (ок)
        return int(v.timestamp() * 1000) if v.tzinfo else int(v.replace(tzinfo=None).timestamp() * 1000)
    if isinstance(v, date):
        dt = datetime(v.year, v.month, v.day)
        return int(dt.timestamp() * 1000)
    s = str(v).strip()
    if not s:
        return 0
    if s.isdigit():
        n = int(s)
        return n if n > 10_000_000_000 else n * 1000
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return int(dt.timestamp() * 1000)
    except Exception:
        return 0


class KaspiClient:  # ← ВАЖНО: экспортируем именно KaspiClient (как ждут main.py и re-export)
    """
    Клиент Kaspi с ручной пагинацией page[number].
    filter_field: creationDate | plannedShipmentDate | shipmentDate | deliveryDate
    """

    def __init__(self, base_url: str | None = None):
        self.base_url = (base_url or KASPI_BASE_URL).rstrip("/")

    def _headers(self) -> Dict[str, str]:
        token = get_current_kaspi_token()
        if not token:
            raise RuntimeError("Kaspi token is not set for this tenant")
        return {
            "X-Auth-Token": token,
            "Accept": "application/vnd.api+json",
            "Content-Type": "application/vnd.api+json",
            "User-Agent": "KaspiAnalytics/1.0",
        }

    def iter_orders(self, *, start, end, filter_field: str = "creationDate") -> Iterable[dict]:
        start_ms = _to_ms(start)
        end_ms = _to_ms(end)
        if not start_ms or not end_ms:
            raise RuntimeError(f"Invalid start/end for Kaspi filter: start={start} end={end}")

        page_size = 200
        page_num = 1

        base_params = {
            "filter[orders][by]": filter_field,
            "filter[orders][date][ge]": start_ms,
            "filter[orders][date][le]": end_ms,
            "page[size]": page_size,
            "include": "entries",
        }

        with httpx.Client(base_url=self.base_url, timeout=60.0) as cli:
            while True:
                params = dict(base_params)
                params["page[number]"] = page_num

                r = cli.get("/orders", params=params, headers=self._headers())
                try:
                    r.raise_for_status()
                except httpx.HTTPStatusError as e:
                    body = ""
                    try:
                        body = r.text
                    except Exception:
                        pass
                    raise RuntimeError(f"Kaspi API {r.status_code}: {body or e}") from e

                j = r.json()
                data = j.get("data") or []
                for it in data:
                    yield it

                if len(data) < page_size:
                    break  # больше страниц нет

                page_num += 1


__all__ = ["KaspiClient"]
