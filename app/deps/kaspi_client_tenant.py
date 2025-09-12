# app/deps/kaspi_client_tenant.py
import os, httpx
from datetime import datetime, timedelta, date
from typing import Dict, Iterable
from .auth import get_current_kaspi_token

KASPI_BASE_URL = (os.getenv("KASPI_BASE_URL") or "https://kaspi.kz/shop/api/v2").rstrip("/")

def _to_ms(d: datetime | date) -> int:
    if isinstance(d, date) and not isinstance(d, datetime):
        d = datetime(d.year, d.month, d.day)
    return int(d.timestamp() * 1000)

class KaspiClient:
    def __init__(self, token: str | None = None, base_url: str | None = None):
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

    def iter_orders(self, *, start, end, filter_field: str = "creationDate") -> Iterable[dict]:
        # включаем конец дня: [start; end 23:59:59.999]
        start_ms = _to_ms(start)
        end_ms = _to_ms(end + timedelta(days=1)) - 1

        field = (filter_field or "creationDate").strip()  # creationDate|plannedShipmentDate|shipmentDate|deliveryDate

        params = {
            "include": "entries",
            "page[size]": 200,
            "page[number]": 1,
            "filter[orders][by]": field,
            f"filter[orders][{field}][ge]": start_ms,
            f"filter[orders][{field}][le]": end_ms,
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
                # next уже содержит page[number], поэтому дальше ходим по ссылке как есть
                url = nxt
                params = {}
