from __future__ import annotations
from typing import Dict, Iterator, Optional
from datetime import datetime, timezone
import httpx

DEFAULT_BASE = "https://kaspi.kz/shop/api/v2"

def _to_ms(dt: datetime) -> int:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)

class KaspiClient:
    def __init__(self, token: str, base_url: Optional[str] = None, read_timeout: float = 80.0):
        self.base_url = (base_url or DEFAULT_BASE).rstrip("/")
        self.token = token
        self.timeout = httpx.Timeout(connect=10.0, read=read_timeout, write=20.0, pool=60.0)
        self.limits = httpx.Limits(max_connections=20, max_keepalive_connections=10)

    def _headers(self) -> Dict[str, str]:
        return {
            "X-Auth-Token": self.token,
            "Accept": "application/vnd.api+json",
            "Content-Type": "application/vnd.api+json",
            "User-Agent": "Mozilla/5.0",
        }

    def iter_orders(self, *, start: datetime, end: datetime, filter_field: str = "creationDate") -> Iterator[Dict]:
        """
        Итератор по заказам в интервале [start; end].
        filter_field ∈ {"creationDate","plannedShipmentDate","shipmentDate","deliveryDate"}
        """
        page = 0
        while True:
            params = {
                "page[number]": str(page),
                "page[size]": "200",
                f"filter[{filter_field}][ge]": str(_to_ms(start)),
                f"filter[{filter_field}][le]": str(_to_ms(end)),
                "include": "attributes",
            }
            url = f"{self.base_url}/orders"
            with httpx.Client(timeout=self.timeout, limits=self.limits) as cli:
                r = cli.get(url, headers=self._headers(), params=params)
                r.raise_for_status()
                j = r.json()
            data = j.get("data") or []
            if not data:
                return
            for it in data:
                yield it
            page += 1
