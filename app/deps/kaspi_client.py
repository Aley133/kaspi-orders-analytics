from __future__ import annotations
from typing import Dict, Iterator, Optional
from datetime import datetime, timezone
import httpx, asyncio

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
        self.limits = httpx.Limits(max_connections=10, max_keepalive_connections=5)

    def _headers(self) -> Dict[str, str]:
        return {
            "X-Auth-Token": self.token,
            "Accept": "application/vnd.api+json",
            "Content-Type": "application/vnd.api+json",
            "User-Agent": "Mozilla/5.0",
        }

    def iter_orders(self, *, start: datetime, end: datetime, filter_field: str = "creationDate") -> Iterator[Dict]:
        page = 0
        while True:
            params = {
                "page[number]": str(page),
                "page[size]": "1",
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

    async def verify_token(self) -> bool:
        url = f"{self.base_url}/orders"
        params = {"page[number]": "0", "page[size]": "1"}
        async with httpx.AsyncClient(timeout=self.timeout, limits=self.limits) as cli:
            r = await cli.get(url, headers=self._headers(), params=params)
            if r.status_code == 200:
                return True
            if r.status_code in (401, 403):
                return False
            r.raise_for_status()
            return True
