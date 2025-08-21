from __future__ import annotations
from datetime import datetime
from typing import Dict, Generator, Optional
import httpx
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

BASE_URL = "https://kaspi.kz/shop/api/v2"

class KaspiClient:
    def __init__(self, token: str, base_url: str = BASE_URL, timeout_connect: float = 10.0, timeout_read: float = 20.0):
        self.base_url = base_url.rstrip('/')
        self.headers = {
            "Accept": "application/vnd.api+json",
            "Content-Type": "application/vnd.api+json",
            "X-Auth-Token": token,
            "User-Agent": "kaspi-orders-service/0.4.4",
        }
        self.timeout = httpx.Timeout(connect=timeout_connect, read=timeout_read, write=timeout_read, pool=timeout_read)

    @retry(wait=wait_exponential(multiplier=1, min=1, max=8),
           stop=stop_after_attempt(3), reraise=True,
           retry=retry_if_exception_type((httpx.RequestError, httpx.HTTPStatusError)))
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

    def iter_orders(self, start: datetime, end: datetime, state: Optional[str] = None, page_size: int = 50, filter_field: str = "creationDate") -> Generator[Dict, None, None]:
        if page_size > 100: page_size = 100
        params: Dict[str, object] = {
            "page[number]": 0,
            "page[size]": page_size,
            f"filter[orders][{filter_field}][$ge]": self._to_ms(start),
            f"filter[orders][{filter_field}][$le]": self._to_ms(end),
        }
        if state: params["filter[orders][state]"] = state
        while True:
            data = self._get("orders", params)
            items = data.get("data", [])
            for it in items: yield it
            meta = data.get("meta", {})
            page_count = meta.get("pageCount")
            current = int(params["page[number]"])
            if page_count is not None and current + 1 >= int(page_count): break
            if not items: break
            params["page[number]"] = current + 1
