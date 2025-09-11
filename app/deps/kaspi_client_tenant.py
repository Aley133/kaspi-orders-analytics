# deps/app/kaspi_client.py
import os, asyncio, httpx
from typing import Dict, Iterable
from deps.auth import get_current_kaspi_token

KASPI_BASE_URL = (os.getenv("KASPI_BASE_URL") or "https://kaspi.kz/shop/api/v2").rstrip("/")

class KaspiClient:
    def __init__(self, token: str | None = None, base_url: str | None = None):
        self.base_url = (base_url or KASPI_BASE_URL).rstrip("/")

    def _headers(self) -> Dict[str,str]:
        token = get_current_kaspi_token()
        if not token:
            # жёстко: без персонального токена — 401
            raise RuntimeError("Kaspi token is not set for this tenant")
        return {
            "X-Auth-Token": token,
            "Accept": "application/vnd.api+json",
            "Content-Type": "application/vnd.api+json",
            "User-Agent": "Mozilla/5.0",
        }

    def iter_orders(self, *, start, end, filter_field: str = "creationDate") -> Iterable[dict]:
        """
        Синхронный генератор поверх httpx (как ожидал ваш main.py).
        """
        params = {
            "page[size]": "200",
            "filter[date][ge]": int(start.timestamp() * 1000),
            "filter[date][le]": int(end.timestamp() * 1000),
            "filter[orders][by]": filter_field,
            "include": "entries"
        }
        with httpx.Client(base_url=self.base_url, timeout=60.0) as cli:
            url = "/orders"
            while True:
                r = cli.get(url, params=params, headers=self._headers())
                r.raise_for_status()
                j = r.json()
                for it in (j.get("data") or []):
                    yield it
                lnext = j.get("links", {}).get("next")
                if not lnext: break
                url = lnext  # post-URL already absolute or relative
                params = {}
