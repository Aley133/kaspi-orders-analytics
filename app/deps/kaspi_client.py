from __future__ import annotations
from typing import Dict, Iterator, Optional, Callable
from datetime import datetime, timezone
import os, contextvars
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

# ── Контекст и прокси ────────────────────────────────────────────────────────
_current_tenant: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar("tenant_id", default=None)
_token_resolver: Optional[Callable[[Optional[str]], Optional[str]]] = None
_fallback_env_token = (os.getenv("KASPI_TOKEN") or "").strip()

def set_current_tenant(tenant_id: Optional[str]) -> None:
    _current_tenant.set(tenant_id)

def set_token_resolver(resolver: Callable[[Optional[str]], Optional[str]]) -> None:
    global _token_resolver
    _token_resolver = resolver

def token_for_current_tenant() -> Optional[str]:
    tid = _current_tenant.get()
    if _token_resolver:
        try:
            tok = _token_resolver(tid)
            if tok:
                return tok.strip()
        except Exception:
            pass
    return _fallback_env_token or None

class KaspiClientProxy:
    def __init__(self, base_url: Optional[str] = None, read_timeout: float = 80.0):
        self.base_url = (base_url or DEFAULT_BASE).rstrip("/")
        self.read_timeout = read_timeout

    def _real(self) -> KaspiClient:
        tok = token_for_current_tenant()
        if not tok:
            raise RuntimeError("Kaspi token not set for this tenant (and no fallback KASPI_TOKEN)")
        return KaspiClient(token=tok, base_url=self.base_url, read_timeout=self.read_timeout)

    # проксируем используемые методы
    def iter_orders(self, **kw):
        return self._real().iter_orders(**kw)

    async def verify_token(self) -> bool:
        return await self._real().verify_token()
