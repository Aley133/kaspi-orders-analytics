from __future__ import annotations
from typing import Dict, Any, List, Optional
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception
from .config import settings

BASE_URL = "https://kaspi.kz/shop/api/v2"

class KaspiError(Exception):
    pass

def build_headers() -> Dict[str, str]:
    if not settings.KASPI_TOKEN:
        raise KaspiError("KASPI_TOKEN is empty")
    return {
        "Accept": "application/vnd.api+json",
        "Content-Type": "application/vnd.api+json",
        "X-Auth-Token": settings.KASPI_TOKEN,
        "User-Agent": "kaspi-orders-service/0.5.0",
    }

def _retryable(exc: Exception) -> bool:
    if isinstance(exc, httpx.RequestError):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        sc = exc.response.status_code
        return sc == 429 or 500 <= sc < 600
    return False

@retry(reraise=True, stop=stop_after_attempt(3),
       wait=wait_exponential(multiplier=1, min=1, max=6),
       retry=retry_if_exception(_retryable))
async def _get(client: httpx.AsyncClient, url: str, params: Dict[str, Any]) -> Dict[str, Any]:
    r = await client.get(url, headers=build_headers(), params=params, timeout=httpx.Timeout(20.0, connect=10.0))
    r.raise_for_status()
    return r.json()

async def list_orders(date_field: str, start_ms: int, end_ms: int, page_size: int=100, extra_filters: Optional[Dict[str, Any]]=None) -> List[Dict[str, Any]]:
    base = f"{BASE_URL}/orders"
    items: List[Dict[str, Any]] = []
    page = 0
    param_variants = [
        {f"filter[orders][{date_field}][ge]": start_ms, f"filter[orders][{date_field}][le]": end_ms},
        {f"filter[{date_field}][ge]": start_ms, f"filter[{date_field}][le]": end_ms},
        {f"filter[{date_field}][from]": start_ms, f"filter[{date_field}][to]": end_ms},
        {"from": start_ms, "to": end_ms},
    ]
    async with httpx.AsyncClient() as client:
        while True:
            ok = False
            last_err = None
            for pv in param_variants:
                params = {"page[number]": page, "page[size]": page_size, **pv}
                if extra_filters:
                    params.update(extra_filters)
                try:
                    data = await _get(client, base, params=params)
                    ok = True
                    break
                except httpx.HTTPStatusError as e:
                    if date_field != "creationDate" and e.response.status_code in (400, 422):
                        return await list_orders("creationDate", start_ms, end_ms, page_size, extra_filters)
                    last_err = e
                except Exception as e:
                    last_err = e
            if not ok:
                if last_err:
                    raise last_err
                break

            chunk = data.get("data") or data.get("orders") or data.get("items") or []
            if not chunk:
                break
            items.extend(chunk)

            meta = data.get("meta") or {}
            page_count = meta.get("pageCount")
            if page_count is not None:
                page += 1
                if page >= int(page_count):
                    break
            else:
                if len(chunk) < page_size:
                    break
                page += 1

    seen = {}
    for it in items:
        _id = it.get("id") or it.get("orderId") or it.get("number")
        if _id is not None:
            seen[_id] = it
    return list(seen.values())
