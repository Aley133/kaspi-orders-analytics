import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception
from typing import Dict, Any, List, Optional
from .config import settings

BASE_URL = "https://kaspi.kz/shop/api/v2"

class KaspiError(Exception):
    pass

def build_headers() -> Dict[str, str]:
    if not settings.KASPI_TOKEN:
        raise KaspiError("KASPI_TOKEN is required")
    return {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "X-Auth-Token": settings.KASPI_TOKEN,
    }

def _is_retryable(exc: Exception) -> bool:
    if isinstance(exc, httpx.RequestError):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        sc = exc.response.status_code
        return sc == 429 or 500 <= sc < 600
    return False

@retry(reraise=True,
       stop=stop_after_attempt(3),
       wait=wait_exponential(multiplier=0.8, min=0.8, max=4),
       retry=retry_if_exception(_is_retryable))
async def _get(client: httpx.AsyncClient, url: str, params: Dict[str, Any]) -> Dict[str, Any]:
    resp = await client.get(url, params=params, headers=build_headers(),
                            timeout=httpx.Timeout(20.0, connect=10.0))
    resp.raise_for_status()
    return resp.json()

async def list_orders(date_field: str, start_ms: int, end_ms: int, page_size: int = 100, extra_filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    url = f"{BASE_URL}/orders"
    items: List[Dict[str, Any]] = []
    page = 0

    param_patterns = [
        {f"filter[{date_field}][ge]": start_ms, f"filter[{date_field}][le]": end_ms},
        {f"filter[{date_field}][from]": start_ms, f"filter[{date_field}][to]": end_ms},
        {"from": start_ms, "to": end_ms},
    ]

    async with httpx.AsyncClient() as client:
        while True:
            got = None
            last_exc: Optional[Exception] = None
            for pat in param_patterns:
                params = {"page[number]": page, "page[size]": page_size, **pat}
                if extra_filters:
                    params.update(extra_filters)
                try:
                    got = await _get(client, url, params=params)
                    break
                except httpx.HTTPStatusError as e:
                    if date_field != "creationDate" and e.response.status_code in (400, 422):
                        return await list_orders("creationDate", start_ms, end_ms, page_size, extra_filters)
                    last_exc = e
                except Exception as e:
                    last_exc = e
            if got is None:
                if last_exc:
                    raise last_exc
                break

            chunk = got.get("data") or got.get("orders") or got.get("items") or []
            if not chunk:
                break
            items.extend(chunk)
            if len(chunk) < page_size:
                break
            page += 1

    dedup = {}
    for it in items:
        _id = it.get("id") or it.get("orderId") or it.get("number")
        if _id is not None:
            dedup[_id] = it
    return list(dedup.values())
