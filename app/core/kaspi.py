import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from typing import Dict, Any, List, Optional
from .config import settings

BASE_URL = "https://kaspi.kz/shop/api/v2"

class KaspiError(Exception):
    pass

def _headers() -> Dict[str, str]:
    if not settings.KASPI_TOKEN:
        raise KaspiError("KASPI_TOKEN is required")
    return {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "X-Auth-Token": settings.KASPI_TOKEN,
    }

@retry(reraise=True, stop=stop_after_attempt(5), wait=wait_exponential(multiplier=0.5, min=0.5, max=4), retry=retry_if_exception_type(httpx.RequestError))
async def _get(client: httpx.AsyncClient, url: str, params: Dict[str, Any]) -> Dict[str, Any]:
    resp = await client.get(url, params=params, headers=_headers(), timeout=30.0)
    resp.raise_for_status()
    return resp.json()

async def list_orders(date_field: str, start_ms: int, end_ms: int, page_size: int = 100, extra_filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    url = f"{BASE_URL}/orders"
    items: List[Dict[str, Any]] = []
    page = 0

    params_base: Dict[str, Any] = {
        "page[number]": 0,
        "page[size]": page_size,
        f"filter[{date_field}][ge]": start_ms,
        f"filter[{date_field}][le]": end_ms,
    }
    if extra_filters:
        params_base.update(extra_filters)

    async with httpx.AsyncClient() as client:
        while True:
            params = dict(params_base, **{"page[number]": page})
            try:
                data = await _get(client, url, params=params)
            except httpx.HTTPStatusError as e:
                if date_field != "creationDate" and e.response.status_code in (400, 422):
                    return await list_orders("creationDate", start_ms, end_ms, page_size, extra_filters)
                raise

            chunk = data.get("data") or data.get("orders") or data.get("items") or []
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
