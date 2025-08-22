from __future__ import annotations
from datetime import datetime
from typing import Dict, Generator, Optional, Any, List
import os
import httpx
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

DEFAULT_BASE_URL = "https://kaspi.kz/shop/api/v2"


class KaspiClient:
    def __init__(
        self,
        token: str,
        base_url: str = DEFAULT_BASE_URL,
        timeout_connect: float = 10.0,
        timeout_read: float = 20.0,
    ):
        self.base_url = (base_url or DEFAULT_BASE_URL).rstrip("/")
        self.headers = {
            "Accept": "application/vnd.api+json",
            "Content-Type": "application/vnd.api+json",
            "X-Auth-Token": token,
            "User-Agent": "kaspi-orders-service/0.4.6",
        }
        self.timeout = httpx.Timeout(
            connect=timeout_connect, read=timeout_read, write=timeout_read, pool=timeout_read
        )

    # ---------- low-level ----------
    @retry(
        wait=wait_exponential(multiplier=1, min=1, max=8),
        stop=stop_after_attempt(3),
        reraise=True,
        retry=retry_if_exception_type((httpx.RequestError, httpx.HTTPStatusError)),
    )
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

    # ---------- orders ----------
    def iter_orders(
        self,
        start: datetime,
        end: datetime,
        page_size: int = 50,
        filter_field: str = "creationDate",
        state: Optional[str] = None,
    ) -> Generator[Dict, None, None]:
        if page_size > 100:
            page_size = 100
        params: Dict[str, object] = {
            "page[number]": 0,
            "page[size]": page_size,
            f"filter[orders][{filter_field}][$ge]": self._to_ms(start),
            f"filter[orders][{filter_field}][$le]": self._to_ms(end),
        }
        if state:
            params["filter[orders][state]"] = state
        while True:
            data = self._get("orders", params)
            items = data.get("data", [])
            for it in items:
                yield it
            meta = data.get("meta", {})
            page_count = meta.get("pageCount")
            current = int(params["page[number]"])
            if page_count is not None and current + 1 >= int(page_count):
                break
            if not items:
                break
            params["page[number]"] = current + 1

    # ---------- json:api iterator ----------
    def _iter_jsonapi(self, rel_url: str, params: Optional[Dict[str, Any]] = None) -> Generator[Dict, None, None]:
        url = f"{self.base_url}/{rel_url.lstrip('/')}"
        with httpx.Client(headers=self.headers, timeout=self.timeout) as client:
            while True:
                resp = client.get(url, params=params)
                resp.raise_for_status()
                js = resp.json()
                data = js.get("data") if isinstance(js, dict) else js
                if isinstance(data, list):
                    for item in data:
                        yield item
                elif data:
                    yield data
                next_link = js.get("links", {}).get("next") if isinstance(js, dict) else None
                if not next_link:
                    break
                url = next_link
                params = None

    @staticmethod
    def _wrap_product_item(item: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(item, dict):
            return {"id": str(item), "attributes": {"raw": item}}
        attrs = item.get("attributes")
        if not isinstance(attrs, dict):
            attrs = {k: v for k, v in item.items() if k not in ("id", "type", "links", "relationships")}
        pid = (
            item.get("id")
            or attrs.get("id")
            or attrs.get("sku")
            or attrs.get("code")
            or attrs.get("offerId")
            or attrs.get("article")
        )
        return {"id": pid, "attributes": attrs}

    # ---------- products / autodetect ----------
    def _paths_for_probe(self) -> List[str]:
        env_paths = [p.strip() for p in os.getenv("KASPI_PRODUCTS_ENDPOINTS", "").split(",") if p.strip()]
        merchant_id = os.getenv("MERCHANT_ID") or os.getenv("PARTNER_ID")
        guessed = [
            "catalog/offers",
            "merchant/offers",
            "merchant/products",
            "offers",
            "merchant/offer",
            "merchant/product-cards",
        ]
        if merchant_id:
            guessed += [
                f"merchants/{merchant_id}/offers",
                f"merchants/{merchant_id}/products",
                f"merchants/{merchant_id}/product-cards",
            ]
        return env_paths + guessed

    def iter_products(self, active_only: bool = True, page_size: int = 100) -> Generator[Dict, None, None]:
        if page_size > 200:
            page_size = 200

        city_id = os.getenv("KASPI_CITY_ID")
        merchant_id = os.getenv("MERCHANT_ID") or os.getenv("PARTNER_ID")

        base_q = {"page[size]": page_size}
        active_opts = (
            [
                {"filter[products][active]": "true"},
                {"filter[offers][active]": "true"},
                {"filter[offer][active]": "true"},
                {"active": "true"},
            ]
            if active_only
            else [{}]
        )
        city_opts = [{"cityId": city_id}] if city_id else [{}]
        merch_opts = [{"merchantId": merchant_id}] if merchant_id else [{}]

        last_err: Optional[Exception] = None

        for rel in self._paths_for_probe():
            for a in active_opts:
                for c in city_opts:
                    for m in merch_opts:
                        q = {**base_q, **a, **c, **m}
                        try:
                            any_yielded = False
                            for raw in self._iter_jsonapi(rel, params=q):
                                any_yielded = True
                                yield self._wrap_product_item(raw)
                            if any_yielded:
                                return
                        except httpx.HTTPStatusError as e:
                            if e.response.status_code in (404, 403):
                                last_err = e
                                continue
                            raise
                        except Exception as e:
                            last_err = e
                            continue

        if last_err:
            raise last_err
        raise RuntimeError("No known products endpoint responded")

    # алиасы
    def iter_offers(self, active_only: bool = True, page_size: int = 100) -> Generator[Dict, None, None]:
        yield from self.iter_products(active_only=active_only, page_size=page_size)

    def iter_catalog(self, active_only: bool = True, page_size: int = 100) -> Generator[Dict, None, None]:
        yield from self.iter_products(active_only=active_only, page_size=page_size)

    # ---------- PROBE (диагностика) ----------
    def probe_catalog(self, sample_size: int = 2, active_only: bool = True) -> List[Dict[str, Any]]:
        """Перебирает пути и наборы параметров и возвращает статусы (не кидает исключения)."""
        results: List[Dict[str, Any]] = []
        city_id = os.getenv("KASPI_CITY_ID")
        merchant_id = os.getenv("MERCHANT_ID") or os.getenv("PARTNER_ID")

        base_q = {"page[size]": sample_size}
        active_opts = (
            [
                {"filter[products][active]": "true"},
                {"filter[offers][active]": "true"},
                {"filter[offer][active]": "true"},
                {"active": "true"},
            ]
            if active_only
            else [{}]
        )
        city_opts = [{"cityId": city_id}] if city_id else [{}]
        merch_opts = [{"merchantId": merchant_id}] if merchant_id else [{}]

        with httpx.Client(headers=self.headers, timeout=self.timeout) as client:
            for rel in self._paths_for_probe():
                for a in active_opts:
                    for c in city_opts:
                        for m in merch_opts:
                            q = {**base_q, **a, **c, **m}
                            url = f"{self.base_url}/{rel.lstrip('/')}"
                            try:
                                r = client.get(url, params=q)
                                ok = r.status_code == 200
                                count = 0
                                if ok:
                                    js = r.json()
                                    data = js.get("data") if isinstance(js, dict) else js
                                    if isinstance(data, list):
                                        count = len(data)
                                    elif data:
                                        count = 1
                                results.append({
                                    "url": url,
                                    "params": q,
                                    "status": r.status_code,
                                    "ok": ok,
                                    "count": count
                                })
                            except Exception as e:
                                results.append({
                                    "url": url,
                                    "params": q,
                                    "status": None,
                                    "ok": False,
                                    "error": str(e),
                                })
        # сначала успешные
        results.sort(key=lambda x: (not x["ok"], -(x.get("count") or 0)))
        return results
