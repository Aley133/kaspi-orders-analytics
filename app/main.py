from __future__ import annotations

"""
ЧИСТАЯ ВЕРСИЯ API по заказам Kaspi.
Главные правила фильтрации:
1) Источник отбора ВСЕГДА — строгое временное окно по выбранному полю date_field
   (start <= chosen_field_datetime <= end), с поддержкой start_time/end_time (HH:MM).
2) Группировка по «дню» (op_day) — отдельно настраивается через assign_mode:
   - raw: календарный день выбранного поля
   - accept: календарный день с учётом cut-off store_accept_until (для creationDate/date)
   - business: бизнес-день (20:00→20:00 по умолчанию) для shipment/delivery
   - smart (по умолч.): выбирает режим по выбранному полю (см. _assign_op_day)
3) Фильтры по статусам: states (IN), exclude_states (NOT IN). Поддерживает русские названия.
4) НИЧЕГО не «перекидываем» на следующий день автоматически, кроме как для op_day (группировок).
   Само включение/исключение заказа в список происходит ТОЛЬКО по строгому окну времени выбранного поля.

Эндпойнты:
- GET /orders/ids       — плоский список, можно grouped=1 для группировки по дням
- GET /orders/ids.csv   — CSV-список номеров
- GET /orders/analytics — агрегаты по дням (count/amount) + разбиение по городам/статусам
"""

import os
import re
import uuid
import asyncio
from datetime import datetime, timedelta, time
from typing import Optional, Dict, List, Iterable, Tuple, Callable

import pytz
import httpx
from httpx import HTTPStatusError, RequestError
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse, JSONResponse, RedirectResponse
from pydantic import BaseModel

# === Tenant-aware Kaspi client (ожидается, что у вас уже есть этот модуль) ===
from app.deps.auth import attach_kaspi_token_middleware, get_current_kaspi_token
from app.deps.kaspi_client_tenant import KaspiClient as TenantKaspiClient

# ======================== Конфиг ========================
DEFAULT_TZ        = os.getenv("TZ", "Asia/Almaty")
KASPI_BASE_URL    = os.getenv("KASPI_BASE_URL", "https://kaspi.kz/shop/api/v2").rstrip("/")
CURRENCY          = os.getenv("CURRENCY", "KZT")
BUSINESS_DAY_START= os.getenv("BUSINESS_DAY_START", "20:00")  # HH:MM
STORE_ACCEPT_UNTIL= os.getenv("STORE_ACCEPT_UNTIL", "17:00")  # HH:MM
CHUNK_DAYS        = int(os.getenv("CHUNK_DAYS", "7") or 7)     # шаг пагинации по датам
AMOUNT_FIELDS     = [s.strip() for s in os.getenv("AMOUNT_FIELDS", "totalPrice").split(",") if s.strip()]
AMOUNT_DIVISOR    = float(os.getenv("AMOUNT_DIVISOR", "1") or 1)

# Какие поля считаем «полем приёма» и «логистическими полями»
ACCEPT_FIELDS   = {"creationDate", "date"}
BUSINESS_FIELDS = {"shipmentDate", "deliveryDate", "plannedDeliveryDate"}

# Маппинг статусов на коды
RU_STATUS_MAP = {
    "НОВЫЙ": "NEW",
    "ОПЛАТА ПОДТВЕРЖДЕНА": "APPROVED_BY_BANK",
    "ПРИНЯТ МАГАЗИНОМ": "ACCEPTED_BY_MERCHANT",
    "ГОТОВ К ОТГРУЗКЕ": "READY_FOR_SHIPMENT",
    "KASPI ДОСТАВКА": "KASPI_DELIVERY",
    "KASPI ДОСТАВКА (ПЕРЕДАН)": "KASPI_DELIVERY",
    "ДОСТАВЛЕН": "DELIVERED",
    "ЗАВЕРШЁН (АРХИВ)": "ARCHIVE",
    "ЗАВЕРШЕН (АРХИВ)": "ARCHIVE",
    "АРХИВ (ИСТОРИЯ)": "ARCHIVED",
    "ВОЗВРАТ": "RETURNED",
    "ОТМЕНЁН": "CANCELED",
    "ОТМЕНЕН": "CANCELED",
}

# ======================== Утилиты ========================
app = FastAPI(title="Kaspi Orders — clean")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
app.middleware("http")(attach_kaspi_token_middleware)
client = TenantKaspiClient(base_url=KASPI_BASE_URL)


def tzinfo_of(name: str) -> pytz.BaseTzInfo:
    try:
        return pytz.timezone(name)
    except Exception:
        raise HTTPException(status_code=400, detail=f"Bad timezone: {name}")


def parse_date_local(d: str, tz: str) -> datetime:
    tzinfo = tzinfo_of(tz)
    y, m, dd = map(int, d.split("-"))
    return tzinfo.localize(datetime(y, m, dd, 0, 0, 0, 0))


def apply_hhmm(dt_local: datetime, hhmm: Optional[str]) -> datetime:
    if not hhmm:
        return dt_local
    hh, mm = map(int, hhmm.split(":"))
    return dt_local.replace(hour=hh, minute=mm, second=0, microsecond=0)


def _bd_delta(hhmm: str) -> timedelta:
    h, m = map(int, hhmm.split(":"))
    return timedelta(hours=h, minutes=m)


def iter_chunks(start_dt: datetime, end_dt: datetime, step_days: int) -> Iterable[Tuple[datetime, datetime]]:
    cur = start_dt
    while cur <= end_dt:
        nxt = min(cur + timedelta(days=step_days) - timedelta(milliseconds=1), end_dt)
        yield cur, nxt
        cur = nxt + timedelta(milliseconds=1)


def norm_state(s: str) -> str:
    return (s or "").strip().upper()


def parse_states_csv(s: Optional[str]) -> Optional[set[str]]:
    if not s:
        return None
    out: set[str] = set()
    for raw in re.split(r"[\s,;]+", s):
        raw = raw.strip()
        if not raw:
            continue
        code = RU_STATUS_MAP.get(raw.upper()) or raw.upper()
        out.add(code)
    return out


def extract_ms(attrs: dict, field: str) -> Optional[int]:
    v = attrs.get(field)
    if v is None and field == "creationDate":
        v = attrs.get("date")  # алиас
    if v is None:
        return None
    # целые миллисекунды или ISO
    if isinstance(v, (int, float)):
        return int(v)
    try:
        return int(datetime.fromisoformat(str(v).replace("Z", "+00:00")).timestamp() * 1000)
    except Exception:
        return None


def extract_amount(attrs: dict) -> float:
    total = 0.0
    for k in AMOUNT_FIELDS:
        v = attrs
        for key in k.split("."):
            if not isinstance(v, dict) or key not in v:
                v = None
                break
            v = v[key]
        if v is None:
            continue
        try:
            total += float(v)
        except Exception:
            pass
    return round(total / (AMOUNT_DIVISOR or 1.0), 2)


def extract_city(attrs: dict) -> str:
    # Находим первое строковое поле, похожее на город
    def _deep(o):
        if isinstance(o, dict):
            for k, v in o.items():
                kl = str(k).lower()
                if any(h in kl for h in ("city", "locality", "town")) and isinstance(v, str) and v.strip():
                    s = re.sub(r"^\s*(г\.?|город)\s+", "", v, flags=re.IGNORECASE)
                    return s.split(",")[0].strip()
                r = _deep(v)
                if r:
                    return r
        if isinstance(o, list):
            for it in o:
                r = _deep(it)
                if r:
                    return r
        return ""
    return _deep(attrs)


def _assign_op_day(attrs: dict, tzinfo: pytz.BaseTzInfo, *,
                   date_field: str, assign_mode: str,
                   store_accept_until: str, business_day_start: str) -> str:
    """Возвращает строку YYYY-MM-DD для группировки (op_day)."""
    # базовое время — выбранное поле, либо creationDate
    ms = extract_ms(attrs, date_field) or extract_ms(attrs, "creationDate")
    base = datetime.fromtimestamp(ms/1000, tz=pytz.UTC).astimezone(tzinfo) if ms else None

    if assign_mode == "raw" or not base:
        return (base or datetime.now(tzinfo)).date().isoformat()

    if assign_mode == "accept" or (assign_mode == "smart" and date_field in ACCEPT_FIELDS):
        cutoff_h, cutoff_m = map(int, store_accept_until.split(":"))
        cutoff = time(cutoff_h, cutoff_m)
        if base.time() <= cutoff:
            return base.date().isoformat()
        return (base + timedelta(days=1)).date().isoformat()

    if assign_mode == "business" or (assign_mode == "smart" and date_field in BUSINESS_FIELDS):
        shift = timedelta(hours=24) - _bd_delta(business_day_start)
        return (base + shift).date().isoformat()

    # smart + plannedShipmentDate → сам день плана
    if assign_mode == "smart" and date_field == "plannedShipmentDate":
        ms_planned = extract_ms(attrs, "plannedShipmentDate")
        if ms_planned:
            d = datetime.fromtimestamp(ms_planned/1000, tz=pytz.UTC).astimezone(tzinfo)
            return d.date().isoformat()

    return base.date().isoformat()


# ======================== Модели ========================
class ItemOut(BaseModel):
    id: str
    number: str
    state: str
    date: str      # ISO datetime выбранного поля
    op_day: str    # день для группировки
    amount: float = 0.0
    city: str = ""
    sku: Optional[str] = None
    title: Optional[str] = None


class GroupOut(BaseModel):
    day: str
    items: List[ItemOut]
    total_amount: float


class AnalyticsResponse(BaseModel):
    range: Dict[str, str]
    timezone: str
    currency: str
    date_field: str
    total_orders: int
    total_amount: float
    days: List[Dict[str, object]]
    cities: List[Dict[str, object]]
    state_breakdown: Dict[str, int]


# ======================== Внутреннее ядро ========================
async def _first_item_details(order_id: str) -> Optional[Dict[str, str]]:
    token = get_current_kaspi_token()
    if not token:
        return None
    headers = {
        "X-Auth-Token": token,
        "Accept": "application/vnd.api+json",
        "Content-Type": "application/vnd.api+json",
        "User-Agent": "leo-analytics/clean",
    }
    async with httpx.AsyncClient(base_url=KASPI_BASE_URL, timeout=60.0) as cli:
        try:
            r = await cli.get("/orderentries", params={"filter[order.id]": order_id, "page[size]": "200"}, headers=headers)
            r.raise_for_status()
            data = (r.json().get("data") or [])
            if data:
                attrs = data[0].get("attributes", {}) or {}
                title = attrs.get("offerName") or attrs.get("title") or attrs.get("name")
                sku = (attrs.get("offer") or {}).get("code") or attrs.get("sku") or attrs.get("code")
                return {"title": (title or "").strip(), "sku": (sku or "").strip()}
        except Exception:
            pass
        try:
            r = await cli.get(f"/orders/{order_id}/entries", params={"page[size]": "200"}, headers=headers)
            r.raise_for_status()
            data = (r.json().get("data") or [])
            if data:
                attrs = data[0].get("attributes", {}) or {}
                title = attrs.get("offerName") or attrs.get("title") or attrs.get("name")
                sku = (attrs.get("offer") or {}).get("code") or attrs.get("sku") or attrs.get("code")
                return {"title": (title or "").strip(), "sku": (sku or "").strip()}
        except Exception:
            pass
    return None


def _guess_number(attrs: dict, fallback_id: str) -> str:
    for k in ("number", "code", "orderNumber"):
        v = attrs.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
        if isinstance(v, (int, float)):
            return str(v)
    return fallback_id


async def _scan_orders(*,
                       start_dt: datetime, end_dt: datetime, tz: str, date_field: str,
                       states_inc: Optional[set[str]], states_ex: set[str],
                       assign_mode: str, store_accept_until: str, business_day_start: str,
                       enrich: bool, grouped: bool, order: str,
                       limit: int) -> Dict[str, object]:
    tzinfo = tzinfo_of(tz)

    seen: set[str] = set()
    items: List[ItemOut] = []

    # Пробуем фильтровать на стороне Kaspi выбранным полем, при ошибке — creationDate,
    # но отбор всё равно делаем строго по выбранному полю на нашей стороне.
    def _try_fields() -> List[str]:
        return [date_field, "creationDate"] if date_field != "creationDate" else ["creationDate"]

    # Скан по кускам
    for s, e in iter_chunks(start_dt, end_dt, CHUNK_DAYS):
        field_for_server = None
        for f in _try_fields():
            try:
                field_for_server = f
                for order in client.iter_orders(start=s, end=e, filter_field=f):
                    oid = str(order.get("id"))
                    if oid in seen:
                        continue
                    attrs = order.get("attributes", {}) or {}
                    st = norm_state(attrs.get("state", ""))
                    if states_inc and st not in states_inc:
                        continue
                    if st in states_ex:
                        continue

                    # Выбранное поле → время для строгого окна
                    ms = extract_ms(attrs, date_field) or extract_ms(attrs, "creationDate")
                    if not ms:
                        continue
                    dtt = datetime.fromtimestamp(ms/1000, tz=pytz.UTC).astimezone(tzinfo)
                    if dtt < start_dt or dtt > end_dt:
                        continue

                    item = ItemOut(
                        id=oid,
                        number=_guess_number(attrs, oid),
                        state=st,
                        date=dtt.isoformat(),
                        op_day=_assign_op_day(attrs, tzinfo,
                                              date_field=date_field, assign_mode=assign_mode,
                                              store_accept_until=store_accept_until,
                                              business_day_start=business_day_start),
                        amount=extract_amount(attrs),
                        city=extract_city(attrs),
                    )
                    items.append(item)
                    seen.add(oid)
                break  # успешно сходили выбранным полем — не нужно повторять
            except HTTPStatusError as e:
                if e.response.status_code in (400, 422):
                    continue  # пробуем fallback поле
                raise
            except RequestError as e:
                raise HTTPException(status_code=502, detail=f"Network error: {e}")

    # сортировка и лимит
    items.sort(key=lambda it: (it.op_day, it.date), reverse=(order == "desc"))
    if limit and limit > 0:
        items = items[:limit]

    # обогащение (SKU/Title) — без 401 если нет токена
    if enrich and items:
        targets = items if limit == 0 else items[:limit]
        sem = asyncio.Semaphore(6)
        async def _enrich(it: ItemOut):
            async with sem:
                extra = await _first_item_details(it.id)
                if extra:
                    it.sku = extra.get("sku") or it.sku
                    it.title = extra.get("title") or it.title
                await asyncio.sleep(0.01)
        await asyncio.gather(*(_enrich(it) for it in targets))

    # группировка для UI
    groups: List[GroupOut] = []
    if grouped:
        bucket: List[ItemOut] = []
        cur = None
        for it in items:
            if cur is None:
                cur = it.op_day
            if it.op_day != cur:
                groups.append(GroupOut(day=cur, items=bucket, total_amount=round(sum(x.amount for x in bucket), 2)))
                bucket = []
                cur = it.op_day
            bucket.append(it)
        if cur is not None:
            groups.append(GroupOut(day=cur, items=bucket, total_amount=round(sum(x.amount for x in bucket), 2)))

    return {
        "items": [it.dict() for it in items],
        "groups": [g.dict() for g in groups],
        "period_total_count": len(items),
        "period_total_amount": round(sum(it.amount for it in items), 2),
        "currency": CURRENCY,
    }


# ======================== Эндпойнты ========================
@app.get("/orders/ids")
async def list_ids(
    start: str = Query(..., description="YYYY-MM-DD"),
    end: str = Query(..., description="YYYY-MM-DD"),
    tz: str = Query(DEFAULT_TZ),
    date_field: str = Query("creationDate"),
    start_time: Optional[str] = Query(None, pattern=r"^\d{2}:\d{2}$"),
    end_time: Optional[str] = Query(None, pattern=r"^\d{2}:\d{2}$"),
    states: Optional[str] = Query(None, description="CSV of states (codes or RU)"),
    exclude_states: Optional[str] = Query(None),
    assign_mode: str = Query("smart", pattern="^(raw|accept|business|smart)$"),
    business_day_start: str = Query(BUSINESS_DAY_START),
    store_accept_until: str = Query(STORE_ACCEPT_UNTIL),
    order: str = Query("asc", pattern="^(asc|desc)$"),
    grouped: int = Query(1),
    enrich: int = Query(1),
    limit: int = Query(0),
):
    tzinfo = tzinfo_of(tz)
    start_dt = apply_hhmm(parse_date_local(start, tz), start_time)
    # end — до указанного времени включительно
    end_dt = apply_hhmm(parse_date_local(end, tz), end_time) if end_time else (parse_date_local(end, tz) + timedelta(days=1) - timedelta(milliseconds=1))
    if end_dt < start_dt:
        raise HTTPException(status_code=400, detail="end < start")

    inc = parse_states_csv(states)
    exc = parse_states_csv(exclude_states) or set()

    data = await _scan_orders(
        start_dt=start_dt, end_dt=end_dt, tz=tz, date_field=date_field,
        states_inc=inc, states_ex=exc,
        assign_mode=assign_mode, store_accept_until=store_accept_until, business_day_start=business_day_start,
        enrich=bool(enrich), grouped=bool(grouped), order=order, limit=limit,
    )
    return JSONResponse(data)


@app.get("/orders/ids.csv", response_class=PlainTextResponse)
async def list_ids_csv(
    start: str = Query(...),
    end: str = Query(...),
    tz: str = Query(DEFAULT_TZ),
    date_field: str = Query("creationDate"),
    start_time: Optional[str] = Query(None),
    end_time: Optional[str] = Query(None),
    states: Optional[str] = Query(None),
    exclude_states: Optional[str] = Query(None),
    assign_mode: str = Query("smart"),
    business_day_start: str = Query(BUSINESS_DAY_START),
    store_accept_until: str = Query(STORE_ACCEPT_UNTIL),
    order: str = Query("asc"),
    limit: int = Query(0),
):
    # берём плоский список без групп и без обогащения
    data = await list_ids(
        start=start, end=end, tz=tz, date_field=date_field,
        start_time=start_time, end_time=end_time,
        states=states, exclude_states=exclude_states,
        assign_mode=assign_mode, business_day_start=business_day_start, store_accept_until=store_accept_until,
        order=order, grouped=0, enrich=0, limit=limit,
    )
    # data — Response; вытащим JSON
    payload = data.body if hasattr(data, "body") else None
    import json as _json
    j = _json.loads(payload or b"{}")
    return "\n".join(str(it.get("number")) for it in j.get("items", []))


@app.get("/orders/analytics")
async def analytics(
    start: str = Query(...),
    end: str = Query(...),
    tz: str = Query(DEFAULT_TZ),
    date_field: str = Query("creationDate"),
    start_time: Optional[str] = Query(None),
    end_time: Optional[str] = Query(None),
    states: Optional[str] = Query(None),
    exclude_states: Optional[str] = Query(None),
    assign_mode: str = Query("smart"),
    business_day_start: str = Query(BUSINESS_DAY_START),
    store_accept_until: str = Query(STORE_ACCEPT_UNTIL),
):
    tzinfo = tzinfo_of(tz)
    start_dt = apply_hhmm(parse_date_local(start, tz), start_time)
    end_dt = apply_hhmm(parse_date_local(end, tz), end_time) if end_time else (parse_date_local(end, tz) + timedelta(days=1) - timedelta(milliseconds=1))
    if end_dt < start_dt:
        raise HTTPException(status_code=400, detail="end < start")

    inc = parse_states_csv(states)
    exc = parse_states_csv(exclude_states) or set()

    data = await _scan_orders(
        start_dt=start_dt, end_dt=end_dt, tz=tz, date_field=date_field,
        states_inc=inc, states_ex=exc,
        assign_mode=assign_mode, store_accept_until=store_accept_until, business_day_start=business_day_start,
        enrich=False, grouped=False, order="asc", limit=0,
    )

    # посуточные агрегаты по op_day
    days: Dict[str, Dict[str, float]] = {}
    cities: Dict[str, int] = {}
    state_breakdown: Dict[str, int] = {}
    total_amount = 0.0
    for it in data["items"]:
        day = it["op_day"]
        days.setdefault(day, {"count": 0, "amount": 0.0})
        days[day]["count"] += 1
        days[day]["amount"] += float(it["amount"] or 0)
        if it.get("city"):
            cities[it["city"]] = cities.get(it["city"], 0) + 1
        st = it.get("state", "")
        state_breakdown[st] = state_breakdown.get(st, 0) + 1
        total_amount += float(it["amount"] or 0)

    # заполняем пустые дни между start и end (по локальному времени)
    out_days: List[Dict[str, object]] = []
    cur = start_dt.astimezone(tzinfo).date()
    end_date = end_dt.astimezone(tzinfo).date()
    while cur <= end_date:
        key = cur.isoformat()
        d = days.get(key, {"count": 0, "amount": 0.0})
        out_days.append({"x": key, "count": int(d["count"]), "amount": round(d["amount"], 2)})
        cur += timedelta(days=1)

    return JSONResponse({
        "range": {"start": start_dt.astimezone(tzinfo).date().isoformat(),
                   "end": end_dt.astimezone(tzinfo).date().isoformat()},
        "timezone": tz,
        "currency": CURRENCY,
        "date_field": date_field,
        "total_orders": int(data["period_total_count"]),
        "total_amount": round(total_amount, 2),
        "days": out_days,
        "cities": [{"city": k, "count": v} for k, v in sorted(cities.items(), key=lambda x: -x[1])],
        "state_breakdown": state_breakdown,
    })


@app.get("/", include_in_schema=False)
async def root():
    return RedirectResponse(url="/docs")
