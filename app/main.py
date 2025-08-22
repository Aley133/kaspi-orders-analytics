from __future__ import annotations
import os, re
from datetime import datetime, timedelta
from typing import Optional, Dict, List
import pytz
from dotenv import load_dotenv
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse, PlainTextResponse
from pydantic import BaseModel
from cachetools import TTLCache
from httpx import HTTPStatusError, RequestError
from .kaspi_client import KaspiClient

load_dotenv()
app = FastAPI(title="Kaspi Orders — Analytics (LeoXpress)", version="0.4.6")

KASPI_TOKEN = os.getenv("KASPI_TOKEN")
DEFAULT_TZ = os.getenv("TZ", "Asia/Almaty")
CACHE_TTL = int(os.getenv("CACHE_TTL", "300"))
PARTNER_ID = os.getenv("PARTNER_ID") or ""
SHOP_NAME = os.getenv("SHOP_NAME") or ""
AMOUNT_FIELDS = [s.strip() for s in os.getenv("AMOUNT_FIELDS", "totalPrice").split(",") if s.strip()]
AMOUNT_DIVISOR = float(os.getenv("AMOUNT_DIVISOR", "1"))
CURRENCY = os.getenv("CURRENCY", "KZT")
CHUNK_DAYS = int(os.getenv("CHUNK_DAYS", "7"))
DATE_FIELD_DEFAULT = os.getenv("DATE_FIELD_DEFAULT", "creationDate")
DATE_FIELD_OPTIONS = [s.strip() for s in os.getenv("DATE_FIELD_OPTIONS","creationDate,plannedDeliveryDate,plannedShipmentDate,shipmentDate,deliveryDate").split(",") if s.strip()]
CITY_KEYS = [s.strip() for s in os.getenv("CITY_KEYS","city").split(",") if s.strip()]

# Business-day settings
BUSINESS_DAY_START = os.getenv("BUSINESS_DAY_START", "20:00")  # HH:MM local
USE_BUSINESS_DAY = os.getenv("USE_BUSINESS_DAY", "true").lower() in ("1","true","yes","on")

def _bd_delta(hhmm: str):
    try:
        hh, mm = hhmm.split(":")
        return timedelta(hours=int(hh), minutes=int(mm))
    except Exception:
        raise HTTPException(status_code=400, detail=f"Неверный BUSINESS_DAY_START: {hhmm}")

if not KASPI_TOKEN: raise RuntimeError("KASPI_TOKEN не задан в окружении (.env)")
client = KaspiClient(token=KASPI_TOKEN)

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])
_cache = TTLCache(maxsize=512, ttl=CACHE_TTL)

def parse_date_local(d: str, tz_name: str) -> datetime:
    tz = pytz.timezone(tz_name)
    try: base = datetime.strptime(d, "%Y-%m-%d")
    except ValueError as e: raise HTTPException(status_code=400, detail=f"Неверный формат даты: {d}. Ожидается YYYY-MM-DD") from e
    return tz.localize(base)

def apply_hhmm(dt: datetime, hhmm: Optional[str]) -> datetime:
    if not hhmm: return dt
    try:
        hh, mm = hhmm.split(":")
        return dt.replace(hour=int(hh), minute=int(mm), second=0, microsecond=0)
    except Exception:
        raise HTTPException(status_code=400, detail=f"Неверный формат времени: {hhmm}. Ожидается HH:MM")

def extract_amount(attrs: Dict) -> float:
    for key in AMOUNT_FIELDS:
        if key in attrs and isinstance(attrs[key], (int, float)):
            v = float(attrs[key]); return v / AMOUNT_DIVISOR if AMOUNT_DIVISOR and AMOUNT_DIVISOR != 1 else v
    return 0.0

def extract_ms(attrs: Dict, field: str) -> Optional[int]:
    if field not in attrs: return None
    val = attrs[field]
    try:
        if isinstance(val, str): v = int(float(val))
        elif isinstance(val, (int,float)): v = int(val)
        else: return None
    except Exception: return None
    if v < 10_000_000_000: v *= 1000
    return v

def dict_get_path(d: Dict, path: str):
    cur = d
    for part in path.split("."):
        if isinstance(cur, dict) and part in cur: cur = cur[part]
        else: return None
    return cur

CITY_REGEX = re.compile(r"(?:г\.?|город)\s*([A-Za-zА-Яа-яЁё\-\s]+)")
def extract_city(attrs: Dict) -> str:
    for key in CITY_KEYS:
        val = dict_get_path(attrs, key)
        if isinstance(val, str) and val.strip():
            m = CITY_REGEX.search(val); return (m.group(1).strip() if m else val.split(',')[0].strip()) or "—"
    for k in ("deliveryAddress", "address", "pointOfServiceAddress", "pickupPointAddress"):
        val = attrs.get(k)
        if isinstance(val, str) and val.strip():
            m = CITY_REGEX.search(val); return (m.group(1).strip() if m else val.split(',')[0].strip()) or "—"
    return "—"

STATE_MAP = {
    "CANCELED":"CANCELED","CANCELLED":"CANCELED","CANCEL":"CANCELED",
    "CANCELED_BY_SELLER":"CANCELED","CANCELED_BY_CUSTOMER":"CANCELED",
    "COMPLETED":"COMPLETED","DELIVERED":"COMPLETED",
    "KASPI_DELIVERY":"KASPI_DELIVERY","ARCHIVE":"ARCHIVE",
    "APPROVED":"APPROVED","NEW":"NEW","READY_FOR_SHIPMENT":"READY_FOR_SHIPMENT",
}
def norm_state(s: str) -> str: return STATE_MAP.get(s.strip().upper(), s.strip().upper())

def parse_states_csv(csv: Optional[str]) -> Optional[set]:
    if not csv: return None
    arr = [x.strip() for x in csv.split(",") if x.strip()]
    return set(norm_state(x) for x in arr) if arr else None

def iter_chunks(start_dt: datetime, end_dt: datetime, days: int):
    cur = start_dt; delta = timedelta(days=days)
    while cur <= end_dt:
        chunk_end = min(cur + delta - timedelta(milliseconds=1), end_dt)
        yield cur, chunk_end
        cur = chunk_end + timedelta(milliseconds=1)

class SeriesRow(BaseModel):
    x: str; count: int; amount: float
class CityRow(BaseModel):
    city: str; count: int; amount: float
class AnalyticsResponse(BaseModel):
    range: Dict[str,str]; timezone: str; currency: str; date_field: str
    total_orders: int; total_amount: float
    days: List[SeriesRow]; prev_days: List[SeriesRow]
    cities: List[CityRow]; state_breakdown: Dict[str,int]

@app.get("/meta")
async def meta():
    return {"shop": SHOP_NAME, "partner_id": PARTNER_ID, "timezone": DEFAULT_TZ, "currency": CURRENCY,
            "amount_fields": AMOUNT_FIELDS, "divisor": AMOUNT_DIVISOR, "chunk_days": CHUNK_DAYS,
            "date_field_default": DATE_FIELD_DEFAULT, "date_field_options": DATE_FIELD_OPTIONS,
            "city_keys": CITY_KEYS, "use_business_day": USE_BUSINESS_DAY, "business_day_start": BUSINESS_DAY_START}

def _collect_range(start_dt: datetime, end_dt: datetime, tz: str, date_field: str, states_inc: Optional[set], states_ex: set, eff_use_bd: bool, eff_bds: str):
    tzinfo = pytz.timezone(tz)

    seen_ids = set()
    day_counts: Dict[str,int] = {}; day_amounts: Dict[str,float] = {}
    city_counts: Dict[str,int] = {}; city_amounts: Dict[str,float] = {}
    state_counts: Dict[str,int] = {}
    total_orders=0; total_amount=0.0

    for s,e in iter_chunks(start_dt, end_dt, CHUNK_DAYS):
        try:
            try_field = date_field
            while True:
                try:
                    for order in client.iter_orders(start=s, end=e, filter_field=try_field):
                        oid = order.get("id")
                        if oid in seen_ids: continue
                        attrs = order.get("attributes", {})
                        st = norm_state(str(attrs.get("state","")))
                        if states_inc and st not in states_inc: continue
                        if st in states_ex: continue

                        ms = extract_ms(attrs, date_field) or extract_ms(attrs, try_field)
                        if ms is None: continue
                        dtt = datetime.fromtimestamp(ms/1000.0, tz=tzinfo)
                        if dtt < start_dt.astimezone(tzinfo) or dtt > end_dt.astimezone(tzinfo):
                            continue

                        day_key = ((dtt + (timedelta(hours=24) - _bd_delta(eff_bds))) if eff_use_bd else dtt).date().isoformat()
                        amt = extract_amount(attrs)
                        city = extract_city(attrs)

                        day_counts[day_key] = day_counts.get(day_key,0)+1
                        day_amounts[day_key] = day_amounts.get(day_key,0.0)+amt
                        city_counts[city] = city_counts.get(city,0)+1
                        city_amounts[city] = city_amounts.get(city,0.0)+amt
                        state_counts[st] = state_counts.get(st,0)+1

                        seen_ids.add(oid)
                        total_orders += 1; total_amount += amt
                    break
                except HTTPStatusError as ee:
                    if ee.response.status_code in (400, 422) and try_field != "creationDate":
                        try_field = "creationDate"
                        continue
                    raise
        except RequestError as e:
            raise HTTPException(status_code=502, detail=f"Network error: {e}")

    curd = start_dt.date(); last = end_dt.date(); series = []
    while curd <= last:
        k = curd.isoformat()
        series.append({"x": k, "count": day_counts.get(k,0), "amount": round(day_amounts.get(k,0.0),2)})
        curd += timedelta(days=1)

    all_c = [{"city": c, "count": city_counts.get(c,0), "amount": round(city_amounts.get(c,0.0),2)} for c in city_counts]
    all_c.sort(key=lambda x: (-x["count"], -x["amount"], x["city"])); cities = all_c[:10]

    return series, cities, total_orders, round(total_amount,2), state_counts

@app.get("/orders/analytics", response_model=AnalyticsResponse)
async def analytics(start: str = Query(...), end: str = Query(...), tz: str = Query(DEFAULT_TZ),
                    date_field: str = Query(DATE_FIELD_DEFAULT),
                    states: Optional[str] = Query(None), exclude_states: Optional[str] = Query(None),
                    with_prev: bool = Query(True), exclude_canceled: bool = Query(True),
                    start_time: Optional[str] = Query(None), end_time: Optional[str] = Query(None),
                    use_bd: Optional[bool] = Query(None), business_day_start: Optional[str] = Query(None)):
    tzinfo = pytz.timezone(tz)
    # Effective BD settings: request override > env default
    eff_use_bd = USE_BUSINESS_DAY if use_bd is None else bool(use_bd)
    eff_bds = BUSINESS_DAY_START if not business_day_start else business_day_start

    
start_dt = parse_date_local(start, tz)
end_dt = parse_date_local(end, tz) + timedelta(days=1) - timedelta(milliseconds=1)

if eff_use_bd:
    delta = _bd_delta(eff_bds)
    # Окно для дня с меткой D: [D-1 00:00+delta .. D 00:00+delta)
    start_dt = tzinfo.localize(datetime.combine((start_dt.date() - timedelta(days=1)), datetime.min.time())) + delta
    end_dt = tzinfo.localize(datetime.combine(end_dt.date(), datetime.min.time())) + delta - timedelta(milliseconds=1)
else:
    if start_time: start_dt = apply_hhmm(start_dt, start_time)
    if end_time:
        e0 = parse_date_local(end, tz)
        end_dt = apply_hhmm(e0, end_time)
        end_dt = end_dt.replace(tzinfo=tzinfo)

    if end_dt < start_dt: raise HTTPException(status_code=400, detail="end < start")

    inc = parse_states_csv(states)
    exc = parse_states_csv(exclude_states) or set()
    if exclude_canceled: exc |= {"CANCELED"}

    days, cities, tot, tot_amt, st_counts = _collect_range(start_dt, end_dt, tz, date_field, inc, exc, eff_use_bd, eff_bds)

    prev_days = []
    if with_prev:
        span_days = (end_dt.date() - start_dt.date()).days + 1
        prev_end = start_dt - timedelta(milliseconds=1)
        prev_start = prev_end - timedelta(days=span_days) + timedelta(milliseconds=1)
        prev_days, _, _, _, _ = _collect_range(prev_start, prev_end, tz, date_field, inc, exc, eff_use_bd, eff_bds)

    return {"range":{"start":start_dt.astimezone(tzinfo).date().isoformat(),"end":end_dt.astimezone(tzinfo).date().isoformat()},
            "timezone": tz, "currency": CURRENCY, "date_field": date_field,
            "total_orders": tot, "total_amount": tot_amt, "days": days, "prev_days": prev_days, "cities": cities, "state_breakdown": st_counts}

def _guess_number(attrs: Dict, oid: str) -> str:
    for key in ("code","orderNumber","displayOrderCode","merchantOrderId","kaspiId","idForCustomer"):
        v = attrs.get(key)
        if v: return str(v)
    return str(oid)

@app.get("/orders/ids")
async def list_ids(start: str, end: str, tz: str = DEFAULT_TZ, date_field: str = DATE_FIELD_DEFAULT,
                   states: Optional[str] = None, exclude_canceled: bool = True,
                   end_time: Optional[str] = None):
    tzinfo = pytz.timezone(tz)
    start_dt = parse_date_local(start, tz)
    end_dt = parse_date_local(end, tz) + timedelta(days=1) - timedelta(milliseconds=1)
    if end_time:
        e0 = parse_date_local(end, tz)
        end_dt = apply_hhmm(e0, end_time).replace(tzinfo=tzinfo)

    inc = parse_states_csv(states)
    exc = set()
    if exclude_canceled: exc.add("CANCELED")

    seen = set(); items = []
    for s,e in iter_chunks(start_dt, end_dt, CHUNK_DAYS):
        try_field = date_field
        while True:
            try:
                for order in client.iter_orders(start=s, end=e, filter_field=try_field):
                    oid = order.get("id")
                    if oid in seen: continue
                    attrs = order.get("attributes",{})
                    st = norm_state(str(attrs.get("state","")))
                    if inc and st not in inc: continue
                    if st in exc: continue

                    ms = (attrs.get(date_field) if date_field in attrs else attrs.get(try_field))
                    if ms is None:
                        continue
                    ms = int(ms) if isinstance(ms, (int,float,str)) else None
                    if ms and ms < 10_000_000_000: ms *= 1000
                    if not ms: continue
                    dtt = datetime.fromtimestamp(ms/1000.0, tz=tzinfo)
                    if dtt < start_dt.astimezone(tzinfo) or dtt > end_dt.astimezone(tzinfo):
                        continue

                    num = _guess_number(attrs, oid)
                    items.append({
                        "id": str(oid),
                        "number": num,
                        "state": st,
                        "date": dtt.isoformat(),
                        "amount": extract_amount(attrs),
                        "city": extract_city(attrs),
                    })
                    seen.add(oid)
                break
            except HTTPStatusError as ee:
                if ee.response.status_code in (400,422) and try_field != "creationDate":
                    try_field = "creationDate"; continue
                raise
    items.sort(key=lambda x: x["number"])
    return {"count": len(items), "items": items}

@app.get("/orders/ids.csv")
async def list_ids_csv(start: str, end: str, tz: str = DEFAULT_TZ, date_field: str = DATE_FIELD_DEFAULT,
                       states: Optional[str] = None, exclude_canceled: bool = True,
                       end_time: Optional[str] = None):
    data = await list_ids(start, end, tz, date_field, states, exclude_canceled, end_time)  # type: ignore
    output = io.StringIO()
    w = csv.writer(output, lineterminator="\n")
    w.writerow(["number","state","date","amount","city","id"])
    for it in data["items"]:
        w.writerow([it["number"], it["state"], it["date"], it["amount"], it["city"], it["id"]])
    return PlainTextResponse(content=output.getvalue(), media_type="text/csv; charset=utf-8")

@app.get("/orders/debug")
async def debug_list(start: str, end: str, tz: str = DEFAULT_TZ, date_field: str = DATE_FIELD_DEFAULT,
                     states: Optional[str] = None, limit: int = 100):
    tzinfo = pytz.timezone(tz)
    start_dt = parse_date_local(start, tz)
    end_dt = parse_date_local(end, tz) + timedelta(days=1) - timedelta(milliseconds=1)
    inc = parse_states_csv(states)
    out = []
    for s,e in iter_chunks(start_dt, end_dt, CHUNK_DAYS):
        try:
            try_field = date_field
            while True:
                try:
                    for order in client.iter_orders(start=s,end=e,filter_field=try_field):
                        attrs = order.get("attributes", {})
                        st = norm_state(str(attrs.get("state","")))
                        if inc and st not in inc: continue
                        ms = (attrs.get(date_field) if date_field in attrs else attrs.get(try_field))
                        xdate = None
                        if ms is not None:
                            ms = int(ms) if isinstance(ms, (int,float,str)) else None
                            if ms and ms < 10_000_000_000: ms *= 1000
                            if ms: xdate = datetime.fromtimestamp(ms/1000.0, tz=tzinfo).isoformat()
                        out.append({"id": order.get("id"), "state": st, "date": xdate, "number": _guess_number(attrs, order.get("id"))})
                        if len(out)>=limit: return {"count": len(out), "sample": out}
                    break
                except HTTPStatusError as ee:
                    if ee.response.status_code in (400,422) and try_field != "creationDate":
                        try_field = "creationDate"; continue
                    raise
        except RequestError as e:
            raise HTTPException(status_code=502, detail=f"Network: {e}")
    return {"count": len(out), "sample": out}

@app.get("/", include_in_schema=False)
async def root(): return RedirectResponse(url="/ui/")

app.mount("/ui", StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static"), html=True), name="static")