from __future__ import annotations
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timedelta
import pytz
from cachetools import TTLCache
from fastapi import HTTPException
from httpx import HTTPStatusError, RequestError

from ..core.config import settings
from ..core.kaspi import KaspiClient
from ..core.utils import (
    norm_state, parse_states_csv, extract_amount, extract_ms, extract_city,
    tz_localize, apply_hhmm, cutoff_range, guess_order_number
)

client = KaspiClient(token=settings.KASPI_TOKEN)
_cache = TTLCache(maxsize=256, ttl=settings.CACHE_TTL)

def iter_chunks(start_dt: datetime, end_dt: datetime, days: int=7):
    cur = start_dt
    delta = timedelta(days=days)
    while cur <= end_dt:
        chunk_end = min(cur + delta - timedelta(milliseconds=1), end_dt)
        yield cur, chunk_end
        cur = chunk_end + timedelta(milliseconds=1)

def collect_range(*, start_dt: datetime, end_dt: datetime, tz: str, date_field: str,
                  states_inc: Optional[set], states_exc: set,
                  cutoff_mode: bool=False) -> Tuple[List[Dict], List[Dict], int, float, Dict[str,int]]:
    tzinfo = pytz.timezone(tz)
    seen = set()
    days: Dict[str,int] = {}
    amounts: Dict[str,float] = {}
    city_counts: Dict[str,int] = {}
    city_amounts: Dict[str,float] = {}
    state_counts: Dict[str,int] = {}
    total_orders=0
    total_amount=0.0

    for s,e in iter_chunks(start_dt, end_dt, 7):
        try_field = date_field
        while True:
            try:
                for order in client.iter_orders(start=s, end=e, filter_field=try_field):
                    oid = order.get("id")
                    if oid in seen: continue
                    attrs = order.get("attributes", {})
                    st = norm_state(str(attrs.get("state","")))
                    if states_inc and st not in states_inc: continue
                    if st in states_exc: continue
                    ms = extract_ms(attrs, date_field) or extract_ms(attrs, try_field)
                    if ms is None: continue
                    dtt = datetime.fromtimestamp(ms/1000.0, tz=tzinfo)
                    # bounds
                    if cutoff_mode:
                        if dtt > end_dt: continue
                    else:
                        if dtt < start_dt or dtt > end_dt: continue

                    key = dtt.date().isoformat()
                    amt = extract_amount(attrs)
                    city = extract_city(attrs)

                    days[key] = days.get(key,0)+1
                    amounts[key] = amounts.get(key,0.0)+amt
                    city_counts[city] = city_counts.get(city,0)+1
                    city_amounts[city] = city_amounts.get(city,0.0)+amt
                    state_counts[st] = state_counts.get(st,0)+1

                    seen.add(oid)
                    total_orders += 1
                    total_amount += amt
                break
            except HTTPStatusError as ee:
                if ee.response.status_code in (400,422) and try_field != "creationDate":
                    try_field = "creationDate"; continue
                raise
            except RequestError as e:
                raise HTTPException(status_code=502, detail=f"Kaspi network error: {e}")

    series = [{"x": k, "count": days.get(k,0), "amount": round(amounts.get(k,0.0),2)} for k in sorted(days.keys())]
    cities = [{"city": c, "count": city_counts[c], "amount": round(city_amounts[c],2)} for c in city_counts]
    cities.sort(key=lambda x:(-x["count"], -x["amount"], x["city"]))
    return series, cities, total_orders, round(total_amount,2), state_counts

def analytics_payload(*, start: str, end: str, tz: str, date_field: str,
                      states: Optional[str], exclude_canceled: bool,
                      end_time: Optional[str], cutoff_mode: bool, cutoff: str,
                      lookback_days: int, with_prev: bool):
    tzinfo = pytz.timezone(tz)
    states_inc = parse_states_csv(states)
    states_exc = set()
    if exclude_canceled:
        states_exc.add("CANCELED")

    if cutoff_mode:
        start_dt, end_dt = cutoff_range(end, tz, cutoff, lookback_days)
    else:
        start_dt = tz_localize(start, tz)
        end_dt = tz_localize(end, tz) + timedelta(days=1) - timedelta(milliseconds=1)
        if end_time:
            end_dt = apply_hhmm(tz_localize(end, tz), end_time)
        if end_dt < start_dt:
            raise HTTPException(status_code=400, detail="end < start")

    days, cities, tot, tot_amt, st_break = collect_range(
        start_dt=start_dt, end_dt=end_dt, tz=tz, date_field=date_field,
        states_inc=states_inc, states_exc=states_exc, cutoff_mode=cutoff_mode
    )

    prev_days = []
    if with_prev and not cutoff_mode:
        span = (end_dt.date() - start_dt.date()).days + 1
        prev_end = start_dt - timedelta(milliseconds=1)
        prev_start = prev_end - timedelta(days=span) + timedelta(milliseconds=1)
        prev_days, _, _, _, _ = collect_range(
            start_dt=prev_start, end_dt=prev_end, tz=tz, date_field=date_field,
            states_inc=states_inc, states_exc=states_exc, cutoff_mode=False
        )

    return {
        "range": {"start": start_dt.date().isoformat(), "end": end_dt.date().isoformat()},
        "timezone": tz,
        "currency": settings.CURRENCY,
        "date_field": date_field,
        "total_orders": tot,
        "total_amount": tot_amt,
        "days": days,
        "prev_days": prev_days,
        "cities": cities,
        "state_breakdown": st_break
    }

def list_numbers(*, start: str, end: str, tz: str, date_field: str,
                 states: Optional[str], exclude_canceled: bool,
                 end_time: Optional[str], cutoff_mode: bool, cutoff: str,
                 lookback_days: int):
    tzinfo = pytz.timezone(tz)
    states_inc = parse_states_csv(states)
    states_exc = set()
    if exclude_canceled: states_exc.add("CANCELED")

    if cutoff_mode:
        start_dt, end_dt = cutoff_range(end, tz, cutoff, lookback_days)
    else:
        start_dt = tz_localize(start, tz)
        end_dt = tz_localize(end, tz) + timedelta(days=1) - timedelta(milliseconds=1)
        if end_time:
            end_dt = apply_hhmm(tz_localize(end, tz), end_time)

    items = []
    seen = set()
    for s,e in iter_chunks(start_dt, end_dt, 7):
        try_field = date_field
        while True:
            try:
                for order in client.iter_orders(start=s, end=e, filter_field=try_field):
                    oid = order.get("id")
                    if oid in seen: continue
                    attrs = order.get("attributes", {})
                    st = norm_state(str(attrs.get("state","")))
                    if states_inc and st not in states_inc: continue
                    if st in states_exc: continue

                    ms = extract_ms(attrs, date_field) or extract_ms(attrs, try_field)
                    if ms is None: continue
                    dtt = datetime.fromtimestamp(ms/1000.0, tz=tzinfo)
                    if cutoff_mode:
                        if dtt > end_dt: continue
                    else:
                        if dtt < start_dt or dtt > end_dt: continue

                    items.append({
                        "id": str(oid),
                        "number": guess_order_number(attrs, oid),
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
    return items
