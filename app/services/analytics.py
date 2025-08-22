from __future__ import annotations
from typing import Dict, List, Optional
from datetime import datetime, timedelta
import pytz
from collections import defaultdict, Counter

from ..core.config import settings
from ..core.kaspi import list_orders
from ..core.utils import (
    norm_state, parse_states_csv, extract_amount, extract_ms, extract_city,
    tz_localize, apply_hhmm, cutoff_range, guess_order_number, parse_hhmm
)

def _within_cutoff(dt: datetime, cutoff_t) -> bool:
    if not cutoff_t:
        return True
    cutoff_dt = dt.replace(hour=cutoff_t.hour, minute=cutoff_t.minute, second=0, microsecond=0)
    return dt <= cutoff_dt

async def _collect(start: str, end: str, tz: str, date_field: str,
                   states_inc: Optional[set], states_exc: set,
                   end_time: Optional[str], cutoff_mode: bool, cutoff: str,
                   lookback_days: int) -> List[Dict]:
    tzinfo = pytz.timezone(tz)
    start_dt = tzinfo.localize(datetime.fromisoformat(start))
    end_dt = tzinfo.localize(datetime.fromisoformat(end))

    if cutoff_mode:
        fetch_start, fetch_end, cutoff_t = cutoff_range(start, end, tz, cutoff, lookback_days)
    else:
        fetch_start, fetch_end, cutoff_t = start_dt, end_dt, None

    orders = await list_orders(date_field, int(fetch_start.timestamp()*1000), int(fetch_end.timestamp()*1000))

    out: List[Dict] = []
    end_t = parse_hhmm(end_time) if end_time else None

    for o in orders:
        st = norm_state(o.get("state") or o.get("status"))
        if states_exc and st in states_exc:
            continue
        if states_inc and st not in states_inc:
            continue

        raw = o.get(date_field) or (o.get("attributes") or {}).get(date_field)
        ms = extract_ms(raw)
        if ms is None:
            continue
        dt = datetime.fromtimestamp(ms/1000.0, tz=tzinfo)

        if cutoff_t and not _within_cutoff(dt, cutoff_t):
            continue
        if end_t and not apply_hhmm(dt, None, end_t):
            continue

        out.append({
            "id": o.get("id") or o.get("orderId") or "",
            "number": guess_order_number(o),
            "state": st,
            "date": dt.isoformat(),
            "amount": extract_amount(o),
            "city": extract_city(o),
        })
    return out

def _aggregate(items: List[Dict]) -> Dict[str, any]:
    days = defaultdict(lambda: {"x": None, "count": 0, "amount": 0.0})
    cities = defaultdict(lambda: {"city": None, "count": 0, "amount": 0.0})
    state_counter = Counter()
    for it in items:
        day = it["date"][:10]
        days[day]["x"] = day
        days[day]["count"] += 1
        days[day]["amount"] += it["amount"]

        c = it.get("city") or ""
        if c:
            cities[c]["city"] = c
            cities[c]["count"] += 1
            cities[c]["amount"] += it["amount"]
        state_counter[it["state"]] += 1

    days_list = sorted(days.values(), key=lambda x: x["x"])
    cities_list = sorted(cities.values(), key=lambda r: (-r["amount"], r["city"]))

    return {
        "days": days_list,
        "cities": cities_list[:50],
        "state_breakdown": [{"state": k, "count": v} for k,v in state_counter.most_common()],
        "total_orders": sum(d["count"] for d in days_list),
        "total_amount": sum(d["amount"] for d in days_list),
    }

async def analytics_payload(start: str, end: str, tz: str, date_field: str,
                            states: Optional[str], exclude_canceled: bool,
                            end_time: Optional[str], cutoff_mode: bool,
                            cutoff: str, lookback_days: int, with_prev: bool=False) -> Dict:
    inc = parse_states_csv(states)
    exc = {"CANCELLED","CANCELED"} if exclude_canceled else set()
    items = await _collect(start, end, tz, date_field, inc if inc else None, exc, end_time, cutoff_mode, cutoff, lookback_days)
    agg = _aggregate(items)
    res = {
        **agg,
        "currency": settings.CURRENCY,
        "date_field": date_field,
        "range": {"start": start, "end": end},
    }
    if with_prev:
        s = datetime.fromisoformat(start)
        e = datetime.fromisoformat(end)
        span = (e - s).days + 1
        prev_end = (s - timedelta(days=1)).date().isoformat()
        prev_start = (s - timedelta(days=span)).date().isoformat()
        prev_items = await _collect(prev_start, prev_end, tz, date_field, inc if inc else None, exc, end_time, cutoff_mode, cutoff, lookback_days)
        res["prev_days"] = _aggregate(prev_items)["days"]
    return res

async def list_numbers(start: str, end: str, tz: str, date_field: str,
                       states: Optional[str], exclude_canceled: bool,
                       end_time: Optional[str], cutoff_mode: bool,
                       cutoff: str, lookback_days: int, limit: int=20000) -> Dict:
    inc = parse_states_csv(states)
    exc = {"CANCELLED","CANCELED"} if exclude_canceled else set()
    items = await _collect(start, end, tz, date_field, inc if inc else None, exc, end_time, cutoff_mode, cutoff, lookback_days)
    items = items[:limit]
    return {"count": len(items), "items": items}
