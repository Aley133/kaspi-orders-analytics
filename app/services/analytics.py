from typing import Optional, Dict, Any, List, Tuple
from collections import defaultdict, Counter
from datetime import datetime, timedelta
from ..core.config import settings
from ..core.utils import (
    date_range_with_cutoff,
    ms, normalize_amount, pick_city, pick_date, get_tz
)
from ..core.kaspi import list_orders

def get_meta():
    return {
        "tz": settings.TZ,
        "currency": "KZT",
        "amount_fields": settings.amount_fields,
        "amount_divisor": settings.AMOUNT_DIVISOR,
        "date_field_default": settings.DATE_FIELD_DEFAULT,
        "date_field_options": settings.date_field_options,
        "cutoff": settings.DAY_CUTOFF,
        "pack_lookback_days": settings.PACK_LOOKBACK_DAYS,
    }

def _parse_csv(s: Optional[str]) -> List[str]:
    if not s:
        return []
    return [x.strip() for x in s.split(",") if x.strip()]

def _include_order(order: Dict[str, Any], include_states: List[str], exclude_states: List[str], exclude_canceled: bool) -> bool:
    st = (order.get("state") or order.get("status") or "").upper()
    if exclude_canceled and st in {"CANCELLED", "CANCELED", "CANCEL"}:
        return False
    if include_states and st not in include_states:
        return False
    if exclude_states and st in exclude_states:
        return False
    return True

def _within_cutoff_local(order_dt: datetime, cutoff_hhmm) -> bool:
    cutoff_dt = order_dt.replace(hour=cutoff_hhmm.hour, minute=cutoff_hhmm.minute, second=0, microsecond=0)
    return order_dt <= cutoff_dt

async def _fetch_orders_window(start: str, end: str, tz_name: str, date_field: str, use_cutoff_window: bool, lte_cutoff_only: bool, lookback_days: int):
    start_dt, end_dt, cutoff_t = date_range_with_cutoff(
        start, end, tz_name, settings.DAY_CUTOFF, use_cutoff_window, lte_cutoff_only, lookback_days
    )
    all_orders = await list_orders(date_field, ms(start_dt), ms(end_dt))
    return all_orders, start_dt, end_dt, cutoff_t

def _aggregate(orders: List[Dict[str, Any]], date_field: str, tz_name: str, include_states: List[str], exclude_states: List[str], exclude_canceled: bool, lte_cutoff_only: bool, cutoff_t):
    tzinfo = get_tz(tz_name)

    days = defaultdict(lambda: {"x": None, "count": 0, "amount": 0.0})
    cities = defaultdict(lambda: {"city": None, "count": 0, "amount": 0.0})
    state_counter = Counter()
    order_ids: List[Dict[str, Any]] = []

    for o in orders:
        if not _include_order(o, include_states, exclude_states, exclude_canceled):
            continue
        dt = pick_date(o, date_field)
        if not dt:
            continue
        dt = dt.astimezone(tzinfo)
        if lte_cutoff_only and not _within_cutoff_local(dt, cutoff_t):
            continue

        key_day = dt.date().isoformat()
        amt = normalize_amount(o)
        city = pick_city(o)
        st = (o.get("state") or o.get("status") or "").upper()
        days[key_day]["x"] = key_day
        days[key_day]["count"] += 1
        days[key_day]["amount"] += amt

        if city:
            cities[city]["city"] = city
            cities[city]["count"] += 1
            cities[city]["amount"] += amt

        state_counter[st] += 1

        order_ids.append({
            "id": o.get("id") or o.get("orderId") or "",
            "number": o.get("number") or "",
            "state": st,
            "date": dt.isoformat(),
            "amount": amt,
            "city": city,
        })

    days_list = sorted(list(days.values()), key=lambda r: r["x"])
    cities_list = sorted(list(cities.values()), key=lambda r: (-r["amount"], r["city"]))

    total_orders = sum(d["count"] for d in days_list)
    total_amount = sum(d["amount"] for d in days_list)

    return {
        "days": days_list,
        "cities": cities_list[:50],
        "state_breakdown": [{"state": k, "count": v} for k, v in state_counter.most_common()],
        "total_orders": total_orders,
        "total_amount": total_amount,
        "order_ids": order_ids,
    }

async def fetch_analytics(start: str, end: str, tz: str, date_field: str, states: Optional[str], exclude_states: Optional[str], exclude_canceled: bool, start_time: Optional[str], end_time: Optional[str], with_prev: bool, use_cutoff_window: bool, lte_cutoff_only: bool, lookback_days: int):
    include_states = [s.upper() for s in _parse_csv(states)]
    excl_states = [s.upper() for s in _parse_csv(exclude_states)]

    orders, start_dt, end_dt, cutoff_t = await _fetch_orders_window(
        start, end, tz, date_field, use_cutoff_window, lte_cutoff_only, lookback_days
    )
    agg = _aggregate(orders, date_field, tz, include_states, excl_states, exclude_canceled, lte_cutoff_only, cutoff_t)

    result: Dict[str, Any] = {
        **agg,
        "currency": "KZT",
        "date_field": date_field,
        "range": {"start": start_dt.isoformat(), "end": end_dt.isoformat()},
    }

    if with_prev:
        days_span = (datetime.fromisoformat(end) - datetime.fromisoformat(start)).days + 1
        prev_end = datetime.fromisoformat(start) - timedelta(days=1)
        prev_start = prev_end - timedelta(days=days_span - 1)
        orders_prev, pstart_dt, pend_dt, _ = await _fetch_orders_window(
            prev_start.date().isoformat(), prev_end.date().isoformat(), tz, date_field, use_cutoff_window, lte_cutoff_only, lookback_days
        )
        agg_prev = _aggregate(orders_prev, date_field, tz, include_states, excl_states, exclude_canceled, lte_cutoff_only, cutoff_t)
        result["prev_days"] = agg_prev["days"]

    return result

async def fetch_order_ids(start: str, end: str, tz: str, date_field: str, states: Optional[str], exclude_states: Optional[str], exclude_canceled: bool, use_cutoff_window: bool, lte_cutoff_only: bool, lookback_days: int, limit: int = 10000):
    include_states = [s.upper() for s in _parse_csv(states)]
    excl_states = [s.upper() for s in _parse_csv(exclude_states)]

    orders, start_dt, end_dt, cutoff_t = await _fetch_orders_window(
        start, end, tz, date_field, use_cutoff_window, lte_cutoff_only, lookback_days
    )
    agg = _aggregate(orders, date_field, tz, include_states, excl_states, exclude_canceled, lte_cutoff_only, cutoff_t)

    out = [{"id": i["id"], "number": i["number"], "state": i["state"], "date": i["date"], "amount": i["amount"], "city": i["city"]}
           for i in agg["order_ids"]]
    return out[:limit]
