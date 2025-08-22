from datetime import datetime, timedelta, time
from dateutil import tz, parser
from typing import Optional, Dict, Any
from .config import settings

def parse_hhmm(s: Optional[str]) -> Optional[time]:
    if not s:
        return None
    hh, mm = s.split(":")
    return time(int(hh), int(mm))

def get_tz(tz_name: str):
    return tz.gettz(tz_name) or tz.gettz(settings.TZ)

def date_range_with_cutoff(start: str, end: str, tz_name: str, cutoff: str, use_cutoff_window: bool, lte_cutoff_only: bool, lookback_days: int):
    tzinfo = get_tz(tz_name)
    start_dt = datetime.fromisoformat(start).replace(tzinfo=tzinfo)
    end_dt = datetime.fromisoformat(end).replace(tzinfo=tzinfo)
    cutoff_t = parse_hhmm(cutoff) or time(20, 0)

    if use_cutoff_window:
        start_dt = start_dt - timedelta(days=max(0, lookback_days))
    return start_dt, end_dt, cutoff_t

def ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)

def normalize_amount(order: Dict[str, Any]) -> float:
    total = 0
    for f in settings.amount_fields:
        v = order.get(f)
        if isinstance(v, (int, float)):
            total += v
    return float(total) / max(1, settings.AMOUNT_DIVISOR)

def pick_city(order: Dict[str, Any]) -> str:
    return order.get("city", "") or order.get("deliveryAddressCity", "") or order.get("customerCity", "") or ""

def pick_date(order: Dict[str, Any], field: str):
    v = order.get(field)
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return datetime.fromtimestamp(float(v)/1000.0, tz=get_tz(settings.TZ))
    try:
        return parser.isoparse(str(v)).astimezone(get_tz(settings.TZ))
    except Exception:
        return None
