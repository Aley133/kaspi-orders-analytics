from __future__ import annotations
from datetime import datetime, time, timedelta
from typing import Optional, Dict, Any
import pytz

from .config import settings

def parse_hhmm(s: Optional[str]) -> Optional[time]:
    if not s:
        return None
    hh, mm = s.split(":")
    return time(int(hh), int(mm))

def tz_localize(dt: datetime, tzname: str) -> datetime:
    tzinfo = pytz.timezone(tzname)
    if dt.tzinfo is None:
        return tzinfo.localize(dt)
    return dt.astimezone(tzinfo)

def extract_ms(x: Any) -> Optional[int]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return int(x)  # assume ms
    try:
        return int(datetime.fromisoformat(str(x)).timestamp()*1000)
    except Exception:
        return None

def extract_amount(order: Dict[str, Any]) -> float:
    total = 0.0
    for f in settings.AMOUNT_FIELDS:
        v = order.get(f)
        if isinstance(v, (int, float)):
            total += float(v)
    return total / max(1.0, float(settings.AMOUNT_DIVISOR))

def extract_city(order: Dict[str, Any]) -> str:
    for k in ("city","deliveryAddressCity","customerCity"):
        v = order.get(k)
        if v:
            return str(v)
    return ""

def norm_state(v: Optional[str]) -> str:
    return (v or "").upper().strip()

def parse_states_csv(s: Optional[str]) -> set:
    if not s:
        return set()
    return {norm_state(x) for x in s.split(",") if x.strip()}

def apply_hhmm(dt: datetime, start_h: Optional[time], end_h: Optional[time]) -> bool:
    if not start_h and not end_h:
        return True
    t = dt.timetz()
    if start_h and (t.hour, t.minute) < (start_h.hour, start_h.minute):
        return False
    if end_h and (t.hour, t.minute) > (end_h.hour, end_h.minute):
        return False
    return True

def cutoff_range(start: str, end: str, tzname: str, cutoff: str, lookback_days: int):
    tzinfo = pytz.timezone(tzname)
    start_dt = tzinfo.localize(datetime.fromisoformat(start))
    end_dt = tzinfo.localize(datetime.fromisoformat(end))
    start_dt = start_dt - timedelta(days=max(0, lookback_days))
    return start_dt, end_dt, parse_hhmm(cutoff)

def guess_order_number(o: Dict[str, Any]) -> str:
    for k in ("number","code","orderNumber"):
        v = o.get(k)
        if v:
            return str(v)
    attrs = o.get("attributes") or {}
    for k in ("code","number","orderNumber"):
        v = attrs.get(k)
        if v:
            return str(v)
    return str(o.get("id") or "")
