from __future__ import annotations
from typing import Dict, Optional
import re
from datetime import datetime, timedelta
import pytz
from .config import settings

STATE_MAP = {
    "CANCELED":"CANCELED","CANCELLED":"CANCELED","CANCEL":"CANCELED",
    "CANCELED_BY_SELLER":"CANCELED","CANCELED_BY_CUSTOMER":"CANCELED",
    "COMPLETED":"COMPLETED","DELIVERED":"COMPLETED",
    "KASPI_DELIVERY":"KASPI_DELIVERY","ARCHIVE":"ARCHIVE",
    "APPROVED":"APPROVED","NEW":"NEW","READY_FOR_SHIPMENT":"READY_FOR_SHIPMENT",
    "ON_DELIVERY":"ON_DELIVERY","DELIVERY":"ON_DELIVERY"
}
def norm_state(s: str) -> str:
    return STATE_MAP.get(s.strip().upper(), s.strip().upper())

def parse_states_csv(csv: Optional[str]):
    if not csv: return None
    arr = [x.strip() for x in csv.split(",") if x.strip()]
    return set(norm_state(x) for x in arr) if arr else None

def tz_localize(date_str: str, tz: str) -> datetime:
    tzinfo = pytz.timezone(tz)
    return tzinfo.localize(datetime.strptime(date_str, "%Y-%m-%d"))

def apply_hhmm(dt: datetime, hhmm: Optional[str]) -> datetime:
    if not hhmm: return dt
    hh,mm = hhmm.split(":")
    return dt.replace(hour=int(hh), minute=int(mm), second=0, microsecond=0)

def cutoff_range(day: str, tz: str, cutoff: str, lookback_days: int):
    tzinfo = pytz.timezone(tz)
    end_dt = apply_hhmm(tz_localize(day, tz), cutoff)
    start_dt = end_dt - timedelta(days=lookback_days) + timedelta(milliseconds=1)
    return start_dt, end_dt

def extract_amount(attrs: Dict) -> float:
    for k in settings.AMOUNT_FIELDS:
        if k in attrs and isinstance(attrs[k], (int, float)):
            v = float(attrs[k])
            return v / settings.AMOUNT_DIVISOR if settings.AMOUNT_DIVISOR and settings.AMOUNT_DIVISOR != 1 else v
    return 0.0

def extract_ms(attrs: Dict, field: str) -> Optional[int]:
    if field not in attrs: return None
    val = attrs[field]
    try:
        if isinstance(val, str): v = int(float(val))
        elif isinstance(val, (int,float)): v = int(val)
        else: return None
    except Exception:
        return None
    if v<10_000_000_000: v *= 1000
    return v

CITY_REGEX = re.compile(r"(?:г\.?|город)\s*([A-Za-zА-Яа-яЁё\-\s]+)")
def extract_city(attrs: Dict) -> str:
    # best-effort
    for key in ("city","deliveryAddress","address","pointOfServiceAddress","pickupPointAddress"):
        v = attrs.get(key)
        if isinstance(v, str) and v.strip():
            m = CITY_REGEX.search(v)
            return (m.group(1).strip() if m else v.split(',')[0].strip()) or "—"
    return "—"

def guess_order_number(attrs: Dict, oid: str) -> str:
    for key in ("code","orderNumber","displayOrderCode","merchantOrderId","kaspiId","idForCustomer"):
        v = attrs.get(key)
        if v: return str(v)
    return str(oid)
