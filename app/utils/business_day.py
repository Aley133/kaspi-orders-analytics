
"""
Utilities for handling "business day" boundaries (e.g., 20:00–20:00).
Shifts timestamps by an offset so grouping by calendar date yields business-day buckets.

Usage pattern:
- Compute DB fetch window by subtracting offset from start/end and converting to UTC.
- For each row, compute bucket_date = (local_ts - offset).date().
"""
from __future__ import annotations

from datetime import datetime, date, time, timedelta
from typing import Tuple, Optional
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore


def parse_hhmm(hhmm: str) -> Tuple[int, int]:
    """
    Parse "HH:MM" into (hours, minutes). Raises ValueError for bad input.
    """
    hhmm = (hhmm or "").strip()
    if not hhmm or ":" not in hhmm:
        raise ValueError(f"Invalid HH:MM string: {hhmm!r}")
    h_str, m_str = hhmm.split(":", 1)
    h, m = int(h_str), int(m_str)
    if not (0 <= h <= 23 and 0 <= m <= 59):
        raise ValueError(f"Invalid time bounds: {hhmm!r}")
    return h, m


def offset_delta(hhmm: str) -> timedelta:
    h, m = parse_hhmm(hhmm)
    return timedelta(hours=h, minutes=m)


def to_local(dt_utc: datetime, tz: str) -> datetime:
    """
    Treats dt_utc as UTC and returns the localized time in tz.
    If dt_utc has tzinfo, it is respected.
    """
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=ZoneInfo("UTC"))
    return dt_utc.astimezone(ZoneInfo(tz))


def from_local_to_utc(dt_local: datetime, tz: str) -> datetime:
    if dt_local.tzinfo is None:
        dt_local = dt_local.replace(tzinfo=ZoneInfo(tz))
    return dt_local.astimezone(ZoneInfo("UTC"))


def business_bucket_date(dt_utc: datetime, business_day_start: str, tz: str) -> date:
    """
    Convert a UTC timestamp into a local business-day bucket date.
    We first convert to local tz, subtract the offset (e.g., 20:00), then take `.date()`.
    """
    delta = offset_delta(business_day_start)
    local = to_local(dt_utc, tz)
    shifted = local - delta
    return shifted.date()


def business_window_to_db_range(
    start_business_date: date,
    end_business_date_inclusive: date,
    business_day_start: str,
    tz: str,
) -> Tuple[datetime, datetime]:
    """
    Convert a business-day date range into a UTC datetime range for DB fetching.

    Example:
    - start_business_date = 2025-08-01
    - end_business_date_inclusive = 2025-08-31
    - business_day_start = "20:00"
    - tz = "Asia/Almaty"

    Returns (start_utc, end_utc_exclusive)
    """
    delta = offset_delta(business_day_start)
    tzinfo = ZoneInfo(tz)

    # Start of the first business day in local tz
    start_local = datetime.combine(start_business_date, time(0, 0), tzinfo) + delta
    # End is start of the NEXT day in local tz
    day_after_end = end_business_date_inclusive + timedelta(days=1)
    end_local_exclusive = datetime.combine(day_after_end, time(0, 0), tzinfo) + delta

    start_utc = start_local.astimezone(ZoneInfo("UTC"))
    end_utc = end_local_exclusive.astimezone(ZoneInfo("UTC"))
    return start_utc, end_utc
