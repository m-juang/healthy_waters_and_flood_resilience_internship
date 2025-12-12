from __future__ import annotations
from datetime import datetime, timedelta
from typing import Optional

from dateutil import parser as date_parser

def parse_datetime(value: Optional[str]) -> Optional[datetime]:
    """
    Parse ISO-ish datetime strings safely.
    Returns None if parsing fails.
    """
    if not value:
        return None
    try:
        return date_parser.parse(value)
    except Exception:
        return None

def months_ago(reference: datetime, months: int) -> datetime:
    """
    Subtract months using 30-day approximation (consistent with your existing script).
    If you later want calendar-accurate months, we can use relativedelta.
    """
    return reference - timedelta(days=30 * months)

def iso_date(dt: Optional[datetime]) -> Optional[str]:
    """
    Return YYYY-MM-DD from datetime, or None.
    """
    if dt is None:
        return None
    return dt.date().isoformat()

def now_like(dt: datetime) -> datetime:
    """
    Produce a 'now' with same tzinfo as dt (avoid naive vs aware comparisons).
    """
    return datetime.now(dt.tzinfo)
