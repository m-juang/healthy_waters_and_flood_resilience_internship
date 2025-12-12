from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional

from dateutil import parser as date_parser

def parse_datetime(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return date_parser.parse(s)
    except Exception:
        return None

def months_ago(now: datetime, months: int) -> datetime:
    # Simple approximation (30 days per month)
    return now - timedelta(days=30 * months)
