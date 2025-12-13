from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import pandas as pd
from dateutil import parser as date_parser


def parse_datetime(value: Any) -> Optional[datetime]:
    """
    Parse various datetime formats into a datetime.
    Returns None if cannot parse.
    """
    if value is None:
        return None

    if isinstance(value, datetime):
        return value

    # Sometimes APIs store timestamps as numbers
    if isinstance(value, (int, float)):
        # Heuristic: if too large, treat as milliseconds
        ts = value / 1000 if value > 1_000_000_000_000 else value
        try:
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        except Exception:
            return None

    # Strings (ISO etc.)
    try:
        dt = date_parser.parse(str(value))
        return dt
    except Exception:
        return None


def now_like(dt: Any) -> datetime:
    """
    Returns "now" with timezone matching dt if dt is a datetime with tzinfo.
    Falls back to naive local now() if dt is naive or not a datetime.
    """
    if isinstance(dt, datetime) and dt.tzinfo is not None:
        return datetime.now(dt.tzinfo)
    return datetime.now()


def months_ago(like: Any, months: int) -> datetime:
    """
    Approximate months as 30*months days. Uses now_like(like) for timezone consistency.
    """
    base = now_like(like)
    return base - timedelta(days=30 * months)


# ============================================================================
# NEW FUNCTION (untuk viz/pages.py dan viz/report.py)
# ============================================================================

def format_date_for_display(x: Any) -> str:
    """
    Format a date/datetime/timestamp for display in reports.
    Returns "Unknown" if value is None or cannot be parsed.
    
    Args:
        x: Date value (can be datetime, pd.Timestamp, string, or None)
    
    Returns:
        Formatted date string (YYYY-MM-DD) or "Unknown"
    """
    if pd.isna(x) or x is None:
        return "Unknown"
    
    # Handle pandas Timestamp
    if isinstance(x, pd.Timestamp):
        return x.strftime("%Y-%m-%d")
    
    # Handle datetime
    if isinstance(x, datetime):
        return x.strftime("%Y-%m-%d")
    
    # Try to parse string
    try:
        dt = parse_datetime(x)
        if dt:
            return dt.strftime("%Y-%m-%d")
    except Exception:
        pass
    
    return "Unknown"