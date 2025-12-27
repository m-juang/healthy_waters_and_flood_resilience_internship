"""
Time and Date Utility Functions

Provides utilities for parsing, formatting, and manipulating datetime objects.
Handles various datetime formats from APIs, databases, and user input.

Functions:
    - parse_datetime: Parse various datetime formats
    - now_like: Get current time matching timezone of reference datetime
    - months_ago: Calculate datetime N months in the past
    - iso_z: Convert datetime to ISO 8601 with Z suffix (UTC)
    - format_date_for_display: Format date for human-readable display
    - is_recent: Check if datetime is within N days of now
    - format_duration: Format timedelta as human-readable string

Usage:
    from moata_pipeline.common.time_utils import (
        parse_datetime,
        iso_z,
        format_date_for_display
    )
    
    # Parse various formats
    dt = parse_datetime("2025-01-15T10:30:00Z")
    dt = parse_datetime(1706178600)  # Unix timestamp
    
    # Format for API
    iso_string = iso_z(dt)  # "2025-01-15T10:30:00Z"
    
    # Format for display
    display = format_date_for_display(dt)  # "2025-01-15"

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2024-12-28
"""

from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Union

import pandas as pd
from dateutil import parser as date_parser


def parse_datetime(value: Any) -> Optional[datetime]:
    """
    Parse various datetime formats into a datetime object.
    
    Handles:
    - datetime objects (returned as-is)
    - Unix timestamps (int/float, auto-detects milliseconds)
    - ISO 8601 strings
    - Other common date string formats
    
    Args:
        value: Value to parse (datetime, int, float, or string)
        
    Returns:
        Parsed datetime object, or None if parsing fails
        
    Example:
        >>> parse_datetime("2025-01-15T10:30:00Z")
        datetime.datetime(2025, 1, 15, 10, 30, tzinfo=tzutc())
        
        >>> parse_datetime(1706178600)  # Unix timestamp
        datetime.datetime(2025, 1, 25, 8, 30, tzinfo=timezone.utc)
        
        >>> parse_datetime(1706178600000)  # Milliseconds (auto-detected)
        datetime.datetime(2025, 1, 25, 8, 30, tzinfo=timezone.utc)
        
        >>> parse_datetime(None)
        None
    """
    if value is None:
        return None
    
    # Already a datetime
    if isinstance(value, datetime):
        return value
    
    # Handle pandas Timestamp
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    
    # Numeric timestamps (Unix epoch)
    if isinstance(value, (int, float)):
        # Heuristic: if value > 1 trillion, treat as milliseconds
        # (Unix timestamps in seconds won't exceed ~2.1 billion until 2038)
        ts = value / 1000 if value > 1_000_000_000_000 else value
        try:
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        except (ValueError, OSError):
            # Invalid timestamp
            return None
    
    # String formats (ISO, etc.)
    try:
        dt = date_parser.parse(str(value))
        return dt
    except (ValueError, TypeError):
        return None


def now_like(dt: Any) -> datetime:
    """
    Return current time matching the timezone of a reference datetime.
    
    If dt is timezone-aware, returns now() in that timezone.
    If dt is naive or not a datetime, returns naive local now().
    
    Args:
        dt: Reference datetime (can be any type, but typically datetime)
        
    Returns:
        Current datetime matching dt's timezone
        
    Example:
        >>> import pytz
        >>> ref = datetime(2025, 1, 1, tzinfo=pytz.UTC)
        >>> now = now_like(ref)  # Returns current time in UTC
        
        >>> ref_naive = datetime(2025, 1, 1)
        >>> now = now_like(ref_naive)  # Returns naive local time
    """
    if isinstance(dt, datetime) and dt.tzinfo is not None:
        return datetime.now(dt.tzinfo)
    return datetime.now()


def months_ago(like: Any, months: int) -> datetime:
    """
    Calculate datetime N months in the past.
    
    Uses approximate calculation: 1 month = 30 days.
    Timezone matches the reference datetime via now_like().
    
    Args:
        like: Reference datetime for timezone (typically datetime)
        months: Number of months to go back (positive integer)
        
    Returns:
        Datetime N months in the past
        
    Example:
        >>> ref = datetime(2025, 6, 15, tzinfo=timezone.utc)
        >>> past = months_ago(ref, 3)
        >>> # Returns datetime approximately 3 months before now
        
    Note:
        Uses 30-day approximation. For exact calendar month math,
        consider using dateutil.relativedelta instead.
    """
    base = now_like(like)
    return base - timedelta(days=30 * months)


def iso_z(dt: Union[datetime, pd.Timestamp]) -> str:
    """
    Convert datetime to ISO 8601 format string with Z suffix (UTC).
    
    Args:
        dt: datetime or pandas Timestamp (naive assumed UTC, or timezone-aware)
        
    Returns:
        ISO format string with Z suffix (e.g., "2025-05-01T00:00:00Z")
        
    Example:
        >>> from datetime import datetime, timezone
        >>> dt = datetime(2025, 5, 1, 14, 30, tzinfo=timezone.utc)
        >>> iso_z(dt)
        '2025-05-01T14:30:00Z'
        
        >>> dt_naive = datetime(2025, 5, 1, 14, 30)
        >>> iso_z(dt_naive)  # Assumes UTC
        '2025-05-01T14:30:00Z'
    """
    # Handle pandas Timestamp
    if isinstance(dt, pd.Timestamp):
        dt = dt.to_pydatetime()
    
    # If naive, assume UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    
    # Convert to UTC and format
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def format_date_for_display(x: Any) -> str:
    """
    Format a date/datetime for human-readable display.
    
    Handles various input types and returns YYYY-MM-DD format.
    Returns "Unknown" if value is None or cannot be parsed.
    
    Args:
        x: Date value (datetime, pd.Timestamp, string, int, or None)
        
    Returns:
        Formatted date string (YYYY-MM-DD) or "Unknown"
        
    Example:
        >>> format_date_for_display(datetime(2025, 1, 15))
        '2025-01-15'
        
        >>> format_date_for_display("2025-01-15T10:30:00Z")
        '2025-01-15'
        
        >>> format_date_for_display(None)
        'Unknown'
        
        >>> format_date_for_display(pd.NaT)
        'Unknown'
    """
    # Handle None and pandas NaT
    if pd.isna(x) or x is None:
        return "Unknown"
    
    # Handle pandas Timestamp
    if isinstance(x, pd.Timestamp):
        try:
            return x.strftime("%Y-%m-%d")
        except ValueError:
            return "Unknown"
    
    # Handle datetime
    if isinstance(x, datetime):
        try:
            return x.strftime("%Y-%m-%d")
        except ValueError:
            return "Unknown"
    
    # Try to parse as string or other type
    try:
        dt = parse_datetime(x)
        if dt:
            return dt.strftime("%Y-%m-%d")
    except Exception:
        pass
    
    return "Unknown"


def is_recent(dt: Optional[datetime], days: int = 30) -> bool:
    """
    Check if a datetime is within N days of now.
    
    Args:
        dt: Datetime to check (or None)
        days: Number of days to consider "recent" (default: 30)
        
    Returns:
        True if dt is within last N days, False otherwise
        
    Example:
        >>> from datetime import datetime, timezone, timedelta
        >>> recent = datetime.now(timezone.utc) - timedelta(days=5)
        >>> is_recent(recent, days=30)
        True
        
        >>> old = datetime.now(timezone.utc) - timedelta(days=60)
        >>> is_recent(old, days=30)
        False
        
        >>> is_recent(None)
        False
    """
    if dt is None:
        return False
    
    try:
        now = now_like(dt)
        delta = now - dt
        return abs(delta.days) <= days
    except (TypeError, AttributeError):
        return False


def format_duration(td: timedelta) -> str:
    """
    Format a timedelta as human-readable string.
    
    Args:
        td: timedelta object
        
    Returns:
        Human-readable duration string
        
    Example:
        >>> from datetime import timedelta
        >>> format_duration(timedelta(days=5, hours=3, minutes=30))
        '5 days, 3 hours, 30 minutes'
        
        >>> format_duration(timedelta(hours=2))
        '2 hours'
        
        >>> format_duration(timedelta(seconds=45))
        '45 seconds'
    """
    total_seconds = int(td.total_seconds())
    
    if total_seconds < 0:
        return "0 seconds"
    
    days = total_seconds // 86400
    hours = (total_seconds % 86400) // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    
    parts = []
    
    if days > 0:
        parts.append(f"{days} day{'s' if days != 1 else ''}")
    if hours > 0:
        parts.append(f"{hours} hour{'s' if hours != 1 else ''}")
    if minutes > 0:
        parts.append(f"{minutes} minute{'s' if minutes != 1 else ''}")
    if seconds > 0 and not days:  # Don't show seconds if days present
        parts.append(f"{seconds} second{'s' if seconds != 1 else ''}")
    
    if not parts:
        return "0 seconds"
    
    return ", ".join(parts)


def ensure_utc(dt: datetime) -> datetime:
    """
    Ensure datetime is timezone-aware and in UTC.
    
    Args:
        dt: datetime object (naive or timezone-aware)
        
    Returns:
        Timezone-aware datetime in UTC
        
    Example:
        >>> dt_naive = datetime(2025, 1, 15, 10, 30)
        >>> dt_utc = ensure_utc(dt_naive)
        >>> dt_utc.tzinfo
        datetime.timezone.utc
    """
    if dt.tzinfo is None:
        # Naive datetime - assume UTC
        return dt.replace(tzinfo=timezone.utc)
    else:
        # Convert to UTC
        return dt.astimezone(timezone.utc)