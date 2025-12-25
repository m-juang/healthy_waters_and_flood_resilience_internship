from __future__ import annotations

from typing import Any, Dict, List, Optional, TypedDict

JsonDict = Dict[str, Any]
JsonList = List[Any]


class GaugeEntry(TypedDict, total=False):
    gauge: JsonDict
    traces: list[JsonDict]
    last_data_time: str


def safe_int(x: Any) -> Optional[int]:
    """Safely convert any value to int, returning None on failure."""
    if x is None:
        return None
    try:
        return int(x)
    except (TypeError, ValueError):
        return None


def safe_float(x: Any) -> Optional[float]:
    """Safely convert any value to float, returning None on failure."""
    if x is None:
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def safe_str(x: Any) -> Optional[str]:
    """Safely convert any value to stripped string, returning None if empty."""
    if x is None:
        return None
    s = str(x).strip()
    return s if s else None