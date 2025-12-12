from __future__ import annotations

from typing import Any, Dict, List, Optional, TypedDict

JsonDict = Dict[str, Any]
JsonList = List[Any]

class GaugeEntry(TypedDict, total=False):
    gauge: JsonDict
    traces: list[JsonDict]
    last_data_time: str
