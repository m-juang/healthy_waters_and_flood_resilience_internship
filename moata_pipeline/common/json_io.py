from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def read_json_maybe_wrapped(path: Path) -> Any:
    """
    Reads JSON files that may be:
    - a plain list/dict
    - OR wrapped like: { "data": [...] }

    Returns the actual payload.
    """
    obj = read_json(path)

    if isinstance(obj, dict) and "data" in obj:
        return obj["data"]

    return obj
