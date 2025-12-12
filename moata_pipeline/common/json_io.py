from __future__ import annotations
from pathlib import Path
from typing import Any
import json

from .constants import DEFAULT_ENCODING

def read_json(path: Path) -> Any:
    """
    Read JSON file and return parsed python object.
    """
    if not path.exists():
        raise FileNotFoundError(f"JSON not found: {path}")
    text = path.read_text(encoding=DEFAULT_ENCODING)
    return json.loads(text)

def write_json(path: Path, data: Any, indent: int = 2) -> Path:
    """
    Write python object to JSON file (pretty printed).
    Creates parent directory automatically.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=indent, ensure_ascii=False), encoding=DEFAULT_ENCODING)
    return path

def read_json_maybe_wrapped(path: Path) -> Any:
    """
    Backward/forward compatible loader:
    - If file is {"schema_version":..., "data": ...}, return the 'data'
    - Else, return raw JSON content
    """
    obj = read_json(path)
    if isinstance(obj, dict) and "data" in obj and "schema_version" in obj:
        return obj["data"]
    return obj

def wrap_with_schema(data: Any, schema_version: int = 1, generated_at_iso: str | None = None) -> dict:
    """
    Minimal schema wrapper for future-proofing.
    Use this when you’re ready to version your outputs.
    """
    payload = {"schema_version": schema_version, "data": data}
    if generated_at_iso:
        payload["generated_at"] = generated_at_iso
    return payload
