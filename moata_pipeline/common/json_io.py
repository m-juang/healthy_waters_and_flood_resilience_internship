from __future__ import annotations

import json
from pathlib import Path
from typing import Any

def read_json(path: Path) -> Any:
    if not path.exists():
        raise FileNotFoundError(f"JSON not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))

def write_json(path: Path, data: Any, indent: int = 2) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=indent, ensure_ascii=False), encoding="utf-8")

def write_jsonl(path: Path, rows: list[dict]) -> None:
    """
    Optional utility if later you want resume-safe incremental writes.
    Each row is one JSON object per line.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
