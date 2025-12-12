from __future__ import annotations
import re

def safe_filename(name: str, max_len: int = 120) -> str:
    """
    Convert arbitrary text into a safe filename.
    """
    name = (name or "").strip()
    name = re.sub(r"[^\w\s\-\.]", "_", name)
    name = re.sub(r"\s+", "_", name)
    return name[:max_len] if len(name) > max_len else name
