from __future__ import annotations

from pathlib import Path


def ensure_dir(p: Path) -> None:
    """
    Create directory if it doesn't exist (including parent directories).
    
    Args:
        p: Path to directory
    """
    p.mkdir(parents=True, exist_ok=True)