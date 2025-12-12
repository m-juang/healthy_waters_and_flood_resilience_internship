from __future__ import annotations

from pathlib import Path

# Project root (assumes moata_pipeline/ is inside the project folder)
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Default data folders
OUTPUT_DIR = PROJECT_ROOT / "moata_output"
FILTERED_DIR = PROJECT_ROOT / "moata_filtered"
VIZ_DIR = FILTERED_DIR / "viz"

def ensure_dir(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p
