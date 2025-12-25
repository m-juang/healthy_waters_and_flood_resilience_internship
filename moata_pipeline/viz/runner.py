from __future__ import annotations

import logging
from pathlib import Path

from moata_pipeline.common.paths import PipelinePaths
from moata_pipeline.common.file_utils import ensure_dir
from .cleaning import load_and_clean
from .pages import build_gauge_pages
from .report import build_report

logger = logging.getLogger(__name__)


def run_visual_report(csv_path: Path | None = None, out_dir: Path | None = None) -> Path:
    print(">>> Starting run_visual_report")
    
    paths = PipelinePaths()
    csv_path = csv_path or paths.alarm_summary_csv
    out_dir = out_dir or paths.rain_gauges_viz_dir

    print(f">>> csv_path: {csv_path}, exists: {csv_path.exists()}")
    print(f">>> out_dir: {out_dir}")

    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    ensure_dir(out_dir)

    print(">>> Loading CSV...")
    df = load_and_clean(csv_path)
    print(f">>> Loaded {len(df)} rows")

    # Save cleaned copy
    cleaned_path = out_dir / "cleaned_alarm_summary.csv"
    df.to_csv(cleaned_path, index=False)
    print(f">>> Saved cleaned CSV: {cleaned_path}")

    print(">>> Building per-gauge pages...")
    build_gauge_pages(df, out_dir)

    print(">>> Building report...")
    build_report(df, out_dir)

    report_path = out_dir / "report.html"
    print(f">>> DONE: {report_path}")
    return report_path