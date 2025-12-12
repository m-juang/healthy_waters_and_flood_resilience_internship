from __future__ import annotations

import logging
from pathlib import Path

from moata_pipeline.common.paths import PipelinePaths

from .cleaning import load_and_clean
from .charts import ensure_dir, build_charts
from .pages import build_gauge_pages
from .report import build_report


logger = logging.getLogger(__name__)


def run_visual_report(csv_path: Path | None = None, out_dir: Path | None = None) -> Path:
    paths = PipelinePaths()
    csv_path = csv_path or paths.alarm_summary_csv
    out_dir = out_dir or paths.viz_dir

    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    ensure_dir(out_dir)

    logger.info("Loading CSV: %s", csv_path)
    df = load_and_clean(csv_path)

    # Save cleaned copy
    cleaned_path = out_dir / "cleaned_alarm_summary.csv"
    df.drop(columns=["last_data_dt", "threshold_num"], errors="ignore").to_csv(cleaned_path, index=False)
    logger.info("Saved cleaned CSV: %s", cleaned_path)

    logger.info("Building charts...")
    build_charts(df, out_dir)

    logger.info("Building per-gauge pages...")
    build_gauge_pages(df, out_dir)

    logger.info("Building main report...")
    build_report(df, out_dir)

    report_path = out_dir / "report.html"
    logger.info("DONE. Open in browser: %s", report_path)
    return report_path
