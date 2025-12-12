from __future__ import annotations

import argparse
import logging
from pathlib import Path

from moata_pipeline.logging_setup import setup_logging
from moata_pipeline.viz.runner import run_visual_report


def main() -> None:
    parser = argparse.ArgumentParser(description="Visualize alarm_summary.csv for non data scientists.")
    parser.add_argument("--csv", type=str, default="", help="Path to alarm_summary.csv")
    parser.add_argument("--out", type=str, default="", help="Output folder for report and images")
    args = parser.parse_args()

    setup_logging("INFO")

    csv_path = Path(args.csv) if args.csv else None
    out_dir = Path(args.out) if args.out else None

    report_path = run_visual_report(csv_path=csv_path, out_dir=out_dir)
    print(f"\n✅ Done! Open in browser: {report_path}")


if __name__ == "__main__":
    main()
