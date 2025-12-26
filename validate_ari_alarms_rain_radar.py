"""
Validate rain radar ARI analysis results.

Checks which catchments would trigger alarms based on proportion of area
exceeding the ARI threshold.

Output:
    outputs/rain_radar/ari_alarm_validation.csv
"""
from __future__ import annotations

import argparse
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from moata_pipeline.logging_setup import setup_logging

logger = logging.getLogger(__name__)


def run_validation(
    ari_summary_path: Path,
    output_path: Path,
    proportion_threshold: float = 0.30,
) -> dict:
    """
    Validate ARI analysis against proportion threshold.
    """
    logger.info("Loading ARI summary from %s", ari_summary_path)
    df = pd.read_csv(ari_summary_path)
    logger.info("✓ Loaded %d catchment records", len(df))
    
    # Validate against proportion threshold
    df["would_alarm"] = df["proportion_exceeding"] >= proportion_threshold
    df["alarm_status"] = df["would_alarm"].map({True: "ALARM", False: "OK"})
    
    # Save results
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    cols = [
        "catchment_id", "catchment_name", "max_ari", "pixels_total",
        "pixels_exceeding", "proportion_exceeding", "alarm_status",
        "peak_duration", "peak_depth_mm", "peak_timestamp",
    ]
    available_cols = [c for c in cols if c in df.columns]
    df[available_cols].to_csv(output_path, index=False)
    logger.info("✓ Saved validation to %s", output_path)
    
    # Generate report
    lines = []
    lines.append("=" * 70)
    lines.append("RAIN RADAR ALARM VALIDATION REPORT")
    lines.append(f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    lines.append("=" * 70)
    lines.append("")
    lines.append("CONFIGURATION")
    lines.append("-" * 70)
    lines.append("ARI Threshold: 5 years")
    lines.append(f"Proportion Threshold: {proportion_threshold*100:.0f}%")
    lines.append("")
    lines.append("SUMMARY")
    lines.append("-" * 70)
    lines.append(f"Total catchments: {len(df)}")
    
    alarm_count = df["would_alarm"].sum()
    lines.append(f"Would trigger alarm: {alarm_count} ({100*alarm_count/len(df):.1f}%)")
    lines.append(f"OK: {len(df) - alarm_count}")
    lines.append("")
    
    # List alarms
    alarms = df[df["would_alarm"]].sort_values("proportion_exceeding", ascending=False)
    if not alarms.empty:
        lines.append("CATCHMENTS THAT WOULD ALARM")
        lines.append("-" * 70)
        for _, row in alarms.iterrows():
            name = row.get("catchment_name", "Unknown")
            prop = row.get("proportion_exceeding", 0)
            max_ari = row.get("max_ari", 0)
            lines.append(f"  {name}: {prop*100:.1f}% area, max ARI {max_ari:.1f}y")
    else:
        lines.append("NO ALARMS WOULD BE TRIGGERED")
        lines.append("-" * 70)
        lines.append("All catchments below proportion threshold.")
    
    lines.append("")
    lines.append("=" * 70)
    
    report = "\n".join(lines)
    
    return {
        "validation_df": df,
        "report": report,
        "output_path": output_path,
        "alarm_count": alarm_count,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate rain radar ARI analysis")
    parser.add_argument(
        "--date",
        metavar="YYYY-MM-DD",
        help="Validate historical data for specific date",
    )
    parser.add_argument(
        "--input",
        metavar="PATH",
        help="Path to ari_analysis_summary.csv (overrides --date)",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.30,
        help="Proportion threshold for alarm (default: 0.30 = 30%%)",
    )
    
    args = parser.parse_args()
    
    setup_logging("INFO")
    
    logger.info("=" * 80)
    logger.info("RAIN RADAR ALARM VALIDATION")
    logger.info("=" * 80)
    
    # Find input file
    if args.input:
        input_path = Path(args.input)
    elif args.date:
        input_path = Path(f"outputs/rain_radar/historical/{args.date}/analyze/ari_analysis_summary.csv")
    else:
        # Auto-detect
        candidates = [
            *sorted(Path("outputs/rain_radar/historical").glob("*/analyze/ari_analysis_summary.csv")),
            Path("outputs/rain_radar/analyze/ari_analysis_summary.csv"),
        ]
        input_path = None
        for p in reversed(candidates):
            if p.exists():
                input_path = p
                break
        
        if not input_path:
            logger.error("No ARI analysis summary found.")
            logger.error("Run 'python analyze_rain_radar.py' first.")
            return
    
    if not input_path.exists():
        logger.error("Input file not found: %s", input_path)
        return
    
    logger.info("Input: %s", input_path)
    logger.info("Proportion threshold: %.0f%%", args.threshold * 100)
    
    # Output path
    output_path = input_path.parent.parent / "ari_alarm_validation.csv"
    
    # Run validation
    result = run_validation(
        ari_summary_path=input_path,
        output_path=output_path,
        proportion_threshold=args.threshold,
    )
    
    logger.info("\n%s", result["report"])
    
    logger.info("=" * 80)
    logger.info("COMPLETE!")
    logger.info("=" * 80)
    logger.info("Results saved to: %s", result["output_path"])
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
