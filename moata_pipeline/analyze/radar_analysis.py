"""
Radar Data Analysis Module

Runs ARI analysis on radar QPE data and generates comprehensive summary reports.

Key Functions:
    run_radar_analysis: Main entry point for radar ARI analysis
    
Helper Functions:
    _process_catchment_file: Process individual catchment radar data
    _generate_report: Generate human-readable analysis report

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2024-12-28
Version: 1.0.0
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

from moata_pipeline.analyze.ari_calculator import ARICalculator, DURATION_CONFIG


# Version info
__version__ = "1.0.0"


# =============================================================================
# Custom Exceptions
# =============================================================================

class RadarAnalysisError(Exception):
    """Base exception for radar analysis errors."""
    pass


class NoRadarDataError(RadarAnalysisError):
    """Raised when no radar data files found."""
    pass


# =============================================================================
# Helper Functions
# =============================================================================

def _process_catchment_file(
    calc: ARICalculator,
    filepath: Path,
    ari_threshold: float,
) -> Dict[str, Any]:
    """
    Process one catchment radar file and return ARI summary.
    
    Args:
        calc: Initialized ARICalculator instance
        filepath: Path to catchment radar CSV
        ari_threshold: ARI threshold for exceedance
        
    Returns:
        Dictionary with catchment summary statistics
    """
    logger = logging.getLogger(__name__)
    
    # Load and validate data
    df = pd.read_csv(filepath)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    
    pixels = df["pixel_index"].unique()
    total_pixels = len(pixels)
    coeffs = calc.load_coefficients()
    
    # Initialize tracking
    max_ari = 0.0
    max_info: Dict[str, Any] = {}
    pixels_exceeding = set()
    exceedance_records = []
    
    # Process each pixel
    for pixel_idx in pixels:
        if pixel_idx not in coeffs.index:
            continue
        
        pixel_data = df[df["pixel_index"] == pixel_idx].copy()
        pixel_data = pixel_data.sort_values("timestamp").set_index("timestamp")
        pixel_coeffs = coeffs.loc[pixel_idx]
        
        pixel_max_ari = 0.0
        
        # Process each duration
        for dur_name, minutes in DURATION_CONFIG.items():
            b_col = f"{dur_name}_b"
            m_col = f"{dur_name}_m"
            
            if b_col not in pixel_coeffs or m_col not in pixel_coeffs:
                continue
            
            b = pixel_coeffs[b_col]
            m = pixel_coeffs[m_col]
            
            if pd.isna(b) or pd.isna(m):
                continue
            
            # Calculate rolling sum
            rolling = pixel_data["value"].rolling(
                window=minutes,
                min_periods=minutes
            ).sum()
            
            # Calculate ARI for each timestamp
            for ts, depth in rolling.items():
                if pd.isna(depth) or depth <= 0:
                    continue
                
                ari = calc.calculate_ari(depth, b, m)
                
                # Track pixel max
                if ari > pixel_max_ari:
                    pixel_max_ari = ari
                
                # Track overall max
                if ari > max_ari:
                    max_ari = ari
                    max_info = {
                        "peak_pixel_index": int(pixel_idx),
                        "peak_timestamp": ts,
                        "peak_duration": dur_name,
                        "peak_depth_mm": round(depth, 2),
                    }
                
                # Record exceedances
                if ari >= ari_threshold:
                    exceedance_records.append({
                        "pixel_index": int(pixel_idx),
                        "timestamp": ts,
                        "duration": dur_name,
                        "depth_mm": round(depth, 2),
                        "ari_years": round(ari, 2),
                    })
        
        # Track pixels with exceedances
        if pixel_max_ari >= ari_threshold:
            pixels_exceeding.add(pixel_idx)
    
    # Calculate proportion
    proportion = len(pixels_exceeding) / total_pixels if total_pixels > 0 else 0
    
    return {
        "max_ari": round(max_ari, 2),
        "pixels_total": total_pixels,
        "pixels_exceeding": len(pixels_exceeding),
        "proportion_exceeding": round(proportion, 4),
        "exceedance_records": exceedance_records,
        **max_info,
    }


def _generate_report(
    summary_df: pd.DataFrame,
    exceedance_df: pd.DataFrame,
    ari_threshold: float,
) -> str:
    """
    Generate human-readable analysis report.
    
    Args:
        summary_df: Summary statistics DataFrame
        exceedance_df: Exceedance records DataFrame
        ari_threshold: ARI threshold used
        
    Returns:
        Formatted report text
    """
    lines = []
    lines.append("=" * 70)
    lines.append("RAIN RADAR ARI ANALYSIS REPORT")
    lines.append(f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    lines.append("=" * 70)
    lines.append("")
    
    # Overall stats
    lines.append("SUMMARY STATISTICS")
    lines.append("-" * 70)
    lines.append(f"Total catchments analyzed: {len(summary_df)}")
    lines.append(f"ARI threshold: {ari_threshold} years")
    lines.append("")
    
    catchments_with_exceedance = int((summary_df["max_ari"] >= ari_threshold).sum())
    lines.append(f"Catchments with ARI >= {ari_threshold}: {catchments_with_exceedance}")
    
    high_proportion = int((summary_df["proportion_exceeding"] >= 0.10).sum())
    lines.append(f"Catchments with >= 10% area exceeding: {high_proportion}")
    
    if not exceedance_df.empty:
        lines.append(f"Total exceedance records: {len(exceedance_df)}")
        lines.append(f"Unique pixels with exceedance: {exceedance_df['pixel_index'].nunique()}")
    
    lines.append("")
    
    # Top catchments
    lines.append("TOP 20 CATCHMENTS BY MAX ARI")
    lines.append("-" * 70)
    
    top_20 = summary_df.head(20)
    for _, row in top_20.iterrows():
        name = row.get("catchment_name", "Unknown")
        max_ari = row.get("max_ari", 0)
        duration = row.get("peak_duration", "N/A")
        depth = row.get("peak_depth_mm", 0)
        proportion = row.get("proportion_exceeding", 0)
        
        if max_ari > 0:
            lines.append(f"  {name}")
            lines.append(f"    Max ARI: {max_ari:.1f} years ({duration}, {depth}mm)")
            lines.append(f"    Area exceeding: {proportion*100:.1f}%")
    
    lines.append("")
    
    # Proportion distribution
    lines.append("PROPORTION EXCEEDING DISTRIBUTION")
    lines.append("-" * 70)
    
    bins = [0, 0.01, 0.05, 0.10, 0.25, 0.50, 1.01]
    labels = ["0-1%", "1-5%", "5-10%", "10-25%", "25-50%", "50-100%"]
    
    summary_df = summary_df.copy()
    summary_df["proportion_bin"] = pd.cut(
        summary_df["proportion_exceeding"],
        bins=bins,
        labels=labels,
        include_lowest=True,
    )
    
    for label in labels:
        count = int((summary_df["proportion_bin"] == label).sum())
        lines.append(f"  {label}: {count} catchments")
    
    lines.append("")
    lines.append("=" * 70)
    
    return "\n".join(lines)


# =============================================================================
# Main Analysis Function
# =============================================================================

def run_radar_analysis(
    radar_data_dir: Path,
    output_dir: Path,
    tp108_path: Path = Path("data/inputs/tp108_stats.csv"),
    ari_threshold: float = 5.0,
) -> Dict[str, Any]:
    """
    Run ARI analysis on all radar data files.
    
    Process Steps:
        1. Initialize ARICalculator with TP108 coefficients
        2. Process each catchment radar CSV file
        3. Calculate ARI values for all pixels and durations
        4. Generate summary statistics and exceedance records
        5. Save outputs (summary CSV, exceedances CSV, report TXT)
        
    Args:
        radar_data_dir: Directory containing radar CSV files
        output_dir: Directory for output files
        tp108_path: Path to TP108 coefficients CSV
        ari_threshold: ARI threshold for exceedance (default: 5.0 years)
        
    Returns:
        Dictionary containing:
            - summary_df: Summary statistics per catchment
            - exceedance_df: All ARI exceedance records
            - report: Text report
            - output_dir: Output directory path
            - summary_path: Path to summary CSV
            - exceedance_path: Path to exceedances CSV
            
    Raises:
        NoRadarDataError: If no radar data files found
        ValueError: If parameters are invalid
        
    Example:
        >>> result = run_radar_analysis(
        ...     radar_data_dir=Path("outputs/rain_radar/raw/radar_data"),
        ...     output_dir=Path("outputs/rain_radar/analyze"),
        ...     ari_threshold=5.0
        ... )
        >>> print(f"Analyzed {len(result['summary_df'])} catchments")
    """
    logger = logging.getLogger(__name__)
    
    # Validate inputs
    if ari_threshold <= 0:
        raise ValueError(f"ari_threshold must be positive, got {ari_threshold}")
    
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info("=" * 80)
    logger.info("Rain Radar ARI Analysis")
    logger.info("=" * 80)
    logger.info(f"Radar data directory: {radar_data_dir}")
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"TP108 coefficients: {tp108_path}")
    logger.info(f"ARI threshold: {ari_threshold} years")
    logger.info("=" * 80)
    logger.info("")
    
    try:
        # Initialize calculator
        logger.info("Initializing ARI calculator...")
        calc = ARICalculator(tp108_path=tp108_path, ari_threshold=ari_threshold)
        calc.load_coefficients()
        logger.info("✓ Calculator ready")
        
        # Get radar files
        radar_files = sorted(Path(radar_data_dir).glob("*.csv"))
        logger.info(f"Found {len(radar_files)} radar data files")
        
        if not radar_files:
            raise NoRadarDataError(
                f"No radar data files found in {radar_data_dir}\n\n"
                f"Run data collection first:\n"
                f"  python retrieve_rain_radar.py"
            )
        
        # Process each catchment
        logger.info("")
        logger.info("Processing catchments...")
        summaries = []
        all_exceedances = []
        
        for i, filepath in enumerate(radar_files, start=1):
            if i % 25 == 0 or i == 1:
                logger.info(f"  [{i}/{len(radar_files)}] Processing...")
            
            try:
                # Extract catchment info from filename
                parts = filepath.stem.split("_", 1)
                catchment_id = int(parts[0]) if parts[0].isdigit() else None
                catchment_name = parts[1] if len(parts) > 1 else filepath.stem
                
                # Process file
                result = _process_catchment_file(calc, filepath, ari_threshold)
                
                # Add catchment info
                result["catchment_id"] = catchment_id
                result["catchment_name"] = catchment_name
                
                # Collect exceedance records
                for rec in result.pop("exceedance_records", []):
                    rec["catchment_id"] = catchment_id
                    rec["catchment_name"] = catchment_name
                    all_exceedances.append(rec)
                
                summaries.append(result)
                
            except Exception as e:
                logger.warning(f"  Failed to process {filepath.name}: {e}")
                continue
        
        logger.info(f"✓ Processed {len(summaries)} catchments successfully")
        
        # Create DataFrames
        logger.info("")
        logger.info("Creating summary DataFrames...")
        summary_df = pd.DataFrame(summaries)
        summary_df = summary_df.sort_values("max_ari", ascending=False)
        
        exceedance_df = pd.DataFrame(all_exceedances)
        if not exceedance_df.empty:
            exceedance_df = exceedance_df.sort_values(["catchment_name", "timestamp"])
        
        # Save outputs
        logger.info("")
        logger.info("Saving outputs...")
        
        summary_path = output_dir / "ari_analysis_summary.csv"
        summary_df.to_csv(summary_path, index=False)
        logger.info(f"✓ Saved summary to {summary_path}")
        
        exceedance_path = output_dir / "ari_exceedances.csv"
        exceedance_df.to_csv(exceedance_path, index=False)
        logger.info(f"✓ Saved exceedances to {exceedance_path}")
        
        # Generate report
        logger.info("")
        logger.info("Generating analysis report...")
        report = _generate_report(summary_df, exceedance_df, ari_threshold)
        
        report_path = output_dir / "analysis_report.txt"
        report_path.write_text(report, encoding="utf-8")
        logger.info(f"✓ Saved report to {report_path}")
        
        # Summary
        logger.info("")
        logger.info("=" * 80)
        logger.info("✓ Analysis Complete")
        logger.info("=" * 80)
        logger.info(f"Catchments analyzed: {len(summary_df)}")
        logger.info(f"Exceedances found: {len(exceedance_df)}")
        logger.info(f"Output directory: {output_dir}")
        logger.info("=" * 80)
        
        return {
            "summary_df": summary_df,
            "exceedance_df": exceedance_df,
            "report": report,
            "output_dir": output_dir,
            "summary_path": summary_path,
            "exceedance_path": exceedance_path,
        }
        
    except NoRadarDataError as e:
        logger.error(str(e))
        raise
        
    except Exception as e:
        logger.error(f"Analysis failed: {e}")
        logger.exception("Full traceback:")
        raise RadarAnalysisError(f"Radar analysis failed: {e}") from e