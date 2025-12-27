"""
Analysis Runner Module

Provides high-level entry point functions for running analysis tasks.
Used by entry point scripts (analyze_rain_gauges.py, analyze_rain_radar.py).

Functions:
    run_filter_active_gauges: Filter and analyze rain gauge data
    
Note:
    This module handles the offline analysis pipeline for rain gauge data,
    including filtering, alarm analysis, and report generation.

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2024-12-28
Version: 1.0.0
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

from moata_pipeline.common.json_io import read_json_maybe_wrapped, write_json
from moata_pipeline.common.paths import PipelinePaths
from moata_pipeline.common.constants import (
    INACTIVE_THRESHOLD_MONTHS,
    DEFAULT_EXCLUDE_KEYWORD,
)

from .filtering import FilterConfig, filter_gauges
from .alarm_analysis import analyze_alarms
from .reporting import create_summary_report


# Version info
__version__ = "1.0.0"


# =============================================================================
# Custom Exceptions
# =============================================================================

class AnalysisRunnerError(Exception):
    """Base exception for analysis runner errors."""
    pass


class InputDataError(AnalysisRunnerError):
    """Raised when input data is invalid or missing."""
    pass


class OutputError(AnalysisRunnerError):
    """Raised when output generation fails."""
    pass


# =============================================================================
# Helper Functions
# =============================================================================

def _validate_input_data(data: Any, source_path: Path) -> None:
    """
    Validate input data structure.
    
    Args:
        data: Loaded data to validate
        source_path: Path where data was loaded from
        
    Raises:
        InputDataError: If data is invalid
    """
    if not isinstance(data, list):
        raise InputDataError(
            f"Invalid input data format in {source_path}\n\n"
            f"Expected: list of gauge dictionaries\n"
            f"Got: {type(data).__name__}\n\n"
            f"The file may be corrupted or in an unexpected format.\n"
            f"Try running data collection again."
        )
    
    if len(data) == 0:
        raise InputDataError(
            f"No data found in {source_path}\n\n"
            f"The input file is empty. Run data collection first:\n"
            f"  python retrieve_rain_gauges.py"
        )


def _save_active_gauges_json(
    active_gauges: list,
    output_dir: Path,
    logger: logging.Logger
) -> Path:
    """
    Save active gauges to JSON file.
    
    Removes non-serializable datetime objects before saving.
    
    Args:
        active_gauges: List of active gauge dictionaries
        output_dir: Output directory
        logger: Logger instance
        
    Returns:
        Path to saved JSON file
    """
    # Remove non-serializable datetime objects
    active_serializable = []
    for gauge in active_gauges:
        gauge_copy = dict(gauge)
        gauge_copy.pop("last_data_time_dt", None)  # Remove datetime object
        active_serializable.append(gauge_copy)
    
    output_path = output_dir / "active_auckland_gauges.json"
    write_json(output_path, active_serializable)
    
    logger.info(f"✓ Saved active_auckland_gauges.json ({len(active_serializable)} gauges)")
    return output_path


def _save_traces_csv(
    all_traces_df: Optional[pd.DataFrame],
    output_dir: Path,
    logger: logging.Logger
) -> Optional[Path]:
    """
    Save all traces DataFrame to CSV.
    
    Args:
        all_traces_df: DataFrame with all traces
        output_dir: Output directory
        logger: Logger instance
        
    Returns:
        Path to saved CSV, or None if no data
    """
    if all_traces_df is None or all_traces_df.empty:
        logger.warning("No traces data to save")
        return None
    
    output_path = output_dir / "all_traces.csv"
    all_traces_df.to_csv(output_path, index=False)
    
    logger.info(f"✓ Saved all_traces.csv ({len(all_traces_df)} rows)")
    return output_path


def _save_alarms_csv(
    alarms_only_df: Optional[pd.DataFrame],
    output_dir: Path,
    logger: logging.Logger
) -> Dict[str, Optional[Path]]:
    """
    Save alarms DataFrames to CSV files (full and simple versions).
    
    Args:
        alarms_only_df: DataFrame with alarms only
        output_dir: Output directory
        logger: Logger instance
        
    Returns:
        Dictionary with 'full' and 'simple' paths
    """
    result = {"full": None, "simple": None}
    
    if alarms_only_df is None or alarms_only_df.empty:
        logger.warning("No alarms data to save")
        return result
    
    # Save full version (all columns)
    full_path = output_dir / "alarm_summary_full.csv"
    alarms_only_df.to_csv(full_path, index=False)
    result["full"] = full_path
    logger.info(f"✓ Saved alarm_summary_full.csv ({len(alarms_only_df)} rows)")
    
    # Save simple version (essential columns only)
    simple_cols = [
        "gauge_name",
        "trace_description",
        "alarm_name",
        "alarm_type",
        "threshold",
    ]
    
    # Check if all required columns exist
    missing_cols = [col for col in simple_cols if col not in alarms_only_df.columns]
    if missing_cols:
        logger.warning(
            f"Cannot create simple alarm summary: missing columns {missing_cols}"
        )
        return result
    
    alarms_simple_df = alarms_only_df[simple_cols].copy()
    alarms_simple_df.columns = ["Gauge", "Trace", "Alarm Name", "Type", "Threshold"]
    
    simple_path = output_dir / "alarm_summary.csv"
    alarms_simple_df.to_csv(simple_path, index=False)
    result["simple"] = simple_path
    logger.info(f"✓ Saved alarm_summary.csv ({len(alarms_simple_df)} rows - simplified)")
    
    return result


def _save_report(
    report_text: str,
    output_dir: Path,
    logger: logging.Logger
) -> Path:
    """
    Save analysis report to text file.
    
    Args:
        report_text: Report content
        output_dir: Output directory
        logger: Logger instance
        
    Returns:
        Path to saved report
    """
    output_path = output_dir / "analysis_report.txt"
    output_path.write_text(report_text, encoding="utf-8")
    
    logger.info(f"✓ Saved analysis_report.txt")
    return output_path


# =============================================================================
# Public Runner Functions
# =============================================================================

def run_filter_active_gauges(
    input_json: Optional[Path] = None,
    out_dir: Optional[Path] = None,
    inactive_months: int = INACTIVE_THRESHOLD_MONTHS,
    exclude_keyword: str = DEFAULT_EXCLUDE_KEYWORD,
) -> Dict[str, Any]:
    """
    Run offline analysis pipeline for rain gauge data.
    
    Pipeline Steps:
        1. Load collected gauge data from JSON
        2. Filter for active Auckland gauges
        3. Analyze alarms and create summary DataFrames
        4. Generate analysis report
        5. Save outputs (JSON, CSV, TXT)
        
    Output Files:
        - active_auckland_gauges.json: Filtered gauge data
        - all_traces.csv: All traces with full details
        - alarm_summary_full.csv: Alarms only with full details
        - alarm_summary.csv: Alarms only with essential columns
        - analysis_report.txt: Text summary report
        
    Args:
        input_json: Path to input JSON file (default: auto-detect from outputs)
        out_dir: Output directory (default: outputs/rain_gauges/analyze)
        inactive_months: Inactivity threshold in months (default from constants)
        exclude_keyword: Keyword to exclude from gauge names (default from constants)
        
    Returns:
        Dictionary containing:
            - output_dir: Path to output directory
            - filtered_data: Filtered gauge data
            - all_traces_df: DataFrame with all traces
            - alarms_only_df: DataFrame with alarms only
            - report: Analysis report text
            
    Raises:
        InputDataError: If input data is invalid or missing
        OutputError: If output generation fails
        AnalysisRunnerError: If analysis fails
        
    Example:
        >>> result = run_filter_active_gauges(
        ...     inactive_months=6,
        ...     exclude_keyword="test"
        ... )
        >>> print(f"Analyzed {len(result['filtered_data']['active_gauges'])} gauges")
    """
    logger = logging.getLogger(__name__)
    
    logger.info("=" * 80)
    logger.info("Rain Gauge Analysis Pipeline")
    logger.info("=" * 80)
    
    # Initialize paths
    paths = PipelinePaths()
    input_path = input_json or paths.rain_gauges_traces_alarms_json
    output_dir = out_dir or paths.rain_gauges_analyze_dir
    
    logger.info(f"Input: {input_path}")
    logger.info(f"Output: {output_dir}")
    logger.info(f"Inactive threshold: {inactive_months} months")
    logger.info(f"Exclude keyword: '{exclude_keyword}'")
    logger.info("=" * 80)
    logger.info("")
    
    try:
        # Ensure output directory exists
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Step 1: Load input data
        logger.info("Step 1: Loading input data...")
        
        if not input_path.exists():
            raise InputDataError(
                f"Input file not found: {input_path}\n\n"
                f"Run data collection first:\n"
                f"  python retrieve_rain_gauges.py"
            )
        
        all_data = read_json_maybe_wrapped(input_path)
        _validate_input_data(all_data, input_path)
        
        logger.info(f"✓ Loaded {len(all_data)} gauges from {input_path}")
        
        # Step 2: Filter gauges
        logger.info("")
        logger.info("Step 2: Filtering active gauges...")
        
        filter_config = FilterConfig(
            inactive_threshold_months=inactive_months,
            exclude_keyword=exclude_keyword,
        )
        
        filtered = filter_gauges(all_data, filter_config)
        active_gauges = filtered.get("active_gauges", [])
        
        logger.info(f"✓ Filtered to {len(active_gauges)} active gauges")
        logger.info(f"  Excluded: {filtered.get('num_excluded', 0)}")
        logger.info(f"  Inactive: {filtered.get('num_inactive', 0)}")
        
        if len(active_gauges) == 0:
            logger.warning("No active gauges found after filtering!")
        
        # Step 3: Analyze alarms
        logger.info("")
        logger.info("Step 3: Analyzing alarms...")
        
        all_traces_df, alarms_only_df = analyze_alarms(active_gauges)
        
        if all_traces_df is not None:
            logger.info(f"✓ Analyzed {len(all_traces_df)} traces total")
        
        if alarms_only_df is not None:
            logger.info(f"✓ Found {len(alarms_only_df)} alarms")
        else:
            logger.warning("No alarms found in any traces")
        
        # Step 4: Generate report
        logger.info("")
        logger.info("Step 4: Generating analysis report...")
        
        report = create_summary_report(
            filtered,
            alarms_only_df if alarms_only_df is not None else pd.DataFrame(),
        )
        
        logger.info("✓ Report generated")
        
        # Step 5: Save outputs
        logger.info("")
        logger.info("Step 5: Saving outputs...")
        
        json_path = _save_active_gauges_json(active_gauges, output_dir, logger)
        traces_path = _save_traces_csv(all_traces_df, output_dir, logger)
        alarms_paths = _save_alarms_csv(alarms_only_df, output_dir, logger)
        report_path = _save_report(report, output_dir, logger)
        
        # Summary
        logger.info("")
        logger.info("=" * 80)
        logger.info("✓ Analysis Complete")
        logger.info("=" * 80)
        logger.info(f"Active gauges: {len(active_gauges)}")
        logger.info(f"Total traces: {len(all_traces_df) if all_traces_df is not None else 0}")
        logger.info(f"Alarms found: {len(alarms_only_df) if alarms_only_df is not None else 0}")
        logger.info("")
        logger.info("Output files:")
        logger.info(f"  - {json_path.name}")
        if traces_path:
            logger.info(f"  - {traces_path.name}")
        if alarms_paths.get("full"):
            logger.info(f"  - {alarms_paths['full'].name}")
        if alarms_paths.get("simple"):
            logger.info(f"  - {alarms_paths['simple'].name}")
        logger.info(f"  - {report_path.name}")
        logger.info("")
        logger.info(f"Output directory: {output_dir}")
        logger.info("=" * 80)
        
        return {
            "output_dir": output_dir,
            "filtered_data": filtered,
            "all_traces_df": all_traces_df,
            "alarms_only_df": alarms_only_df,
            "report": report,
        }
        
    except InputDataError as e:
        logger.error("Input data error:")
        logger.error(str(e))
        raise
        
    except Exception as e:
        logger.error(f"Analysis failed: {e}")
        logger.exception("Full traceback:")
        raise AnalysisRunnerError(
            f"Rain gauge analysis failed: {e}\n\n"
            f"Check logs above for details."
        ) from e