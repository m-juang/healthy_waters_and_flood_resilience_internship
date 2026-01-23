#!/usr/bin/env python3
"""
Rain Radar ARI Analysis Visualization Script

Creates interactive HTML dashboard showing ARI analysis results including:
- Top catchments by maximum ARI
- ARI exceedance distribution
- Duration-based analysis
- Spatial coverage statistics

Reads from analysis/ folder:
- Reads ari_analysis_summary.csv
- Reads ari_exceedances.csv
- Generates visualizations of ARI analysis results

Usage:
    # Visualize analysis for specific date
    python visualize.py --date 2025-05-09
    
    # Visualize today's analysis
    python visualize.py
    
    # Custom input/output directories
    python visualize.py --date 2025-05-09 --input outputs/rain_radar/analysis/
    
    # Don't open browser automatically
    python visualize.py --date 2025-05-09 --no-open

Output:
    outputs/rain_radar/YYYYMMDD-YYYYMMDD/visualizations/
    +-- analysis_dashboard.html       # Interactive dashboard
    +-- analysis_statistics.csv       # Summary statistics
    +-- visualization_report.txt      # Text report

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2026-01-23
Version: 3.1.0 - Fixed popup and browser order
"""

import sys
from pathlib import Path
# Add project root to Python path
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

import argparse
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List, Tuple

import pandas as pd

from moata_pipeline.common.script_utils import setup_script_logger
from moata_pipeline.common.paths import PipelinePaths

__version__ = "3.1.0"


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Visualize radar ARI analysis results",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --date 2025-05-09         # Visualize specific date
  %(prog)s                           # Use today's date
  %(prog)s --date 2025-05-09 --no-open  # Don't open browser
  %(prog)s --date 2025-05-09 --log-level DEBUG

Input files (from analyze.py):
  - ari_analysis_summary.csv: Per-catchment ARI summary
  - ari_exceedances.csv: All exceedance records

Output:
  Interactive HTML dashboard with ARI analysis visualization
  Charts showing ARI distribution and top catchments
  Statistics on exceedance frequency and affected areas
        """
    )
    
    # Date argument
    parser.add_argument(
        "--date",
        metavar="YYYY-MM-DD",
        help="Date to visualize (default: today)"
    )
    
    # Input/output directories
    parser.add_argument(
        "--input",
        type=Path,
        help="Custom input directory with analysis data (default: auto-detect)"
    )
    
    parser.add_argument(
        "--output",
        type=Path,
        help="Custom output directory for visualizations (default: auto-detect)"
    )
    
    # Logging
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)"
    )
    
    # Browser option
    parser.add_argument(
        "--no-open",
        action="store_true",
        help="Do not open dashboard in browser after generation"
    )
    
    return parser.parse_args()


def find_analysis_data(input_dir: Path, logger: logging.Logger, paths: Optional[PipelinePaths] = None) -> Tuple[Path, Optional[Path]]:
    """
    Find analysis data files in input directory.
    
    Args:
        input_dir: Directory to search
        logger: Logger instance
        paths: Optional PipelinePaths instance for canonical file paths
        
    Returns:
        Tuple of (summary_file_path, exceedances_file_path or None)
        
    Raises:
        FileNotFoundError: If summary file not found
    """
    # Use PipelinePaths file properties if available
    if paths is not None:
        summary_file = paths.rain_radar_ari_analysis_summary_csv
        exceedances_file = paths.rain_radar_ari_analysis_exceedances_csv
    else:
        # Fallback to manual path construction
        summary_file = input_dir / "ari_analysis_summary.csv"
        exceedances_file = input_dir / "ari_exceedances.csv"
    
    # Validate summary file exists
    if not summary_file.exists():
        raise FileNotFoundError(
            f"Analysis summary file not found: {summary_file}\n"
            f"Expected: ari_analysis_summary.csv in {input_dir}\n"
            f"Have you run 'Analyze Data' step first?\n"
            f"  python scripts/radar/analyze.py --date YYYY-MM-DD"
        )
    
    logger.info(f"Found ari_analysis_summary.csv")
    
    # Check exceedances file (optional)
    if exceedances_file.exists():
        logger.info(f"Found ari_exceedances.csv")
        return summary_file, exceedances_file
    else:
        logger.info("No ari_exceedances.csv found (optional)")
        return summary_file, None


def load_analysis_data(
    summary_file: Path, 
    exceedances_file: Optional[Path],
    logger: logging.Logger
) -> Tuple[pd.DataFrame, Optional[pd.DataFrame]]:
    """
    Load analysis data from CSV files.
    
    Args:
        summary_file: Path to ari_analysis_summary.csv
        exceedances_file: Path to ari_exceedances.csv (optional)
        logger: Logger instance
        
    Returns:
        Tuple of (summary_df, exceedances_df or None)
    """
    logger.info(f"Loading summary data from: {summary_file}")
    summary_df = pd.read_csv(summary_file)
    logger.info(f"Loaded {len(summary_df)} catchment records")
    
    exceedances_df = None
    if exceedances_file is not None:
        logger.info(f"Loading exceedances data from: {exceedances_file}")
        try:
            # Check if file is empty or has no data
            exceedances_df = pd.read_csv(exceedances_file)
            if len(exceedances_df) > 0:
                if "timestamp" in exceedances_df.columns:
                    exceedances_df["timestamp"] = pd.to_datetime(exceedances_df["timestamp"])
                logger.info(f"Loaded {len(exceedances_df)} exceedance records")
            else:
                logger.info("Exceedances file is empty (no exceedances detected during analysis)")
                exceedances_df = None
        except pd.errors.EmptyDataError:
            # File exists but is empty (no exceedances detected)
            logger.info("Exceedances file is empty (no exceedances detected during analysis)")
            exceedances_df = None
        except Exception as e:
            logger.warning(f"Could not load exceedances file: {e}")
            exceedances_df = None
    
    return summary_df, exceedances_df


def generate_statistics(
    summary_df: pd.DataFrame, 
    exceedances_df: Optional[pd.DataFrame],
    logger: logging.Logger
) -> Dict:
    """
    Generate statistics from analysis data.
    
    Args:
        summary_df: Summary DataFrame
        exceedances_df: Exceedances DataFrame (optional)
        logger: Logger instance
        
    Returns:
        Dictionary with statistics
    """
    stats = {}
    
    # Basic counts
    stats["total_catchments"] = len(summary_df)
    
    # ARI statistics
    if "max_ari" in summary_df.columns:
        stats["max_ari_overall"] = round(summary_df["max_ari"].max(), 2)
        stats["mean_ari"] = round(summary_df["max_ari"].mean(), 2)
        stats["median_ari"] = round(summary_df["max_ari"].median(), 2)
        
        # Count catchments by ARI threshold
        stats["catchments_ari_5plus"] = int((summary_df["max_ari"] >= 5).sum())
        stats["catchments_ari_10plus"] = int((summary_df["max_ari"] >= 10).sum())
        stats["catchments_ari_20plus"] = int((summary_df["max_ari"] >= 20).sum())
        stats["catchments_ari_50plus"] = int((summary_df["max_ari"] >= 50).sum())
    
    # Proportion statistics
    if "proportion_exceeding" in summary_df.columns:
        stats["max_proportion"] = round(summary_df["proportion_exceeding"].max() * 100, 1)
        stats["mean_proportion"] = round(summary_df["proportion_exceeding"].mean() * 100, 1)
        
        # Count by proportion thresholds
        stats["catchments_10pct_area"] = int((summary_df["proportion_exceeding"] >= 0.10).sum())
        stats["catchments_25pct_area"] = int((summary_df["proportion_exceeding"] >= 0.25).sum())
        stats["catchments_50pct_area"] = int((summary_df["proportion_exceeding"] >= 0.50).sum())
    
    # Pixel statistics
    if "pixels_total" in summary_df.columns:
        stats["total_pixels_analyzed"] = int(summary_df["pixels_total"].sum())
    if "pixels_exceeding" in summary_df.columns:
        stats["total_pixels_exceeding"] = int(summary_df["pixels_exceeding"].sum())
    
    # Exceedance statistics
    if exceedances_df is not None and len(exceedances_df) > 0:
        stats["total_exceedances"] = len(exceedances_df)
        stats["unique_pixels_with_exceedance"] = int(exceedances_df["pixel_index"].nunique())
        
        if "timestamp" in exceedances_df.columns:
            stats["first_exceedance"] = exceedances_df["timestamp"].min()
            stats["last_exceedance"] = exceedances_df["timestamp"].max()
        
        # Duration breakdown
        if "duration" in exceedances_df.columns:
            duration_counts = exceedances_df["duration"].value_counts().to_dict()
            stats["exceedances_by_duration"] = duration_counts
    
    logger.info(f"Statistics: {stats.get('catchments_ari_5plus', 0)} catchments with ARI >= 5 years")
    return stats


def get_top_catchments(summary_df: pd.DataFrame, top_n: int = 20) -> pd.DataFrame:
    """
    Get top N catchments by maximum ARI.
    
    Args:
        summary_df: Summary DataFrame
        top_n: Number of top catchments to return
        
    Returns:
        DataFrame with top catchments
    """
    if "max_ari" not in summary_df.columns:
        return pd.DataFrame()
    
    top_df = summary_df.nlargest(top_n, "max_ari").copy()
    return top_df


def get_duration_breakdown(exceedances_df: Optional[pd.DataFrame]) -> pd.DataFrame:
    """
    Get exceedance breakdown by duration.
    
    Args:
        exceedances_df: Exceedances DataFrame
        
    Returns:
        DataFrame with duration counts
    """
    if exceedances_df is None or len(exceedances_df) == 0:
        return pd.DataFrame()
    
    if "duration" not in exceedances_df.columns:
        return pd.DataFrame()
    
    breakdown = exceedances_df.groupby("duration").agg({
        "ari_years": ["count", "mean", "max"],
        "depth_mm": ["mean", "max"],
    }).reset_index()
    
    # Flatten column names
    breakdown.columns = [
        "duration", "count", "mean_ari", "max_ari", "mean_depth_mm", "max_depth_mm"
    ]
    
    # Sort by duration order
    duration_order = ["30m", "60m", "6h", "12h", "24h"]
    breakdown["sort_key"] = breakdown["duration"].apply(
        lambda x: duration_order.index(x) if x in duration_order else 99
    )
    breakdown = breakdown.sort_values("sort_key").drop(columns=["sort_key"])
    
    return breakdown


def create_html_dashboard(
    summary_df: pd.DataFrame,
    exceedances_df: Optional[pd.DataFrame],
    stats: Dict,
    output_dir: Path,
    logger: logging.Logger
) -> Path:
    """
    Create interactive HTML dashboard for analysis results.
    
    Args:
        summary_df: Summary DataFrame
        exceedances_df: Exceedances DataFrame (optional)
        stats: Statistics dictionary
        output_dir: Output directory for HTML file
        logger: Logger instance
        
    Returns:
        Path to created HTML file
    """
    html_file = output_dir / "analysis_dashboard.html"
    
    logger.info(f"Creating HTML dashboard: {html_file}")
    
    # Get top catchments
    top_catchments = get_top_catchments(summary_df)
    
    # Get duration breakdown
    duration_breakdown = get_duration_breakdown(exceedances_df)
    
    # Build HTML
    html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Radar ARI Analysis Dashboard</title>
    <style>
        * {{
            box-sizing: border-box;
        }}
        body {{
            font-family: 'Segoe UI', Arial, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f0f2f5;
            color: #333;
        }}
        .container {{
            background-color: white;
            padding: 30px;
            border-radius: 12px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
            max-width: 1400px;
            margin: 0 auto;
        }}
        h1 {{
            color: #1a1a2e;
            border-bottom: 4px solid #3b82f6;
            padding-bottom: 15px;
            margin-bottom: 30px;
            font-size: 28px;
        }}
        h2 {{
            color: #1a1a2e;
            margin-top: 40px;
            margin-bottom: 20px;
            font-size: 20px;
            border-left: 4px solid #3b82f6;
            padding-left: 15px;
        }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 15px;
            margin: 30px 0;
        }}
        .stat-box {{
            background: linear-gradient(135deg, #3b82f6 0%, #1d4ed8 100%);
            color: white;
            padding: 20px;
            border-radius: 12px;
            text-align: center;
            transition: transform 0.2s;
        }}
        .stat-box:hover {{
            transform: translateY(-3px);
        }}
        .stat-box.warning {{
            background: linear-gradient(135deg, #f59e0b 0%, #d97706 100%);
        }}
        .stat-box.danger {{
            background: linear-gradient(135deg, #ef4444 0%, #dc2626 100%);
        }}
        .stat-box.success {{
            background: linear-gradient(135deg, #10b981 0%, #059669 100%);
        }}
        .stat-value {{
            font-size: 36px;
            font-weight: bold;
            margin: 8px 0;
        }}
        .stat-label {{
            font-size: 12px;
            opacity: 0.9;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
            background: white;
            border-radius: 8px;
            overflow: hidden;
            box-shadow: 0 2px 4px rgba(0,0,0,0.05);
        }}
        th {{
            background-color: #1e3a5f;
            color: white;
            padding: 14px 12px;
            text-align: left;
            font-weight: 600;
            font-size: 13px;
        }}
        td {{
            border-bottom: 1px solid #eee;
            padding: 12px;
            font-size: 14px;
        }}
        tr:hover {{
            background-color: #f8fafc;
        }}
        tr:last-child td {{
            border-bottom: none;
        }}
        .ari-badge {{
            display: inline-block;
            padding: 4px 10px;
            border-radius: 20px;
            font-size: 12px;
            font-weight: bold;
        }}
        .ari-badge.extreme {{
            background-color: #fecaca;
            color: #dc2626;
        }}
        .ari-badge.high {{
            background-color: #fed7aa;
            color: #ea580c;
        }}
        .ari-badge.moderate {{
            background-color: #fef08a;
            color: #ca8a04;
        }}
        .ari-badge.low {{
            background-color: #bbf7d0;
            color: #16a34a;
        }}
        .info-box {{
            background-color: #eff6ff;
            border-left: 4px solid #3b82f6;
            padding: 15px 20px;
            margin: 20px 0;
            border-radius: 0 8px 8px 0;
        }}
        .warning-box {{
            background-color: #fef3c7;
            border-left: 4px solid #f59e0b;
            padding: 15px 20px;
            margin: 20px 0;
            border-radius: 0 8px 8px 0;
        }}
        .progress-bar {{
            background-color: #e5e7eb;
            border-radius: 10px;
            height: 10px;
            overflow: hidden;
        }}
        .progress-fill {{
            height: 100%;
            border-radius: 10px;
            transition: width 0.3s ease;
        }}
        .progress-fill.blue {{
            background: linear-gradient(90deg, #3b82f6, #1d4ed8);
        }}
        .progress-fill.orange {{
            background: linear-gradient(90deg, #f59e0b, #ea580c);
        }}
        .progress-fill.red {{
            background: linear-gradient(90deg, #ef4444, #dc2626);
        }}
        footer {{
            margin-top: 50px;
            padding-top: 20px;
            border-top: 1px solid #eee;
            font-size: 12px;
            color: #666;
            text-align: center;
        }}
        .two-column {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 30px;
        }}
        @media (max-width: 900px) {{
            .two-column {{
                grid-template-columns: 1fr;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>📊 Radar ARI Analysis Dashboard</h1>
        
        <div class="info-box">
            <strong>Analysis Summary:</strong> 
            Analyzed {stats.get("total_catchments", 0)} catchments for ARI (Average Recurrence Interval) exceedances.
            Maximum ARI detected: <strong>{stats.get("max_ari_overall", 0):.1f} years</strong>
        </div>
        
        <div class="stats-grid">
            <div class="stat-box">
                <div class="stat-label">Total Catchments</div>
                <div class="stat-value">{stats.get("total_catchments", 0)}</div>
            </div>
            <div class="stat-box danger">
                <div class="stat-label">Max ARI</div>
                <div class="stat-value">{stats.get("max_ari_overall", 0):.1f}</div>
                <div class="stat-label">years</div>
            </div>
            <div class="stat-box warning">
                <div class="stat-label">ARI ≥ 5 Years</div>
                <div class="stat-value">{stats.get("catchments_ari_5plus", 0)}</div>
                <div class="stat-label">catchments</div>
            </div>
            <div class="stat-box warning">
                <div class="stat-label">ARI ≥ 10 Years</div>
                <div class="stat-value">{stats.get("catchments_ari_10plus", 0)}</div>
                <div class="stat-label">catchments</div>
            </div>
            <div class="stat-box">
                <div class="stat-label">≥25% Area</div>
                <div class="stat-value">{stats.get("catchments_25pct_area", 0)}</div>
                <div class="stat-label">catchments</div>
            </div>
"""
    
    if "total_exceedances" in stats:
        html_content += f"""
            <div class="stat-box success">
                <div class="stat-label">Total Exceedances</div>
                <div class="stat-value">{stats.get("total_exceedances", 0):,}</div>
            </div>
"""
    
    html_content += """
        </div>
        
        <h2>🏆 Top 20 Catchments by Maximum ARI</h2>
"""
    
    if len(top_catchments) > 0:
        html_content += """
        <table>
            <tr>
                <th style="width: 40px;">Rank</th>
                <th>Catchment Name</th>
                <th style="width: 100px;">Max ARI</th>
                <th style="width: 100px;">Duration</th>
                <th style="width: 100px;">Depth (mm)</th>
                <th style="width: 100px;">Area %</th>
                <th style="width: 150px;">Coverage</th>
            </tr>
"""
        max_ari_val = top_catchments["max_ari"].max() if "max_ari" in top_catchments.columns else 1
        
        for rank, (_, row) in enumerate(top_catchments.iterrows(), 1):
            catchment_name = row.get("catchment_name", "Unknown")
            max_ari = row.get("max_ari", 0)
            duration = row.get("peak_duration", "N/A")
            depth = row.get("peak_depth_mm", 0)
            proportion = row.get("proportion_exceeding", 0) * 100
            
            # Determine badge class
            if max_ari >= 50:
                badge_class = "extreme"
            elif max_ari >= 20:
                badge_class = "high"
            elif max_ari >= 10:
                badge_class = "moderate"
            else:
                badge_class = "low"
            
            # Progress bar width
            bar_width = (max_ari / max_ari_val * 100) if max_ari_val > 0 else 0
            bar_class = "red" if max_ari >= 20 else "orange" if max_ari >= 10 else "blue"
            
            html_content += f"""
            <tr>
                <td><strong>{rank}</strong></td>
                <td>{catchment_name}</td>
                <td><span class="ari-badge {badge_class}">{max_ari:.1f} yr</span></td>
                <td>{duration}</td>
                <td>{depth:.1f}</td>
                <td>{proportion:.1f}%</td>
                <td>
                    <div class="progress-bar">
                        <div class="progress-fill {bar_class}" style="width: {bar_width}%"></div>
                    </div>
                </td>
            </tr>
"""
        html_content += """
        </table>
"""
    else:
        html_content += """
        <div class="info-box">
            ✅ No significant ARI exceedances detected during this period.
        </div>
"""
    
    # Add duration breakdown if available
    if len(duration_breakdown) > 0:
        html_content += """
        <h2>⏱️ Exceedances by Duration</h2>
        <table>
            <tr>
                <th>Duration</th>
                <th>Count</th>
                <th>Mean ARI (yr)</th>
                <th>Max ARI (yr)</th>
                <th>Mean Depth (mm)</th>
                <th>Max Depth (mm)</th>
            </tr>
"""
        for _, row in duration_breakdown.iterrows():
            html_content += f"""
            <tr>
                <td><strong>{row['duration']}</strong></td>
                <td>{int(row['count']):,}</td>
                <td>{row['mean_ari']:.1f}</td>
                <td>{row['max_ari']:.1f}</td>
                <td>{row['mean_depth_mm']:.1f}</td>
                <td>{row['max_depth_mm']:.1f}</td>
            </tr>
"""
        html_content += """
        </table>
"""
    
    # ARI Distribution section
    html_content += """
        <h2>📈 ARI Distribution</h2>
        <div class="two-column">
            <div>
                <h3 style="font-size: 16px; margin-bottom: 15px;">By ARI Threshold</h3>
                <table>
                    <tr>
                        <th>Threshold</th>
                        <th>Catchments</th>
                    </tr>
"""
    
    ari_thresholds = [
        ("ARI ≥ 5 years", stats.get("catchments_ari_5plus", 0)),
        ("ARI ≥ 10 years", stats.get("catchments_ari_10plus", 0)),
        ("ARI ≥ 20 years", stats.get("catchments_ari_20plus", 0)),
        ("ARI ≥ 50 years", stats.get("catchments_ari_50plus", 0)),
    ]
    
    for label, count in ari_thresholds:
        html_content += f"""
                    <tr>
                        <td>{label}</td>
                        <td><strong>{count}</strong></td>
                    </tr>
"""
    
    html_content += """
                </table>
            </div>
            <div>
                <h3 style="font-size: 16px; margin-bottom: 15px;">By Area Coverage</h3>
                <table>
                    <tr>
                        <th>Coverage</th>
                        <th>Catchments</th>
                    </tr>
"""
    
    area_thresholds = [
        ("≥ 10% area exceeding", stats.get("catchments_10pct_area", 0)),
        ("≥ 25% area exceeding", stats.get("catchments_25pct_area", 0)),
        ("≥ 50% area exceeding", stats.get("catchments_50pct_area", 0)),
    ]
    
    for label, count in area_thresholds:
        html_content += f"""
                    <tr>
                        <td>{label}</td>
                        <td><strong>{count}</strong></td>
                    </tr>
"""
    
    html_content += """
                </table>
            </div>
        </div>
"""
    
    # Interpretation section
    html_content += """
        <div class="warning-box" style="margin-top: 30px;">
            <strong>Understanding ARI Values:</strong><br>
            <ul style="margin: 10px 0 0 20px; padding: 0;">
                <li><strong>ARI 5 years:</strong> Rainfall intensity expected once every 5 years on average</li>
                <li><strong>ARI 10 years:</strong> More severe, design standard for minor drainage</li>
                <li><strong>ARI 50+ years:</strong> Extreme event, potential flooding risk</li>
                <li><strong>Area Coverage:</strong> Percentage of catchment pixels exceeding threshold</li>
            </ul>
        </div>
"""
    
    html_content += f"""
        <footer>
            <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            <p><strong>Moata AlertLab v{__version__}</strong> - Radar ARI Analysis Visualization</p>
            <p>Auckland Council Internship Project (COMPSCI 778)</p>
        </footer>
    </div>
</body>
</html>
"""
    
    # Write HTML file
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(html_content)
    
    logger.info(f"✓ Dashboard created: {html_file}")
    return html_file


def save_statistics_csv(
    summary_df: pd.DataFrame,
    stats: Dict,
    output_dir: Path,
    logger: logging.Logger
) -> Path:
    """
    Save analysis statistics to CSV.
    
    Args:
        summary_df: Summary DataFrame
        stats: Statistics dictionary
        output_dir: Output directory
        logger: Logger instance
        
    Returns:
        Path to created CSV file
    """
    stats_file = output_dir / "analysis_statistics.csv"
    
    logger.info(f"Saving statistics to: {stats_file}")
    
    # Create statistics rows
    stats_rows = [
        {"metric": k, "value": str(v)} 
        for k, v in stats.items() 
        if not isinstance(v, dict)
    ]
    
    stats_df = pd.DataFrame(stats_rows)
    stats_df.to_csv(stats_file, index=False)
    
    logger.info(f"✓ Saved {len(stats_rows)} statistics")
    return stats_file


def save_summary_report(
    stats: Dict, 
    output_dir: Path, 
    logger: logging.Logger
) -> Path:
    """
    Save text summary report.
    
    Args:
        stats: Statistics dictionary
        output_dir: Output directory
        logger: Logger instance
        
    Returns:
        Path to created report file
    """
    report_file = output_dir / "visualization_report.txt"
    
    logger.info(f"Saving summary report to: {report_file}")
    
    report = f"""
════════════════════════════════════════════════════════════════════════════════
                    RADAR ARI ANALYSIS VISUALIZATION REPORT
════════════════════════════════════════════════════════════════════════════════

Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

CATCHMENT STATISTICS
────────────────────────────────────────────────────────────────────────────────
  Total Catchments Analyzed:    {stats.get("total_catchments", 0)}
  Maximum ARI Detected:         {stats.get("max_ari_overall", 0):.1f} years
  Mean ARI:                     {stats.get("mean_ari", 0):.1f} years
  Median ARI:                   {stats.get("median_ari", 0):.1f} years

ARI THRESHOLD SUMMARY
────────────────────────────────────────────────────────────────────────────────
  Catchments with ARI ≥ 5 yr:   {stats.get("catchments_ari_5plus", 0)}
  Catchments with ARI ≥ 10 yr:  {stats.get("catchments_ari_10plus", 0)}
  Catchments with ARI ≥ 20 yr:  {stats.get("catchments_ari_20plus", 0)}
  Catchments with ARI ≥ 50 yr:  {stats.get("catchments_ari_50plus", 0)}

AREA COVERAGE SUMMARY
────────────────────────────────────────────────────────────────────────────────
  Catchments with ≥ 10% area:   {stats.get("catchments_10pct_area", 0)}
  Catchments with ≥ 25% area:   {stats.get("catchments_25pct_area", 0)}
  Catchments with ≥ 50% area:   {stats.get("catchments_50pct_area", 0)}
  Maximum Area Coverage:        {stats.get("max_proportion", 0):.1f}%

EXCEEDANCE SUMMARY
────────────────────────────────────────────────────────────────────────────────
  Total Exceedance Records:     {stats.get("total_exceedances", "N/A")}
  Unique Pixels with Exceedance:{stats.get("unique_pixels_with_exceedance", "N/A")}

INTERPRETATION
────────────────────────────────────────────────────────────────────────────────
  • ARI (Average Recurrence Interval) indicates the statistical return period
    of rainfall intensity
    
  • Higher ARI values indicate more extreme rainfall events
  
  • Area coverage shows what proportion of the catchment experienced
    rainfall intensity exceeding the ARI threshold
    
  • Catchments with both high ARI and high area coverage are of
    greatest concern for potential flooding

════════════════════════════════════════════════════════════════════════════════
Moata AlertLab v{__version__} - Radar ARI Analysis Visualization
Auckland Council (COMPSCI 778 Internship Project)
"""
    
    with open(report_file, "w", encoding="utf-8") as f:
        f.write(report)
    
    logger.info(f"✓ Summary report saved: {report_file}")
    return report_file


def main() -> int:
    """Main entry point."""
    args = parse_args()
    logger = setup_script_logger(args.log_level, __name__)
    
    try:
        logger.info("=" * 80)
        logger.info("Radar ARI Analysis Visualization - v%s", __version__)
        logger.info("=" * 80)
        
        # Determine paths
        if args.date:
            paths = PipelinePaths.for_date(args.date)
            logger.info(f"Date: {args.date}")
        else:
            paths = PipelinePaths.for_today()
            logger.info("Date: Today")
        
        # Input directory (ANALYSIS results, not alarms!)
        if args.input:
            input_dir = args.input
        else:
            input_dir = paths.rain_radar_analysis_dir
        
        # Output directory (visualizations)
        if args.output:
            output_dir = args.output
        else:
            output_dir = paths.rain_radar_viz_dir
        
        # Create output directory
        output_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Input directory:  {input_dir}")
        logger.info(f"Output directory: {output_dir}")
        logger.info("=" * 80)
        logger.info("")
        
        # Find and load analysis data
        summary_file, exceedances_file = find_analysis_data(input_dir, logger, paths=paths)
        summary_df, exceedances_df = load_analysis_data(summary_file, exceedances_file, logger)
        
        logger.info("")
        
        # Generate statistics
        stats = generate_statistics(summary_df, exceedances_df, logger)
        
        logger.info("")
        logger.info("Generating visualizations...")
        logger.info("-" * 40)
        
        # Create HTML dashboard
        dashboard_path = create_html_dashboard(summary_df, exceedances_df, stats, output_dir, logger)
        
        # Save statistics CSV
        stats_path = save_statistics_csv(summary_df, stats, output_dir, logger)
        
        # Save summary report
        report_path = save_summary_report(stats, output_dir, logger)
        
        logger.info("")
        logger.info("=" * 80)
        logger.info("✅ Analysis visualization completed successfully")
        logger.info("=" * 80)
        logger.info("")
        logger.info("Output files:")
        logger.info(f"  • {dashboard_path.name}")
        logger.info(f"  • {stats_path.name}")
        logger.info(f"  • {report_path.name}")
        logger.info("")
        
        # Prepare dashboard URL
        dashboard_url = dashboard_path.absolute().as_uri()
        
        # Open dashboard in browser (unless --no-open)
        if not args.no_open:
            # Show prompt and wait for user input
            print("")
            print("=" * 60)
            print("  VISUALIZATION COMPLETE")
            print("=" * 60)
            print("")
            print("  Files created:")
            print(f"  • {dashboard_path.name}")
            print(f"  • {stats_path.name}")
            print(f"  • {report_path.name}")
            print("")
            print("=" * 60)
            input("  Press ENTER to open dashboard in browser...")
            print("")
            
            # Open dashboard in browser AFTER user presses ENTER
            import webbrowser
            logger.info(f"Opening dashboard in browser...")
            try:
                webbrowser.open(dashboard_url)
                logger.info(f"✓ Opened: {dashboard_url}")
            except Exception as e:
                logger.warning(f"Could not open browser automatically: {e}")
                logger.info(f"Please open manually: {dashboard_path.absolute()}")
        else:
            logger.info(f"Dashboard ready: {dashboard_path.absolute()}")
        
        return 0
        
    except FileNotFoundError as e:
        logger.error("")
        logger.error("=" * 80)
        logger.error("❌ File not found")
        logger.error("=" * 80)
        logger.error(str(e))
        logger.error("")
        logger.error("Make sure you have run the 'Analyze Data' step first:")
        logger.error("  python scripts/radar/analyze.py --date YYYY-MM-DD")
        return 1
        
    except Exception as e:
        logger.error("")
        logger.error("=" * 80)
        logger.error(f"❌ Error: {e}")
        logger.error("=" * 80)
        logger.exception("Full traceback:")
        return 1


if __name__ == "__main__":
    sys.exit(main())