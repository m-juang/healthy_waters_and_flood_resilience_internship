"""
Visualize rain radar ARI alarm validation results.

Creates charts showing ARI exceedances and catchment statistics.

Usage:
    python visualize_ari_alarms_rain_radar.py                   # Auto-detect
    python visualize_ari_alarms_rain_radar.py --date 2025-05-09 # Specific date

Output:
    outputs/rain_radar/[validation_viz|historical/DATE/validation_viz]/
    ├── ari_distribution.png
    ├── top_catchments.png
    ├── proportion_distribution.png
    ├── validation_dashboard.html
    └── validation_stats.csv
"""
from __future__ import annotations

import argparse
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt

from moata_pipeline.logging_setup import setup_logging

logger = logging.getLogger(__name__)


def create_ari_distribution_chart(df: pd.DataFrame, out_dir: Path) -> None:
    """Create histogram of max ARI values."""
    ari_values = df[df["max_ari"] > 0]["max_ari"]
    
    if ari_values.empty:
        logger.warning("No ARI values > 0 to plot")
        return
    
    plt.figure(figsize=(10, 6))
    plt.hist(ari_values, bins=30, color="#667eea", edgecolor="white", alpha=0.8)
    plt.axvline(5, color="#dc3545", linestyle="--", linewidth=2, label="Threshold (5 years)")
    
    plt.title("Distribution of Max ARI Values Across Catchments")
    plt.xlabel("Max ARI (years)")
    plt.ylabel("Number of Catchments")
    plt.legend()
    
    plt.tight_layout()
    plt.savefig(out_dir / "ari_distribution.png", dpi=200)
    plt.close()
    logger.info("✓ Saved ari_distribution.png")


def create_top_catchments_chart(df: pd.DataFrame, out_dir: Path) -> None:
    """Create bar chart of top catchments by max ARI."""
    top = df.nlargest(15, "max_ari")
    
    if top.empty:
        logger.warning("No data for top catchments chart")
        return
    
    plt.figure(figsize=(12, 8))
    colors = ["#dc3545" if row["proportion_exceeding"] >= 0.30 else "#667eea" for _, row in top.iterrows()]
    
    bars = plt.barh(top["catchment_name"], top["max_ari"], color=colors)
    plt.gca().invert_yaxis()
    plt.axvline(5, color="#dc3545", linestyle="--", linewidth=2, label="ARI Threshold (5 years)")
    
    plt.title("Top 15 Catchments by Max ARI")
    plt.xlabel("Max ARI (years)")
    plt.ylabel("Catchment")
    plt.legend()
    
    plt.tight_layout()
    plt.savefig(out_dir / "top_catchments.png", dpi=200)
    plt.close()
    logger.info("✓ Saved top_catchments.png")


def create_proportion_chart(df: pd.DataFrame, out_dir: Path) -> None:
    """Create chart of proportion exceeding distribution."""
    proportions = df["proportion_exceeding"] * 100
    
    plt.figure(figsize=(10, 6))
    plt.hist(proportions, bins=20, color="#28a745", edgecolor="white", alpha=0.8)
    plt.axvline(30, color="#dc3545", linestyle="--", linewidth=2, label="Alarm Threshold (30%)")
    
    plt.title("Distribution of Area Exceeding ARI Threshold")
    plt.xlabel("Proportion of Catchment Area Exceeding (%)")
    plt.ylabel("Number of Catchments")
    plt.legend()
    
    plt.tight_layout()
    plt.savefig(out_dir / "proportion_distribution.png", dpi=200)
    plt.close()
    logger.info("✓ Saved proportion_distribution.png")


def create_html_dashboard(df: pd.DataFrame, out_dir: Path, data_date: str = None) -> Path:
    """Create HTML dashboard for radar validation results."""
    total = len(df)
    would_alarm = (df["alarm_status"] == "ALARM").sum()
    ok = total - would_alarm
    
    avg_ari = df["max_ari"].mean()
    max_ari = df["max_ari"].max()
    avg_proportion = df["proportion_exceeding"].mean() * 100
    
    date_display = f"Data Date: {data_date}" if data_date else "Data: Last 24 hours"
    
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Rain Radar ARI Validation Dashboard</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: 'Segoe UI', sans-serif; background: #f5f5f5; padding: 20px; }}
        .container {{ max-width: 1400px; margin: 0 auto; }}
        .header {{ background: linear-gradient(135deg, #1e3c72, #2a5298); color: white; padding: 30px; border-radius: 10px; margin-bottom: 20px; text-align: center; }}
        .header h1 {{ font-size: 2em; margin-bottom: 5px; }}
        .header .meta {{ margin-top: 10px; opacity: 0.8; font-size: 0.9em; }}
        .stats-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 15px; margin-bottom: 20px; }}
        .stat-card {{ background: white; padding: 20px; border-radius: 10px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); text-align: center; }}
        .stat-card .value {{ font-size: 2em; font-weight: bold; color: #1e3c72; }}
        .stat-card .label {{ color: #666; font-size: 0.9em; margin-top: 5px; }}
        .stat-card.alarm .value {{ color: #dc3545; }}
        .stat-card.ok .value {{ color: #28a745; }}
        .section {{ background: white; padding: 25px; border-radius: 10px; margin-bottom: 20px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }}
        .section h2 {{ color: #333; margin-bottom: 15px; }}
        .charts {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(400px, 1fr)); gap: 20px; }}
        .chart img {{ max-width: 100%; border-radius: 8px; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 15px; }}
        th {{ background: #667eea; color: white; padding: 12px; text-align: left; }}
        td {{ padding: 10px 12px; border-bottom: 1px solid #eee; }}
        tr:hover {{ background: #f9f9f9; }}
        .status-alarm {{ color: #dc3545; font-weight: bold; }}
        .status-ok {{ color: #28a745; }}
        .search-box {{ width: 100%; padding: 12px; border: 2px solid #ddd; border-radius: 8px; margin-bottom: 15px; }}
        .footer {{ text-align: center; color: #666; padding: 20px; font-size: 0.9em; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🌧️ Rain Radar ARI Validation Dashboard</h1>
            <div class="meta">{date_display} | Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</div>
        </div>
        
        <div class="stats-grid">
            <div class="stat-card">
                <div class="value">{total}</div>
                <div class="label">Total Catchments</div>
            </div>
            <div class="stat-card alarm">
                <div class="value">{would_alarm}</div>
                <div class="label">Would Alarm</div>
            </div>
            <div class="stat-card ok">
                <div class="value">{ok}</div>
                <div class="label">OK</div>
            </div>
            <div class="stat-card">
                <div class="value">{max_ari:.1f}</div>
                <div class="label">Peak ARI (years)</div>
            </div>
            <div class="stat-card">
                <div class="value">{avg_ari:.1f}</div>
                <div class="label">Avg Max ARI</div>
            </div>
            <div class="stat-card">
                <div class="value">{avg_proportion:.1f}%</div>
                <div class="label">Avg Area Exceeding</div>
            </div>
        </div>
        
        <div class="section">
            <h2>📊 Charts</h2>
            <div class="charts">
                <div class="chart"><img src="ari_distribution.png" alt="ARI Distribution"></div>
                <div class="chart"><img src="proportion_distribution.png" alt="Proportion Distribution"></div>
                <div class="chart"><img src="top_catchments.png" alt="Top Catchments"></div>
            </div>
        </div>
        
        <div class="section">
            <h2>🚨 Catchments That Would Alarm (≥30% area exceeding)</h2>
            <table>
                <thead>
                    <tr><th>Catchment</th><th>Max ARI</th><th>Pixels Total</th><th>Pixels Exceeding</th><th>Proportion</th><th>Peak Duration</th></tr>
                </thead>
                <tbody>
"""
    
    alarms = df[df["alarm_status"] == "ALARM"].sort_values("proportion_exceeding", ascending=False)
    for _, row in alarms.iterrows():
        html += f"""                    <tr>
                        <td>{row['catchment_name']}</td>
                        <td>{row['max_ari']:.1f}</td>
                        <td>{row['pixels_total']}</td>
                        <td>{row['pixels_exceeding']}</td>
                        <td class="status-alarm">{row['proportion_exceeding']*100:.1f}%</td>
                        <td>{row.get('peak_duration', '-')}</td>
                    </tr>
"""
    
    if alarms.empty:
        html += """                    <tr><td colspan="6" style="text-align: center; color: #666;">No catchments exceed alarm threshold</td></tr>
"""
    
    html += f"""                </tbody>
            </table>
        </div>
        
        <div class="section">
            <h2>📋 All Catchments</h2>
            <input type="text" id="search" class="search-box" placeholder="🔍 Search catchments...">
            <table id="allTable">
                <thead>
                    <tr><th>Catchment</th><th>Max ARI</th><th>Pixels</th><th>Exceeding</th><th>Proportion</th><th>Status</th></tr>
                </thead>
                <tbody>
"""
    
    for _, row in df.sort_values("max_ari", ascending=False).iterrows():
        status_class = "status-alarm" if row["alarm_status"] == "ALARM" else "status-ok"
        html += f"""                    <tr class="data-row">
                        <td>{row['catchment_name']}</td>
                        <td>{row['max_ari']:.1f}</td>
                        <td>{row['pixels_total']}</td>
                        <td>{row['pixels_exceeding']}</td>
                        <td>{row['proportion_exceeding']*100:.1f}%</td>
                        <td class="{status_class}">{row['alarm_status']}</td>
                    </tr>
"""
    
    html += f"""                </tbody>
            </table>
        </div>
        
        <div class="footer">
            Rain Radar ARI Validation | Auckland Council | {total} Catchments | {datetime.now().strftime('%Y-%m-%d')}
        </div>
    </div>
    
    <script>
        document.getElementById('search').addEventListener('keyup', function() {{
            const q = this.value.toLowerCase();
            document.querySelectorAll('.data-row').forEach(row => {{
                row.style.display = row.textContent.toLowerCase().includes(q) ? '' : 'none';
            }});
        }});
    </script>
</body>
</html>"""
    
    output_path = out_dir / "validation_dashboard.html"
    output_path.write_text(html, encoding="utf-8")
    logger.info("✓ Saved validation_dashboard.html")
    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Visualize rain radar ARI validation results")
    parser.add_argument(
        "--date",
        metavar="YYYY-MM-DD",
        help="Visualize validation for specific date",
    )
    parser.add_argument(
        "--input",
        metavar="PATH",
        help="Path to ari_alarm_validation.csv (overrides --date)",
    )
    
    args = parser.parse_args()
    
    setup_logging("INFO")
    
    logger.info("=" * 80)
    logger.info("RAIN RADAR ARI VALIDATION VISUALIZATION")
    logger.info("=" * 80)
    
    # Find input file
    data_date = None
    if args.input:
        input_path = Path(args.input)
        out_dir = input_path.parent / "validation_viz"
    elif args.date:
        input_path = Path(f"outputs/rain_radar/historical/{args.date}/ari_alarm_validation.csv")
        out_dir = input_path.parent / "validation_viz"
        data_date = args.date
    else:
        # Auto-detect
        candidates = [
            *sorted(Path("outputs/rain_radar/historical").glob("*/ari_alarm_validation.csv")),
            Path("outputs/rain_radar/ari_alarm_validation.csv"),
        ]
        input_path = None
        for p in reversed(candidates):
            if p.exists():
                input_path = p
                # Extract date from path if historical
                if "historical" in str(p):
                    data_date = p.parent.name
                break
        
        if not input_path:
            logger.error("No validation file found.")
            logger.error("Run 'python validate_ari_alarms_rain_radar.py' first")
            return
        
        out_dir = input_path.parent / "validation_viz"
    
    if not input_path.exists():
        logger.error("Input file not found: %s", input_path)
        return
    
    out_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info("Input: %s", input_path)
    logger.info("Output: %s", out_dir)
    
    # Load data
    df = pd.read_csv(input_path)
    logger.info("✓ Loaded %d records", len(df))
    
    # Create visualizations
    create_ari_distribution_chart(df, out_dir)
    create_top_catchments_chart(df, out_dir)
    create_proportion_chart(df, out_dir)
    dashboard_path = create_html_dashboard(df, out_dir, data_date=data_date)
    
    # Save stats
    stats_path = out_dir / "validation_stats.csv"
    df.to_csv(stats_path, index=False)
    logger.info("✓ Saved validation_stats.csv")
    
    logger.info("=" * 80)
    logger.info("COMPLETE!")
    logger.info("=" * 80)
    logger.info("Output directory: %s", out_dir)
    logger.info("Dashboard: %s", dashboard_path)
    logger.info("=" * 80)
    
    print(f"\n✅ Done! Open in browser: {dashboard_path}")


if __name__ == "__main__":
    main()
