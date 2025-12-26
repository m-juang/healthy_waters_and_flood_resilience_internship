"""
Visualize rain gauge ARI alarm validation results.

Creates charts showing validation status, exceedances, and statistics.

Usage:
    python visualize_ari_alarms_rain_gauges.py

Output:
    outputs/rain_gauges/validation_viz/
    ├── validation_summary.png
    ├── top_exceedances.png
    ├── validation_dashboard.html
    └── validation_stats.csv
"""
from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt

from moata_pipeline.logging_setup import setup_logging

logger = logging.getLogger(__name__)

# Config
INPUT_CSV = Path("outputs/rain_gauges/ari_alarm_validation.csv")
OUT_DIR = Path("outputs/rain_gauges/validation_viz")


def create_status_chart(df: pd.DataFrame, out_dir: Path) -> None:
    """Create pie chart of validation status."""
    status_counts = df["status"].value_counts()
    
    colors = {
        "VERIFIED": "#28a745",
        "NOT_VERIFIED": "#dc3545", 
        "UNVERIFIABLE": "#6c757d",
    }
    
    plt.figure(figsize=(8, 6))
    plt.pie(
        status_counts.values,
        labels=status_counts.index,
        autopct="%1.1f%%",
        colors=[colors.get(s, "#999") for s in status_counts.index],
        startangle=90,
    )
    plt.title("ARI Alarm Validation Status")
    plt.tight_layout()
    plt.savefig(out_dir / "validation_summary.png", dpi=200)
    plt.close()
    logger.info("✓ Saved validation_summary.png")


def create_exceedance_chart(df: pd.DataFrame, out_dir: Path) -> None:
    """Create bar chart of top exceedances."""
    df = df.copy()
    df["max_ari_value"] = pd.to_numeric(df["max_ari_value"], errors="coerce")
    df["threshold"] = pd.to_numeric(df["threshold"], errors="coerce")
    df["exceed_by"] = df["max_ari_value"] - df["threshold"]
    
    top = (
        df.dropna(subset=["exceed_by"])
        .sort_values("exceed_by", ascending=False)
        .head(10)
    )
    
    if top.empty:
        logger.warning("No exceedance data to plot")
        return
    
    plt.figure(figsize=(10, 6))
    bars = plt.barh(top["gauge_name"], top["exceed_by"], color="#667eea")
    plt.gca().invert_yaxis()
    plt.axvline(0, linestyle="--", linewidth=1, color="#999")
    
    plt.title("Top 10 ARI Exceedances (Above Threshold)")
    plt.xlabel("Exceedance (ARI years above threshold)")
    plt.ylabel("Rain Gauge")
    
    plt.tight_layout()
    plt.savefig(out_dir / "top_exceedances.png", dpi=200)
    plt.close()
    logger.info("✓ Saved top_exceedances.png")


def create_html_dashboard(df: pd.DataFrame, out_dir: Path) -> Path:
    """Create HTML dashboard for validation results."""
    df = df.copy()
    df["max_ari_value"] = pd.to_numeric(df["max_ari_value"], errors="coerce")
    
    total = len(df)
    verified = (df["status"] == "VERIFIED").sum()
    not_verified = (df["status"] == "NOT_VERIFIED").sum()
    unverifiable = (df["status"] == "UNVERIFIABLE").sum()
    
    avg_ari = df["max_ari_value"].mean()
    max_ari = df["max_ari_value"].max()
    
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Rain Gauge ARI Alarm Validation</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: 'Segoe UI', sans-serif; background: #f5f5f5; padding: 20px; }}
        .container {{ max-width: 1200px; margin: 0 auto; }}
        .header {{ background: linear-gradient(135deg, #28a745, #20c997); color: white; padding: 30px; border-radius: 10px; margin-bottom: 20px; text-align: center; }}
        .header h1 {{ font-size: 2em; margin-bottom: 5px; }}
        .stats-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 15px; margin-bottom: 20px; }}
        .stat-card {{ background: white; padding: 20px; border-radius: 10px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); text-align: center; }}
        .stat-card .value {{ font-size: 2em; font-weight: bold; }}
        .stat-card .label {{ color: #666; font-size: 0.9em; margin-top: 5px; }}
        .stat-card.verified .value {{ color: #28a745; }}
        .stat-card.not-verified .value {{ color: #dc3545; }}
        .stat-card.unverifiable .value {{ color: #6c757d; }}
        .section {{ background: white; padding: 25px; border-radius: 10px; margin-bottom: 20px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }}
        .section h2 {{ color: #333; margin-bottom: 15px; }}
        .charts {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(400px, 1fr)); gap: 20px; }}
        .chart img {{ max-width: 100%; border-radius: 8px; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 15px; }}
        th {{ background: #667eea; color: white; padding: 12px; text-align: left; }}
        td {{ padding: 10px 12px; border-bottom: 1px solid #eee; }}
        tr:hover {{ background: #f9f9f9; }}
        .status-verified {{ color: #28a745; font-weight: bold; }}
        .status-not-verified {{ color: #dc3545; font-weight: bold; }}
        .status-unverifiable {{ color: #6c757d; }}
        .footer {{ text-align: center; color: #666; padding: 20px; font-size: 0.9em; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🌧️ Rain Gauge ARI Alarm Validation</h1>
            <div>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</div>
        </div>
        
        <div class="stats-grid">
            <div class="stat-card">
                <div class="value">{total}</div>
                <div class="label">Total Alarms</div>
            </div>
            <div class="stat-card verified">
                <div class="value">{verified}</div>
                <div class="label">Verified</div>
            </div>
            <div class="stat-card not-verified">
                <div class="value">{not_verified}</div>
                <div class="label">Not Verified</div>
            </div>
            <div class="stat-card unverifiable">
                <div class="value">{unverifiable}</div>
                <div class="label">Unverifiable</div>
            </div>
            <div class="stat-card">
                <div class="value">{avg_ari:.1f}</div>
                <div class="label">Avg Max ARI</div>
            </div>
            <div class="stat-card">
                <div class="value">{max_ari:.1f}</div>
                <div class="label">Peak ARI</div>
            </div>
        </div>
        
        <div class="section">
            <h2>📊 Charts</h2>
            <div class="charts">
                <div class="chart"><img src="validation_summary.png" alt="Validation Summary"></div>
                <div class="chart"><img src="top_exceedances.png" alt="Top Exceedances"></div>
            </div>
        </div>
        
        <div class="section">
            <h2>📋 All Validation Results</h2>
            <table>
                <thead>
                    <tr><th>Gauge</th><th>Alarm Time (UTC)</th><th>Max ARI</th><th>Threshold</th><th>Status</th></tr>
                </thead>
                <tbody>
"""
    
    for _, row in df.iterrows():
        status_class = f"status-{row['status'].lower().replace('_', '-')}"
        max_ari_val = row['max_ari_value'] if pd.notna(row['max_ari_value']) else '-'
        if isinstance(max_ari_val, float):
            max_ari_val = f"{max_ari_val:.1f}"
        
        html += f"""                    <tr>
                        <td>{row['gauge_name']}</td>
                        <td>{row['alarm_time_utc']}</td>
                        <td>{max_ari_val}</td>
                        <td>{row['threshold']}</td>
                        <td class="{status_class}">{row['status']}</td>
                    </tr>
"""
    
    html += f"""                </tbody>
            </table>
        </div>
        
        <div class="footer">
            Rain Gauge ARI Alarm Validation | Auckland Council | {datetime.now().strftime('%Y-%m-%d')}
        </div>
    </div>
</body>
</html>"""
    
    output_path = out_dir / "validation_dashboard.html"
    output_path.write_text(html, encoding="utf-8")
    logger.info("✓ Saved validation_dashboard.html")
    return output_path


def main() -> None:
    setup_logging("INFO")
    
    logger.info("=" * 80)
    logger.info("RAIN GAUGE ARI ALARM VALIDATION VISUALIZATION")
    logger.info("=" * 80)
    
    if not INPUT_CSV.exists():
        logger.error("Input file not found: %s", INPUT_CSV)
        logger.error("Run 'python validate_ari_alarms_rain_gauges.py' first")
        return
    
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    
    logger.info("Loading validation data from %s", INPUT_CSV)
    df = pd.read_csv(INPUT_CSV)
    logger.info("✓ Loaded %d records", len(df))
    
    # Create visualizations
    create_status_chart(df, OUT_DIR)
    create_exceedance_chart(df, OUT_DIR)
    dashboard_path = create_html_dashboard(df, OUT_DIR)
    
    # Save stats
    stats_path = OUT_DIR / "validation_stats.csv"
    df.to_csv(stats_path, index=False)
    logger.info("✓ Saved validation_stats.csv")
    
    logger.info("=" * 80)
    logger.info("COMPLETE!")
    logger.info("=" * 80)
    logger.info("Output directory: %s", OUT_DIR)
    logger.info("Dashboard: %s", dashboard_path)
    logger.info("=" * 80)
    
    print(f"\n✅ Done! Open in browser: {dashboard_path}")


if __name__ == "__main__":
    main()
