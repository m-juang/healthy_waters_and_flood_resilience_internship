"""
HTML dashboard generation for radar data.
"""
from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def build_radar_dashboard(df: pd.DataFrame, out_dir: Path, data_date: str = None) -> Path:
    """
    Build HTML dashboard for radar data.
    
    Args:
        df: DataFrame with catchment statistics
        out_dir: Output directory
        data_date: Optional date string for display
        
    Returns:
        Path to generated HTML file
    """
    logger.info("Building radar dashboard...")
    
    total_catchments = len(df)
    catchments_with_data = int(df["has_data"].sum())
    catchments_with_rain = int((df["total_rainfall"] > 0).sum())
    total_rainfall = df["total_rainfall"].sum()
    total_pixels = int(df["pixel_count"].sum())
    
    by_rainfall = df.nlargest(20, "total_rainfall")
    by_intensity = df.nlargest(20, "max_intensity")
    
    date_display = f"Data Date: {data_date}" if data_date else "Data: Last 24 hours"
    
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Rain Radar Dashboard - Auckland Council</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: 'Segoe UI', sans-serif; background: #f5f5f5; padding: 20px; }}
        .container {{ max-width: 1400px; margin: 0 auto; }}
        .header {{ background: linear-gradient(135deg, #1e3c72, #2a5298); color: white; padding: 30px; border-radius: 10px; margin-bottom: 20px; text-align: center; }}
        .header h1 {{ font-size: 2em; margin-bottom: 5px; }}
        .header .subtitle {{ opacity: 0.9; }}
        .header .meta {{ margin-top: 10px; opacity: 0.8; font-size: 0.9em; }}
        .stats-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 15px; margin-bottom: 20px; }}
        .stat-card {{ background: white; padding: 20px; border-radius: 10px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); text-align: center; }}
        .stat-card .value {{ font-size: 2em; font-weight: bold; color: #1e3c72; }}
        .stat-card .label {{ color: #666; font-size: 0.9em; margin-top: 5px; }}
        .section {{ background: white; padding: 25px; border-radius: 10px; margin-bottom: 20px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }}
        .section h2 {{ color: #1e3c72; margin-bottom: 15px; border-bottom: 2px solid #667eea; padding-bottom: 10px; }}
        table {{ width: 100%; border-collapse: collapse; }}
        th {{ background: #667eea; color: white; padding: 12px; text-align: left; }}
        td {{ padding: 10px 12px; border-bottom: 1px solid #eee; }}
        tr:hover {{ background: #f9f9f9; }}
        .search-box {{ width: 100%; padding: 12px; border: 2px solid #ddd; border-radius: 8px; margin-bottom: 15px; font-size: 1em; }}
        .search-box:focus {{ outline: none; border-color: #667eea; }}
        .has-rain {{ color: #28a745; font-weight: bold; }}
        .no-rain {{ color: #999; }}
        .footer {{ text-align: center; color: #666; padding: 20px; font-size: 0.9em; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🌧️ Rain Radar Dashboard</h1>
            <div class="subtitle">Auckland Council - Stormwater Catchments QPE Analysis</div>
            <div class="meta">{date_display} | Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</div>
        </div>
        
        <div class="stats-grid">
            <div class="stat-card">
                <div class="value">{total_catchments}</div>
                <div class="label">Total Catchments</div>
            </div>
            <div class="stat-card">
                <div class="value">{catchments_with_data}</div>
                <div class="label">With Data</div>
            </div>
            <div class="stat-card">
                <div class="value">{catchments_with_rain}</div>
                <div class="label">With Rainfall</div>
            </div>
            <div class="stat-card">
                <div class="value">{total_pixels:,}</div>
                <div class="label">Total Pixels</div>
            </div>
            <div class="stat-card">
                <div class="value">{total_rainfall:.1f}</div>
                <div class="label">Total Rainfall (mm)</div>
            </div>
        </div>
        
        <div class="section">
            <h2>🏆 Top 20 by Total Rainfall</h2>
            <table>
                <thead>
                    <tr><th>#</th><th>Catchment</th><th>Total (mm)</th><th>Pixels</th><th>Coverage</th></tr>
                </thead>
                <tbody>
"""
    
    for i, (_, row) in enumerate(by_rainfall.iterrows(), 1):
        html += f"""                    <tr>
                        <td>{i}</td>
                        <td>{row['catchment_name']}</td>
                        <td class="has-rain">{row['total_rainfall']:.2f}</td>
                        <td>{row['pixel_count']}</td>
                        <td>{row['rain_coverage_pct']:.1f}%</td>
                    </tr>
"""
    
    html += """                </tbody>
            </table>
        </div>
        
        <div class="section">
            <h2>⚡ Top 20 by Peak Intensity</h2>
            <table>
                <thead>
                    <tr><th>#</th><th>Catchment</th><th>Max (mm/min)</th><th>Total (mm)</th><th>Pixels</th></tr>
                </thead>
                <tbody>
"""
    
    for i, (_, row) in enumerate(by_intensity.iterrows(), 1):
        html += f"""                    <tr>
                        <td>{i}</td>
                        <td>{row['catchment_name']}</td>
                        <td>{row['max_intensity']:.3f}</td>
                        <td>{row['total_rainfall']:.2f}</td>
                        <td>{row['pixel_count']}</td>
                    </tr>
"""
    
    html += """                </tbody>
            </table>
        </div>
        
        <div class="section">
            <h2>📋 All Catchments</h2>
            <input type="text" id="search" class="search-box" placeholder="🔍 Search catchments...">
            <table id="allTable">
                <thead>
                    <tr><th>ID</th><th>Catchment</th><th>Pixels</th><th>Total (mm)</th><th>Max (mm/min)</th><th>Coverage</th></tr>
                </thead>
                <tbody>
"""
    
    for _, row in df.sort_values("total_rainfall", ascending=False).iterrows():
        rain_class = "has-rain" if row["total_rainfall"] > 0 else "no-rain"
        html += f"""                    <tr class="data-row">
                        <td>{row['catchment_id']}</td>
                        <td>{row['catchment_name']}</td>
                        <td>{row['pixel_count']}</td>
                        <td class="{rain_class}">{row['total_rainfall']:.2f}</td>
                        <td>{row['max_intensity']:.3f}</td>
                        <td>{row['rain_coverage_pct']:.1f}%</td>
                    </tr>
"""
    
    html += f"""                </tbody>
            </table>
        </div>
        
        <div class="footer">
            Rain Radar Dashboard | Auckland Council | {total_catchments} Catchments | {datetime.now().strftime('%Y-%m-%d')}
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
    
    out_dir.mkdir(parents=True, exist_ok=True)
    output_path = out_dir / "radar_dashboard.html"
    output_path.write_text(html, encoding="utf-8")
    logger.info("✓ Saved dashboard to %s", output_path)
    
    return output_path