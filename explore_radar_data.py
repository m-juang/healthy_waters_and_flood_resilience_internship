"""
Radar Data Explorer - Interactive HTML Dashboard
Creates comprehensive HTML report with all catchments
"""
import json
import pickle
from pathlib import Path
from typing import Dict, List
from datetime import datetime

import pandas as pd
import numpy as np

class RadarDashboardGenerator:
    """Generate interactive HTML dashboard for all radar data"""
    
    def __init__(self, data_dir: str = "radar_data_output"):
        self.data_dir = Path(data_dir)
        self.catchments_dir = self.data_dir / "catchments"
        self.mappings_dir = self.data_dir / "pixel_mappings"
        self.radar_dir = self.data_dir / "radar_data"
        self.output_dir = self.data_dir / "dashboard"
        self.output_dir.mkdir(exist_ok=True)
        
    def load_catchments(self) -> pd.DataFrame:
        """Load catchments data"""
        file_path = self.catchments_dir / "stormwater_catchments.csv"
        return pd.read_csv(file_path)
    
    def load_pixel_mappings(self) -> Dict[int, List[int]]:
        """Load pixel mappings"""
        file_path = self.mappings_dir / "catchment_pixel_mapping.pkl"
        with open(file_path, "rb") as f:
            return pickle.load(f)
    
    def load_radar_data(self, catchment_id: int) -> pd.DataFrame:
        """Load radar data for a catchment"""
        file_path = self.radar_dir / f"catchment_{catchment_id}_radar_data.csv"
        if not file_path.exists():
            return pd.DataFrame()
        return pd.read_csv(file_path)
    
    def parse_rainfall_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Parse the rainfall values array and extract metrics"""
        def safe_parse(values_str):
            try:
                values = json.loads(values_str)
                valid_values = [v for v in values if v is not None and isinstance(v, (int, float))]
                return {
                    'total_rainfall': sum(valid_values) if valid_values else 0,
                    'max_rainfall': max(valid_values) if valid_values else 0,
                    'avg_rainfall': np.mean(valid_values) if valid_values else 0,
                    'non_zero_count': sum(1 for v in valid_values if v > 0),
                    'data_points': len(valid_values)
                }
            except:
                return {
                    'total_rainfall': 0,
                    'max_rainfall': 0,
                    'avg_rainfall': 0,
                    'non_zero_count': 0,
                    'data_points': 0
                }
        
        parsed = df['values'].apply(safe_parse)
        return pd.concat([df, pd.DataFrame(list(parsed))], axis=1)
    
    def analyze_all_catchments(self, catchments: pd.DataFrame, 
                              pixel_mappings: Dict[int, List[int]]) -> pd.DataFrame:
        """Analyze all catchments and return comprehensive statistics"""
        print(f"\n🌧️ Analyzing ALL {len(pixel_mappings)} catchments...")
        
        all_stats = []
        total = len(pixel_mappings)
        
        for idx, catchment_id in enumerate(pixel_mappings.keys(), 1):
            # Get catchment info
            catchment_row = catchments[catchments['id'] == catchment_id]
            catchment_name = catchment_row['name'].values[0] if len(catchment_row) > 0 else f"ID {catchment_id}"
            
            # Get pixel count
            pixel_count = len(pixel_mappings[catchment_id])
            
            # Load and parse radar data
            df = self.load_radar_data(catchment_id)
            
            if df.empty:
                stats = {
                    'catchment_id': catchment_id,
                    'catchment_name': catchment_name,
                    'pixel_count': pixel_count,
                    'has_data': False,
                    'total_rainfall': 0,
                    'avg_rainfall_per_pixel': 0,
                    'max_intensity': 0,
                    'pixels_with_rain': 0,
                    'rain_coverage_pct': 0,
                    'data_quality_pct': 0
                }
            else:
                df_parsed = self.parse_rainfall_values(df)
                
                # Normalize data quality to max 100%
                # API returns 1441 points (inclusive boundaries) vs expected 1440
                avg_data_points = df_parsed['data_points'].mean()
                data_quality_pct = min(100.0, (avg_data_points / 1440) * 100)
                
                stats = {
                    'catchment_id': catchment_id,
                    'catchment_name': catchment_name,
                    'pixel_count': pixel_count,
                    'has_data': True,
                    'total_rainfall': df_parsed['total_rainfall'].sum(),
                    'avg_rainfall_per_pixel': df_parsed['total_rainfall'].mean(),
                    'max_intensity': df_parsed['max_rainfall'].max(),
                    'pixels_with_rain': (df_parsed['total_rainfall'] > 0).sum(),
                    'rain_coverage_pct': (df_parsed['total_rainfall'] > 0).sum() / len(df_parsed) * 100 if len(df_parsed) > 0 else 0,
                    'data_quality_pct': data_quality_pct
                }
            
            all_stats.append(stats)
            
            if idx % 20 == 0:
                print(f"  Progress: {idx}/{total} ({idx/total*100:.1f}%)")
        
        print(f"  ✅ Completed: {total} catchments analyzed")
        
        return pd.DataFrame(all_stats)
    
    def generate_html_dashboard(self, stats_df: pd.DataFrame, 
                                pixel_mappings: Dict[int, List[int]]):
        """Generate comprehensive HTML dashboard"""
        print(f"\n📊 Generating HTML dashboard...")
        
        # Calculate summary statistics
        total_catchments = len(stats_df)
        catchments_with_data = stats_df['has_data'].sum()
        catchments_with_rain = (stats_df['total_rainfall'] > 0).sum()
        total_pixels = sum(len(pixels) for pixels in pixel_mappings.values())
        total_rainfall = stats_df['total_rainfall'].sum()
        
        # Sort dataframes for rankings
        by_rainfall = stats_df.nlargest(20, 'total_rainfall')
        by_intensity = stats_df.nlargest(20, 'max_intensity')
        by_coverage = stats_df.nlargest(20, 'rain_coverage_pct')
        
        # Generate HTML
        html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Radar Data Dashboard - Auckland Council</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 20px;
            color: #333;
        }}
        
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            border-radius: 20px;
            box-shadow: 0 20px 60px rgba(0,0,0,0.3);
            overflow: hidden;
        }}
        
        .header {{
            background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%);
            color: white;
            padding: 40px;
            text-align: center;
        }}
        
        .header h1 {{
            font-size: 2.5em;
            margin-bottom: 10px;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.2);
        }}
        
        .header .subtitle {{
            font-size: 1.2em;
            opacity: 0.9;
        }}
        
        .header .timestamp {{
            margin-top: 15px;
            font-size: 0.9em;
            opacity: 0.8;
        }}
        
        .content {{
            padding: 40px;
        }}
        
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 40px;
        }}
        
        .stat-card {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 25px;
            border-radius: 15px;
            box-shadow: 0 5px 15px rgba(0,0,0,0.1);
            transition: transform 0.3s;
        }}
        
        .stat-card:hover {{
            transform: translateY(-5px);
            box-shadow: 0 8px 25px rgba(0,0,0,0.2);
        }}
        
        .stat-card .value {{
            font-size: 2.5em;
            font-weight: bold;
            margin: 10px 0;
        }}
        
        .stat-card .label {{
            font-size: 0.9em;
            opacity: 0.9;
            text-transform: uppercase;
            letter-spacing: 1px;
        }}
        
        .section {{
            margin-bottom: 40px;
            background: #f8f9fa;
            padding: 30px;
            border-radius: 15px;
        }}
        
        .section h2 {{
            color: #1e3c72;
            margin-bottom: 20px;
            font-size: 1.8em;
            border-bottom: 3px solid #667eea;
            padding-bottom: 10px;
        }}
        
        .table-container {{
            overflow-x: auto;
            margin-top: 20px;
        }}
        
        table {{
            width: 100%;
            border-collapse: collapse;
            background: white;
            border-radius: 10px;
            overflow: hidden;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        
        th {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 15px;
            text-align: left;
            font-weight: 600;
            text-transform: uppercase;
            font-size: 0.85em;
            letter-spacing: 1px;
        }}
        
        td {{
            padding: 12px 15px;
            border-bottom: 1px solid #e0e0e0;
        }}
        
        tr:hover {{
            background: #f5f5f5;
        }}
        
        .rank {{
            background: #667eea;
            color: white;
            padding: 5px 10px;
            border-radius: 50%;
            font-weight: bold;
            display: inline-block;
            min-width: 30px;
            text-align: center;
        }}
        
        .rank.gold {{ background: #ffd700; color: #333; }}
        .rank.silver {{ background: #c0c0c0; color: #333; }}
        .rank.bronze {{ background: #cd7f32; color: white; }}
        
        .progress-bar {{
            background: #e0e0e0;
            height: 20px;
            border-radius: 10px;
            overflow: hidden;
            position: relative;
        }}
        
        .progress-fill {{
            background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
            height: 100%;
            transition: width 0.3s;
        }}
        
        .search-box {{
            width: 100%;
            padding: 15px;
            font-size: 1em;
            border: 2px solid #667eea;
            border-radius: 10px;
            margin-bottom: 20px;
            transition: all 0.3s;
        }}
        
        .search-box:focus {{
            outline: none;
            box-shadow: 0 0 0 3px rgba(102, 126, 234, 0.2);
        }}
        
        .no-data {{
            color: #999;
            font-style: italic;
        }}
        
        .has-rain {{
            color: #28a745;
            font-weight: bold;
        }}
        
        .no-rain {{
            color: #dc3545;
        }}
        
        .footer {{
            background: #f8f9fa;
            padding: 20px;
            text-align: center;
            color: #666;
            font-size: 0.9em;
        }}
        
        @media (max-width: 768px) {{
            .stats-grid {{
                grid-template-columns: 1fr;
            }}
            
            .header h1 {{
                font-size: 1.8em;
            }}
            
            table {{
                font-size: 0.85em;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🌧️ Rain Radar Data Dashboard</h1>
            <div class="subtitle">Auckland Council - Stormwater Catchments QPE Analysis</div>
            <div class="timestamp">Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</div>
        </div>
        
        <div class="content">
            <!-- Summary Statistics -->
            <div class="stats-grid">
                <div class="stat-card">
                    <div class="label">Total Catchments</div>
                    <div class="value">{total_catchments}</div>
                </div>
                <div class="stat-card">
                    <div class="label">With Radar Data</div>
                    <div class="value">{catchments_with_data}</div>
                </div>
                <div class="stat-card">
                    <div class="label">With Rainfall</div>
                    <div class="value">{catchments_with_rain}</div>
                </div>
                <div class="stat-card">
                    <div class="label">Total Pixels</div>
                    <div class="value">{total_pixels:,}</div>
                </div>
                <div class="stat-card">
                    <div class="label">Total Rainfall</div>
                    <div class="value">{total_rainfall:.1f}</div>
                    <div class="label">mm</div>
                </div>
                <div class="stat-card">
                    <div class="label">Avg per Catchment</div>
                    <div class="value">{total_rainfall/catchments_with_rain:.2f}</div>
                    <div class="label">mm</div>
                </div>
            </div>
            
            <!-- Top 20 by Total Rainfall -->
            <div class="section">
                <h2>🏆 Top 20 Catchments by Total Rainfall</h2>
                <div class="table-container">
                    <table>
                        <thead>
                            <tr>
                                <th>Rank</th>
                                <th>Catchment Name</th>
                                <th>Total Rainfall (mm)</th>
                                <th>Pixels</th>
                                <th>Avg per Pixel (mm)</th>
                                <th>Coverage</th>
                            </tr>
                        </thead>
                        <tbody>
"""
        
        # Add top 20 by rainfall
        for idx, row in by_rainfall.iterrows():
            rank_class = 'gold' if idx == 0 else ('silver' if idx == 1 else ('bronze' if idx == 2 else ''))
            html += f"""
                            <tr>
                                <td><span class="rank {rank_class}">{by_rainfall.index.get_loc(idx) + 1}</span></td>
                                <td><strong>{row['catchment_name']}</strong></td>
                                <td>{row['total_rainfall']:.2f}</td>
                                <td>{row['pixel_count']}</td>
                                <td>{row['avg_rainfall_per_pixel']:.3f}</td>
                                <td>
                                    <div class="progress-bar">
                                        <div class="progress-fill" style="width: {row['rain_coverage_pct']:.1f}%"></div>
                                    </div>
                                    {row['rain_coverage_pct']:.1f}%
                                </td>
                            </tr>
"""
        
        html += """
                        </tbody>
                    </table>
                </div>
            </div>
            
            <!-- Top 20 by Peak Intensity -->
            <div class="section">
                <h2>⚡ Top 20 Catchments by Peak Intensity</h2>
                <div class="table-container">
                    <table>
                        <thead>
                            <tr>
                                <th>Rank</th>
                                <th>Catchment Name</th>
                                <th>Max Intensity (mm/min)</th>
                                <th>Total Rainfall (mm)</th>
                                <th>Pixels</th>
                            </tr>
                        </thead>
                        <tbody>
"""
        
        # Add top 20 by intensity
        for idx, row in by_intensity.iterrows():
            rank_class = 'gold' if idx == 0 else ('silver' if idx == 1 else ('bronze' if idx == 2 else ''))
            html += f"""
                            <tr>
                                <td><span class="rank {rank_class}">{by_intensity.index.get_loc(idx) + 1}</span></td>
                                <td><strong>{row['catchment_name']}</strong></td>
                                <td>{row['max_intensity']:.3f}</td>
                                <td>{row['total_rainfall']:.2f}</td>
                                <td>{row['pixel_count']}</td>
                            </tr>
"""
        
        html += """
                        </tbody>
                    </table>
                </div>
            </div>
            
            <!-- Top 20 by Coverage -->
            <div class="section">
                <h2>📊 Top 20 Catchments by Rain Coverage</h2>
                <div class="table-container">
                    <table>
                        <thead>
                            <tr>
                                <th>Rank</th>
                                <th>Catchment Name</th>
                                <th>Coverage</th>
                                <th>Pixels with Rain</th>
                                <th>Total Pixels</th>
                                <th>Total Rainfall (mm)</th>
                            </tr>
                        </thead>
                        <tbody>
"""
        
        # Add top 20 by coverage
        for idx, row in by_coverage[by_coverage['rain_coverage_pct'] > 0].head(20).iterrows():
            rank_class = 'gold' if idx == 0 else ('silver' if idx == 1 else ('bronze' if idx == 2 else ''))
            html += f"""
                            <tr>
                                <td><span class="rank {rank_class}">{by_coverage[by_coverage['rain_coverage_pct'] > 0].index.get_loc(idx) + 1}</span></td>
                                <td><strong>{row['catchment_name']}</strong></td>
                                <td>
                                    <div class="progress-bar">
                                        <div class="progress-fill" style="width: {row['rain_coverage_pct']:.1f}%"></div>
                                    </div>
                                    {row['rain_coverage_pct']:.1f}%
                                </td>
                                <td>{row['pixels_with_rain']:.0f}</td>
                                <td>{row['pixel_count']}</td>
                                <td>{row['total_rainfall']:.2f}</td>
                            </tr>
"""
        
        html += """
                        </tbody>
                    </table>
                </div>
            </div>
            
            <!-- All Catchments Table -->
            <div class="section">
                <h2>📋 All Catchments - Complete Data</h2>
                <input type="text" id="searchBox" class="search-box" placeholder="🔍 Search by catchment name or ID...">
                <div class="table-container">
                    <table id="allCatchmentsTable">
                        <thead>
                            <tr>
                                <th>ID</th>
                                <th>Catchment Name</th>
                                <th>Pixels</th>
                                <th>Total Rainfall (mm)</th>
                                <th>Avg/Pixel (mm)</th>
                                <th>Max Intensity (mm/min)</th>
                                <th>Coverage (%)</th>
                                <th>Data Quality (%)</th>
                            </tr>
                        </thead>
                        <tbody>
"""
        
        # Add all catchments
        for _, row in stats_df.sort_values('total_rainfall', ascending=False).iterrows():
            rain_class = 'has-rain' if row['total_rainfall'] > 0 else 'no-rain'
            data_status = '✅' if row['has_data'] else '❌'
            
            html += f"""
                            <tr class="catchment-row">
                                <td>{row['catchment_id']}</td>
                                <td><strong>{row['catchment_name']}</strong></td>
                                <td>{row['pixel_count']}</td>
                                <td class="{rain_class}">{row['total_rainfall']:.2f}</td>
                                <td>{row['avg_rainfall_per_pixel']:.3f}</td>
                                <td>{row['max_intensity']:.3f}</td>
                                <td>{row['rain_coverage_pct']:.1f}%</td>
                                <td>{data_status} {row['data_quality_pct']:.1f}%</td>
                            </tr>
"""
        
        html += f"""
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
        
        <div class="footer">
            <p>Rain Radar Data Dashboard | Auckland Council Stormwater Monitoring</p>
            <p>Data Period: Last 24 hours | QPE (Quantitative Precipitation Estimation)</p>
            <p>Total Catchments: {total_catchments} | Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </div>
    </div>
    
    <script>
        // Search functionality
        document.getElementById('searchBox').addEventListener('keyup', function() {{
            const searchValue = this.value.toLowerCase();
            const rows = document.querySelectorAll('.catchment-row');
            
            rows.forEach(row => {{
                const text = row.textContent.toLowerCase();
                row.style.display = text.includes(searchValue) ? '' : 'none';
            }});
        }});
        
        // Animate progress bars on load
        window.addEventListener('load', function() {{
            const fills = document.querySelectorAll('.progress-fill');
            fills.forEach(fill => {{
                const width = fill.style.width;
                fill.style.width = '0%';
                setTimeout(() => {{
                    fill.style.width = width;
                }}, 100);
            }});
        }});
    </script>
</body>
</html>
"""
        
        # Save HTML file
        output_file = self.output_dir / 'radar_dashboard.html'
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html)
        
        print(f"✅ Dashboard saved: {output_file}")
        
        # Also save CSV for Excel analysis
        csv_file = self.output_dir / 'all_catchments_data.csv'
        stats_df.to_csv(csv_file, index=False)
        print(f"✅ CSV saved: {csv_file}")
        
        return output_file
    
    def run(self):
        """Run full dashboard generation"""
        print("="*80)
        print("RADAR DATA DASHBOARD GENERATOR")
        print("="*80)
        
        # Load data
        print("\n📂 Loading data...")
        catchments = self.load_catchments()
        pixel_mappings = self.load_pixel_mappings()
        print(f"  ✅ Loaded {len(catchments)} catchments, {len(pixel_mappings)} with pixel mappings")
        
        # Analyze all catchments
        stats_df = self.analyze_all_catchments(catchments, pixel_mappings)
        
        # Generate HTML dashboard
        output_file = self.generate_html_dashboard(stats_df, pixel_mappings)
        
        print("\n" + "="*80)
        print("DASHBOARD GENERATION COMPLETE!")
        print("="*80)
        print(f"\n📊 Open in browser: {output_file.absolute()}")
        print(f"\n📁 Files created:")
        print(f"  - {output_file.name}")
        print(f"  - all_catchments_data.csv")
        print("\n💡 Tip: Use the search box in the dashboard to filter catchments!")


if __name__ == "__main__":
    generator = RadarDashboardGenerator()
    generator.run()