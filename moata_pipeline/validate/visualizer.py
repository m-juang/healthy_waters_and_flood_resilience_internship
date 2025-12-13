from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


class ValidationVisualizer:
    """Create visualizations for ARI alarm validation results."""
    
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def create_validation_report(self, results: List[Dict[str, Any]]) -> None:
        """
        Generate all validation visualizations.
        
        Args:
            results: List of validation results from runner
        """
        if not results:
            print("No results to visualize")
            return
        
        # Convert to DataFrame
        df = pd.DataFrame(results)
        
        # 1. Summary overview
        self._plot_validation_summary(df)
        
        # 2. ARI exceedances per gauge
        self._plot_exceedances_by_gauge(df)
        
        # 3. ARI values distribution (filter extremes)
        self._plot_ari_distribution(df)
        
        # 4. Duration-based ARI comparison
        self._plot_duration_analysis(df)
        
        # 5. Validation status pie chart
        self._plot_validation_status(df)
        
        print(f"✅ Visualizations saved to: {self.output_dir}")
    
    def _plot_validation_summary(self, df: pd.DataFrame) -> None:
        """Summary bar chart of validation results."""
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
        
        # Left: Exceedances vs Logged Alarms
        x = np.arange(len(df))
        width = 0.35
        
        ax1.bar(x - width/2, df['ari_exceedances_count'], width, 
                label='ARI Exceedances', color='#e74c3c', alpha=0.8)
        ax1.bar(x + width/2, df['logged_alarms'], width,
                label='Logged Alarms', color='#3498db', alpha=0.8)
        
        ax1.set_xlabel('Gauge')
        ax1.set_ylabel('Count')
        ax1.set_title('ARI Exceedances vs Logged Alarms')
        ax1.set_xticks(x)
        ax1.set_xticklabels([f"G{aid}" for aid in df['asset_id']], rotation=45)
        ax1.legend()
        ax1.grid(axis='y', alpha=0.3)
        
        # Right: Summary statistics
        total_gauges = len(df)
        confirmed = (df['validation_status'] == 'CONFIRMED').sum()
        total_exceedances = df['ari_exceedances_count'].sum()
        total_alarms = df['logged_alarms'].sum()
        
        stats_text = f"""
        Validation Summary
        ──────────────────
        Total Gauges: {total_gauges}
        Confirmed: {confirmed} ({confirmed/total_gauges*100:.0f}%)
        
        Total ARI Exceedances: {total_exceedances}
        Total Logged Alarms: {total_alarms}
        
        Match Rate: {total_exceedances/total_alarms*100:.0f}%
        (Note: ARI shows duration windows,
         not exact alarm timestamps)
        """
        
        ax2.text(0.1, 0.5, stats_text, fontsize=12, family='monospace',
                verticalalignment='center', bbox=dict(boxstyle='round', 
                facecolor='wheat', alpha=0.3))
        ax2.axis('off')
        
        plt.tight_layout()
        plt.savefig(self.output_dir / '01_validation_summary.png', dpi=150, bbox_inches='tight')
        plt.close()
    
    def _plot_exceedances_by_gauge(self, df: pd.DataFrame) -> None:
        """Bar chart showing exceedance counts per gauge."""
        fig, ax = plt.subplots(figsize=(12, 6))
        
        # Sort by exceedance count
        df_sorted = df.sort_values('ari_exceedances_count', ascending=False)
        
        colors = ['#27ae60' if status == 'CONFIRMED' else '#95a5a6' 
                  for status in df_sorted['validation_status']]
        
        bars = ax.bar(range(len(df_sorted)), df_sorted['ari_exceedances_count'], 
                      color=colors, alpha=0.7)
        
        # Add threshold line
        ax.axhline(y=df_sorted['logged_alarms'].iloc[0], color='red', 
                   linestyle='--', alpha=0.5, label='Logged Alarms (reference)')
        
        ax.set_xlabel('Gauge (sorted by exceedances)', fontsize=11)
        ax.set_ylabel('Number of ARI Exceedances', fontsize=11)
        ax.set_title('ARI Exceedances by Gauge (Duration Windows > 5 Year Threshold)', 
                     fontsize=12, fontweight='bold')
        ax.set_xticks(range(len(df_sorted)))
        ax.set_xticklabels([f"Asset\n{aid}" for aid in df_sorted['asset_id']], 
                           fontsize=9)
        
        # Legend
        from matplotlib.patches import Patch
        legend_elements = [
            Patch(facecolor='#27ae60', alpha=0.7, label='Confirmed (exceedances found)'),
            Patch(facecolor='#95a5a6', alpha=0.7, label='No exceedances')
        ]
        ax.legend(handles=legend_elements, loc='upper right')
        ax.grid(axis='y', alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(self.output_dir / '02_exceedances_by_gauge.png', dpi=150, bbox_inches='tight')
        plt.close()
    
    def _plot_ari_distribution(self, df: pd.DataFrame) -> None:
        """Distribution of ARI values (filtered for reasonable range)."""
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
        
        # Extract all ARI values
        all_aris = []
        for idx, row in df.iterrows():
            if row['ari_values']:
                for item in row['ari_values']:
                    all_aris.append(item['ari'])
        
        if not all_aris:
            print("No ARI values to plot")
            return
        
        # Left: Log scale (all values)
        ax1.hist(np.log10(all_aris), bins=30, color='#3498db', alpha=0.7, edgecolor='black')
        ax1.axvline(x=np.log10(5), color='red', linestyle='--', linewidth=2, label='5 Year Threshold')
        ax1.set_xlabel('log₁₀(ARI in years)', fontsize=11)
        ax1.set_ylabel('Frequency', fontsize=11)
        ax1.set_title('ARI Distribution (Log Scale)', fontsize=12, fontweight='bold')
        ax1.legend()
        ax1.grid(axis='y', alpha=0.3)
        
        # Right: Filtered reasonable range (1-10k years)
        reasonable = [ari for ari in all_aris if 1 <= ari <= 10000]
        
        if reasonable:
            ax2.hist(reasonable, bins=30, color='#e74c3c', alpha=0.7, edgecolor='black')
            ax2.axvline(x=5, color='green', linestyle='--', linewidth=2, label='5 Year Threshold')
            ax2.set_xlabel('ARI (years)', fontsize=11)
            ax2.set_ylabel('Frequency', fontsize=11)
            ax2.set_title('ARI Distribution (Filtered: 1-10k years)', fontsize=12, fontweight='bold')
            ax2.legend()
            ax2.grid(axis='y', alpha=0.3)
        else:
            ax2.text(0.5, 0.5, 'No values in\nreasonable range\n(1-10k years)', 
                    ha='center', va='center', fontsize=12, transform=ax2.transAxes)
            ax2.axis('off')
        
        plt.tight_layout()
        plt.savefig(self.output_dir / '03_ari_distribution.png', dpi=150, bbox_inches='tight')
        plt.close()
    
    def _plot_duration_analysis(self, df: pd.DataFrame) -> None:
        """ARI values by duration window (box plot)."""
        # Extract duration-based data
        duration_data = {
            '10min': [], '20min': [], '30min': [], '1hr': [], 
            '2hr': [], '6hr': [], '12hr': [], '24hr': []
        }
        
        duration_map = {
            600: '10min', 1200: '20min', 1800: '30min', 3600: '1hr',
            7200: '2hr', 21600: '6hr', 43200: '12hr', 86400: '24hr'
        }
        
        for idx, row in df.iterrows():
            if row['ari_values']:
                for item in row['ari_values']:
                    dur_label = duration_map.get(item['duration'])
                    if dur_label and item['ari'] <= 1e6:  # Filter extreme values
                        duration_data[dur_label].append(item['ari'])
        
        # Filter out empty durations
        duration_data = {k: v for k, v in duration_data.items() if v}
        
        if not duration_data:
            print("No duration data to plot")
            return
        
        fig, ax = plt.subplots(figsize=(12, 6))
        
        positions = range(1, len(duration_data) + 1)
        bp = ax.boxplot(duration_data.values(), positions=positions, 
                        patch_artist=True, widths=0.6)
        
        # Color boxes
        for patch in bp['boxes']:
            patch.set_facecolor('#3498db')
            patch.set_alpha(0.6)
        
        # Add threshold line
        ax.axhline(y=5, color='red', linestyle='--', linewidth=2, 
                   alpha=0.7, label='5 Year Threshold')
        
        ax.set_xlabel('Duration Window', fontsize=11)
        ax.set_ylabel('ARI (years, log scale)', fontsize=11)
        ax.set_title('ARI Values by Duration Window (Filtered: ARI < 1 million)', 
                     fontsize=12, fontweight='bold')
        ax.set_xticks(positions)
        ax.set_xticklabels(duration_data.keys(), rotation=45)
        ax.set_yscale('log')
        ax.legend()
        ax.grid(axis='y', alpha=0.3, which='both')
        
        plt.tight_layout()
        plt.savefig(self.output_dir / '04_duration_analysis.png', dpi=150, bbox_inches='tight')
        plt.close()
    
    def _plot_validation_status(self, df: pd.DataFrame) -> None:
        """Pie chart of validation status."""
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
        
        # Left: Validation status
        status_counts = df['validation_status'].value_counts()
        colors = ['#27ae60' if 'CONFIRMED' in idx else '#e74c3c' 
                  for idx in status_counts.index]
        
        ax1.pie(status_counts.values, labels=status_counts.index, autopct='%1.0f%%',
                startangle=90, colors=colors, textprops={'fontsize': 11})
        ax1.set_title('Validation Status', fontsize=12, fontweight='bold')
        
        # Right: Gauge-level details table
        summary = df[['asset_id', 'ari_exceedances_count', 'logged_alarms', 'validation_status']].copy()
        summary.columns = ['Asset ID', 'Exceedances', 'Alarms', 'Status']
        summary['Status'] = summary['Status'].apply(lambda x: '✅' if x == 'CONFIRMED' else '❌')
        
        # Create table
        table_data = summary.values.tolist()
        table = ax2.table(cellText=table_data, colLabels=summary.columns,
                         cellLoc='center', loc='center', 
                         colWidths=[0.3, 0.25, 0.2, 0.25])
        table.auto_set_font_size(False)
        table.set_fontsize(9)
        table.scale(1, 2)
        
        # Style header
        for i in range(len(summary.columns)):
            table[(0, i)].set_facecolor('#3498db')
            table[(0, i)].set_text_props(weight='bold', color='white')
        
        # Alternate row colors
        for i in range(1, len(summary) + 1):
            for j in range(len(summary.columns)):
                if i % 2 == 0:
                    table[(i, j)].set_facecolor('#ecf0f1')
        
        ax2.axis('off')
        ax2.set_title('Gauge Details', fontsize=12, fontweight='bold', pad=20)
        
        plt.tight_layout()
        plt.savefig(self.output_dir / '05_validation_status.png', dpi=150, bbox_inches='tight')
        plt.close()
    
    def create_html_report(self, results: List[Dict[str, Any]], csv_path: Path) -> Path:
        """Generate HTML report with all visualizations."""
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <title>ARI Alarm Validation Report</title>
            <style>
                body {{
                    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                    margin: 40px;
                    background: #f5f6fa;
                    color: #2c3e50;
                }}
                .container {{
                    max-width: 1200px;
                    margin: 0 auto;
                    background: white;
                    padding: 40px;
                    border-radius: 10px;
                    box-shadow: 0 2px 10px rgba(0,0,0,0.1);
                }}
                h1 {{
                    color: #2c3e50;
                    border-bottom: 3px solid #3498db;
                    padding-bottom: 10px;
                }}
                h2 {{
                    color: #34495e;
                    margin-top: 30px;
                    border-left: 4px solid #3498db;
                    padding-left: 15px;
                }}
                .summary {{
                    background: #ecf0f1;
                    padding: 20px;
                    border-radius: 8px;
                    margin: 20px 0;
                }}
                .summary-grid {{
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                    gap: 15px;
                    margin-top: 15px;
                }}
                .stat-card {{
                    background: white;
                    padding: 15px;
                    border-radius: 8px;
                    text-align: center;
                    box-shadow: 0 2px 5px rgba(0,0,0,0.1);
                }}
                .stat-value {{
                    font-size: 32px;
                    font-weight: bold;
                    color: #3498db;
                }}
                .stat-label {{
                    font-size: 14px;
                    color: #7f8c8d;
                    margin-top: 5px;
                }}
                .chart {{
                    margin: 30px 0;
                    text-align: center;
                }}
                .chart img {{
                    max-width: 100%;
                    border-radius: 8px;
                    box-shadow: 0 2px 8px rgba(0,0,0,0.1);
                }}
                .note {{
                    background: #fff3cd;
                    border-left: 4px solid #ffc107;
                    padding: 15px;
                    margin: 20px 0;
                    border-radius: 4px;
                }}
                .success {{
                    background: #d4edda;
                    border-left: 4px solid #28a745;
                }}
                table {{
                    width: 100%;
                    border-collapse: collapse;
                    margin: 20px 0;
                }}
                th, td {{
                    padding: 12px;
                    text-align: left;
                    border-bottom: 1px solid #ddd;
                }}
                th {{
                    background: #3498db;
                    color: white;
                    font-weight: bold;
                }}
                tr:hover {{
                    background: #f5f6fa;
                }}
                .footer {{
                    margin-top: 40px;
                    padding-top: 20px;
                    border-top: 1px solid #ddd;
                    text-align: center;
                    color: #7f8c8d;
                    font-size: 14px;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>🌧️ ARI Alarm Validation Report</h1>
                <p><strong>Generated:</strong> {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                
                <div class="summary success">
                    <h3>✅ Validation Complete</h3>
                    <p>Successfully validated ARI alarms using Moata API endpoint: 
                    <code>/v1/traces/{{traceId}}/ari?type=Tp108</code></p>
                    
                    <div class="summary-grid">
                        <div class="stat-card">
                            <div class="stat-value">{len(results)}</div>
                            <div class="stat-label">Gauges Validated</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-value">{sum(r['ari_exceedances_count'] for r in results)}</div>
                            <div class="stat-label">Total Exceedances</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-value">{sum(r['logged_alarms'] for r in results)}</div>
                            <div class="stat-label">Logged Alarms</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-value">{sum(1 for r in results if r.get('validation_status') == 'CONFIRMED')}</div>
                            <div class="stat-label">Confirmed</div>
                        </div>
                    </div>
                </div>
                
                <div class="note">
                    <strong>⚠️ Important Note:</strong> ARI endpoint returns duration-based aggregate values 
                    (10 min to 24 hour windows) rather than point-in-time measurements. This is expected behavior 
                    for virtual ARI traces. Validation confirms that rainfall events exceeded the 5-year ARI threshold 
                    during the alarm period.
                </div>
                
                <h2>📊 Visualizations</h2>
                
                <div class="chart">
                    <h3>Validation Summary</h3>
                    <img src="01_validation_summary.png" alt="Validation Summary">
                </div>
                
                <div class="chart">
                    <h3>Exceedances by Gauge</h3>
                    <img src="02_exceedances_by_gauge.png" alt="Exceedances by Gauge">
                </div>
                
                <div class="chart">
                    <h3>ARI Distribution</h3>
                    <img src="03_ari_distribution.png" alt="ARI Distribution">
                </div>
                
                <div class="chart">
                    <h3>Duration Analysis</h3>
                    <img src="04_duration_analysis.png" alt="Duration Analysis">
                </div>
                
                <div class="chart">
                    <h3>Validation Status</h3>
                    <img src="05_validation_status.png" alt="Validation Status">
                </div>
                
                <h2>📄 Detailed Results</h2>
                <p>Full results available in: <code>{csv_path.name}</code></p>
                
                <div class="footer">
                    <p>Auckland Council - Healthy Waters & Flood Resilience Internship</p>
                    <p>Moata Rain Gauge Data Pipeline - Stage 4: ARI Alarm Validation</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        report_path = self.output_dir / 'validation_report.html'
        report_path.write_text(html_content, encoding='utf-8')
        return report_path