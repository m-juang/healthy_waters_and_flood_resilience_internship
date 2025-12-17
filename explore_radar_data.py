"""
Radar Data Explorer and Visualizer
Explores and visualizes downloaded radar data
"""
import json
import pickle
from pathlib import Path
from typing import Dict, List

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (15, 10)

class RadarDataExplorer:
    """Explore and visualize radar data"""
    
    def __init__(self, data_dir: str = "radar_data_output"):
        self.data_dir = Path(data_dir)
        self.catchments_dir = self.data_dir / "catchments"
        self.mappings_dir = self.data_dir / "pixel_mappings"
        self.radar_dir = self.data_dir / "radar_data"
        self.output_dir = self.data_dir / "visualizations"
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
                # Filter out None values
                valid_values = [v for v in values if v is not None]
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
    
    def create_overview_report(self):
        """Create overview statistics report"""
        print("="*80)
        print("RADAR DATA EXPLORER - OVERVIEW REPORT")
        print("="*80)
        
        # Load data
        catchments = self.load_catchments()
        pixel_mappings = self.load_pixel_mappings()
        
        # Get all radar data files
        radar_files = list(self.radar_dir.glob("catchment_*.csv"))
        
        print(f"\n📊 DATA SUMMARY")
        print(f"{'='*80}")
        print(f"Total catchments in database: {len(catchments)}")
        print(f"Catchments with radar data: {len(radar_files)}")
        print(f"Total pixels mapped: {sum(len(pixels) for pixels in pixel_mappings.values())}")
        
        # Analyze pixel distribution
        pixel_counts = [len(pixels) for pixels in pixel_mappings.values()]
        print(f"\n📍 PIXEL DISTRIBUTION")
        print(f"{'='*80}")
        print(f"Average pixels per catchment: {np.mean(pixel_counts):.1f}")
        print(f"Min pixels: {min(pixel_counts)}")
        print(f"Max pixels: {max(pixel_counts)}")
        print(f"Median pixels: {np.median(pixel_counts):.1f}")
        
        # Analyze rainfall data
        print(f"\n🌧️ RAINFALL ANALYSIS")
        print(f"{'='*80}")
        
        all_totals = []
        all_maxes = []
        catchments_with_rain = 0
        
        for catchment_id in list(pixel_mappings.keys())[:20]:  # Sample first 20
            df = self.load_radar_data(catchment_id)
            if df.empty:
                continue
            
            df_parsed = self.parse_rainfall_values(df)
            all_totals.extend(df_parsed['total_rainfall'].tolist())
            all_maxes.extend(df_parsed['max_rainfall'].tolist())
            
            if df_parsed['total_rainfall'].sum() > 0:
                catchments_with_rain += 1
        
        if all_totals:
            print(f"Catchments analyzed: 20")
            print(f"Catchments with rainfall: {catchments_with_rain}")
            print(f"Average total rainfall per pixel: {np.mean(all_totals):.2f} mm")
            print(f"Max rainfall intensity (1-min): {max(all_maxes):.2f} mm")
            print(f"Total rainfall across all pixels: {sum(all_totals):.2f} mm")
        
        return catchments, pixel_mappings
    
    def plot_pixel_distribution(self, pixel_mappings: Dict[int, List[int]]):
        """Plot pixel distribution across catchments"""
        print(f"\n📊 Creating pixel distribution plot...")
        
        pixel_counts = [len(pixels) for pixels in pixel_mappings.values()]
        
        fig, axes = plt.subplots(2, 2, figsize=(15, 10))
        fig.suptitle('Pixel Distribution Across Catchments', fontsize=16, fontweight='bold')
        
        # Histogram
        ax1 = axes[0, 0]
        ax1.hist(pixel_counts, bins=30, edgecolor='black', alpha=0.7, color='skyblue')
        ax1.set_xlabel('Number of Pixels')
        ax1.set_ylabel('Number of Catchments')
        ax1.set_title('Distribution of Pixels per Catchment')
        ax1.axvline(np.mean(pixel_counts), color='red', linestyle='--', 
                    label=f'Mean: {np.mean(pixel_counts):.1f}')
        ax1.legend()
        
        # Box plot
        ax2 = axes[0, 1]
        ax2.boxplot(pixel_counts, vert=True)
        ax2.set_ylabel('Number of Pixels')
        ax2.set_title('Pixel Count Distribution (Box Plot)')
        ax2.grid(True, alpha=0.3)
        
        # Top 20 catchments
        ax3 = axes[1, 0]
        top_20 = sorted(pixel_mappings.items(), key=lambda x: len(x[1]), reverse=True)[:20]
        catchment_ids = [str(c[0]) for c in top_20]
        counts = [len(c[1]) for c in top_20]
        ax3.barh(range(len(counts)), counts, color='coral')
        ax3.set_yticks(range(len(catchment_ids)))
        ax3.set_yticklabels(catchment_ids, fontsize=8)
        ax3.set_xlabel('Number of Pixels')
        ax3.set_title('Top 20 Catchments by Pixel Count')
        ax3.invert_yaxis()
        
        # Cumulative distribution
        ax4 = axes[1, 1]
        sorted_counts = sorted(pixel_counts)
        cumulative = np.arange(1, len(sorted_counts) + 1) / len(sorted_counts) * 100
        ax4.plot(sorted_counts, cumulative, linewidth=2, color='green')
        ax4.set_xlabel('Number of Pixels')
        ax4.set_ylabel('Cumulative Percentage (%)')
        ax4.set_title('Cumulative Distribution')
        ax4.grid(True, alpha=0.3)
        
        plt.tight_layout()
        output_file = self.output_dir / 'pixel_distribution.png'
        plt.savefig(output_file, dpi=150, bbox_inches='tight')
        print(f"✅ Saved: {output_file}")
        plt.close()
    
    def plot_rainfall_analysis(self, pixel_mappings: Dict[int, List[int]], 
                               catchments: pd.DataFrame, sample_size: int = 30):
        """Analyze and plot rainfall data"""
        print(f"\n🌧️ Analyzing rainfall data (sampling {sample_size} catchments)...")
        
        catchment_stats = []
        
        for idx, catchment_id in enumerate(list(pixel_mappings.keys())[:sample_size]):
            df = self.load_radar_data(catchment_id)
            if df.empty:
                continue
            
            df_parsed = self.parse_rainfall_values(df)
            
            catchment_name = catchments[catchments['id'] == catchment_id]['name'].values
            catchment_name = catchment_name[0] if len(catchment_name) > 0 else f"ID {catchment_id}"
            
            catchment_stats.append({
                'catchment_id': catchment_id,
                'catchment_name': catchment_name,
                'total_rainfall': df_parsed['total_rainfall'].sum(),
                'avg_rainfall': df_parsed['total_rainfall'].mean(),
                'max_intensity': df_parsed['max_rainfall'].max(),
                'pixels_with_rain': (df_parsed['total_rainfall'] > 0).sum(),
                'total_pixels': len(df_parsed)
            })
            
            if (idx + 1) % 10 == 0:
                print(f"  Processed {idx + 1}/{sample_size} catchments...")
        
        if not catchment_stats:
            print("❌ No rainfall data found!")
            return
        
        stats_df = pd.DataFrame(catchment_stats)
        
        # Create plots
        fig, axes = plt.subplots(2, 2, figsize=(15, 10))
        fig.suptitle('Rainfall Analysis Across Catchments', fontsize=16, fontweight='bold')
        
        # Total rainfall bar chart
        ax1 = axes[0, 0]
        top_10 = stats_df.nlargest(10, 'total_rainfall')
        ax1.barh(range(len(top_10)), top_10['total_rainfall'], color='steelblue')
        ax1.set_yticks(range(len(top_10)))
        ax1.set_yticklabels([name[:30] for name in top_10['catchment_name']], fontsize=8)
        ax1.set_xlabel('Total Rainfall (mm)')
        ax1.set_title('Top 10 Catchments by Total Rainfall')
        ax1.invert_yaxis()
        
        # Max intensity
        ax2 = axes[0, 1]
        top_10_intensity = stats_df.nlargest(10, 'max_intensity')
        ax2.barh(range(len(top_10_intensity)), top_10_intensity['max_intensity'], color='coral')
        ax2.set_yticks(range(len(top_10_intensity)))
        ax2.set_yticklabels([name[:30] for name in top_10_intensity['catchment_name']], fontsize=8)
        ax2.set_xlabel('Max 1-min Intensity (mm)')
        ax2.set_title('Top 10 Catchments by Peak Intensity')
        ax2.invert_yaxis()
        
        # Scatter: total vs coverage
        ax3 = axes[1, 0]
        stats_df['rain_coverage'] = stats_df['pixels_with_rain'] / stats_df['total_pixels'] * 100
        ax3.scatter(stats_df['rain_coverage'], stats_df['total_rainfall'], 
                   alpha=0.6, s=100, color='green')
        ax3.set_xlabel('Rain Coverage (%)')
        ax3.set_ylabel('Total Rainfall (mm)')
        ax3.set_title('Rainfall vs Coverage')
        ax3.grid(True, alpha=0.3)
        
        # Distribution
        ax4 = axes[1, 1]
        ax4.hist(stats_df['total_rainfall'], bins=20, edgecolor='black', 
                alpha=0.7, color='purple')
        ax4.set_xlabel('Total Rainfall (mm)')
        ax4.set_ylabel('Number of Catchments')
        ax4.set_title('Distribution of Total Rainfall')
        ax4.axvline(stats_df['total_rainfall'].mean(), color='red', 
                   linestyle='--', label=f'Mean: {stats_df["total_rainfall"].mean():.2f} mm')
        ax4.legend()
        
        plt.tight_layout()
        output_file = self.output_dir / 'rainfall_analysis.png'
        plt.savefig(output_file, dpi=150, bbox_inches='tight')
        print(f"✅ Saved: {output_file}")
        plt.close()
        
        # Save stats
        csv_file = self.output_dir / 'rainfall_statistics.csv'
        stats_df.to_csv(csv_file, index=False)
        print(f"✅ Saved statistics: {csv_file}")
    
    def plot_catchment_detail(self, catchment_id: int, catchments: pd.DataFrame):
        """Detailed analysis of a single catchment"""
        print(f"\n🔍 Analyzing catchment {catchment_id} in detail...")
        
        df = self.load_radar_data(catchment_id)
        if df.empty:
            print(f"❌ No data for catchment {catchment_id}")
            return
        
        df_parsed = self.parse_rainfall_values(df)
        
        catchment_name = catchments[catchments['id'] == catchment_id]['name'].values
        catchment_name = catchment_name[0] if len(catchment_name) > 0 else f"ID {catchment_id}"
        
        fig, axes = plt.subplots(2, 2, figsize=(15, 10))
        fig.suptitle(f'Detailed Analysis: {catchment_name}', fontsize=16, fontweight='bold')
        
        # Rainfall per pixel
        ax1 = axes[0, 0]
        ax1.bar(range(len(df_parsed)), df_parsed['total_rainfall'], 
               color='skyblue', edgecolor='black', alpha=0.7)
        ax1.set_xlabel('Pixel Index')
        ax1.set_ylabel('Total Rainfall (mm)')
        ax1.set_title(f'Rainfall by Pixel ({len(df_parsed)} pixels)')
        ax1.grid(True, alpha=0.3, axis='y')
        
        # Max intensity per pixel
        ax2 = axes[0, 1]
        ax2.bar(range(len(df_parsed)), df_parsed['max_rainfall'], 
               color='coral', edgecolor='black', alpha=0.7)
        ax2.set_xlabel('Pixel Index')
        ax2.set_ylabel('Max 1-min Intensity (mm)')
        ax2.set_title('Peak Intensity by Pixel')
        ax2.grid(True, alpha=0.3, axis='y')
        
        # Distribution of totals
        ax3 = axes[1, 0]
        ax3.hist(df_parsed['total_rainfall'], bins=20, edgecolor='black', 
                alpha=0.7, color='green')
        ax3.set_xlabel('Total Rainfall (mm)')
        ax3.set_ylabel('Number of Pixels')
        ax3.set_title('Distribution of Rainfall Totals')
        ax3.axvline(df_parsed['total_rainfall'].mean(), color='red', 
                   linestyle='--', label=f'Mean: {df_parsed["total_rainfall"].mean():.2f} mm')
        ax3.legend()
        
        # Stats table
        ax4 = axes[1, 1]
        ax4.axis('off')
        stats_text = f"""
        CATCHMENT STATISTICS
        {'='*40}
        
        Total Pixels: {len(df_parsed)}
        Pixels with Rain: {(df_parsed['total_rainfall'] > 0).sum()}
        Rain Coverage: {(df_parsed['total_rainfall'] > 0).sum() / len(df_parsed) * 100:.1f}%
        
        RAINFALL TOTALS:
        Total: {df_parsed['total_rainfall'].sum():.2f} mm
        Average: {df_parsed['total_rainfall'].mean():.2f} mm
        Max: {df_parsed['total_rainfall'].max():.2f} mm
        Std Dev: {df_parsed['total_rainfall'].std():.2f} mm
        
        PEAK INTENSITIES:
        Max: {df_parsed['max_rainfall'].max():.2f} mm/min
        Average: {df_parsed['max_rainfall'].mean():.2f} mm/min
        
        DATA QUALITY:
        Avg Data Points/Pixel: {df_parsed['data_points'].mean():.0f}
        Non-zero Minutes: {df_parsed['non_zero_count'].sum()}
        """
        ax4.text(0.1, 0.5, stats_text, fontsize=10, family='monospace',
                verticalalignment='center')
        
        plt.tight_layout()
        output_file = self.output_dir / f'catchment_{catchment_id}_detail.png'
        plt.savefig(output_file, dpi=150, bbox_inches='tight')
        print(f"✅ Saved: {output_file}")
        plt.close()
    
    def run_full_exploration(self):
        """Run complete exploration and visualization"""
        print("\n" + "="*80)
        print("STARTING RADAR DATA EXPLORATION")
        print("="*80)
        
        # Load data
        catchments, pixel_mappings = self.create_overview_report()
        
        # Create visualizations
        self.plot_pixel_distribution(pixel_mappings)
        self.plot_rainfall_analysis(pixel_mappings, catchments, sample_size=50)
        
        # Detailed analysis of top catchments by rainfall
        print(f"\n🔍 Creating detailed catchment analyses...")
        sample_catchments = list(pixel_mappings.keys())[:5]
        for catchment_id in sample_catchments:
            self.plot_catchment_detail(catchment_id, catchments)
        
        print("\n" + "="*80)
        print("EXPLORATION COMPLETE!")
        print("="*80)
        print(f"\n📁 All visualizations saved to: {self.output_dir}")
        print(f"\nGenerated files:")
        for file in sorted(self.output_dir.glob("*.png")):
            print(f"  - {file.name}")
        for file in sorted(self.output_dir.glob("*.csv")):
            print(f"  - {file.name}")


if __name__ == "__main__":
    explorer = RadarDataExplorer()
    explorer.run_full_exploration()