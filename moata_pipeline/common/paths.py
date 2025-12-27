"""
Pipeline Paths Module

Provides canonical directory structure and file paths for pipeline outputs.

Classes:
    PipelinePaths: Central path management for all pipeline outputs

Directory Structure:
    outputs/
    ├── rain_gauges/
    │   ├── raw/              # Raw collected data
    │   ├── analyze/          # Analysis outputs
    │   └── visualizations/   # HTML dashboards, charts
    └── rain_radar/
        ├── raw/              # Raw radar data
        │   └── radar_data/   # Per-catchment CSVs
        ├── analyze/          # ARI analysis outputs
        ├── ari/              # ARI calculation results
        ├── historical/       # Historical data by date
        └── visualizations/   # Radar dashboards

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2024-12-28
Version: 1.0.0
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional


# Version info
__version__ = "1.0.0"


# =============================================================================
# Pipeline Paths Class
# =============================================================================

@dataclass(frozen=True)
class PipelinePaths:
    """
    Canonical directory structure for pipeline outputs.
    
    This class provides a single source of truth for all output paths used
    throughout the rain monitoring system.
    
    Attributes:
        outputs_root: Root directory for all outputs (default: "outputs")
        
    Example:
        >>> paths = PipelinePaths()
        >>> print(paths.rain_gauges_analyze_dir)
        outputs/rain_gauges/analyze
        
        >>> # Custom root
        >>> paths = PipelinePaths(outputs_root=Path("/data/outputs"))
        >>> print(paths.rain_gauges_raw_dir)
        /data/outputs/rain_gauges/raw
    """
    
    # Root directory for all outputs
    outputs_root: Path = Path("outputs")
    
    def __post_init__(self) -> None:
        """Validate outputs_root is a Path object."""
        # Convert to Path if string was provided
        if not isinstance(self.outputs_root, Path):
            object.__setattr__(self, 'outputs_root', Path(self.outputs_root))
    
    # =========================================================================
    # Rain Gauges - Directories
    # =========================================================================
    
    @property
    def rain_gauges_dir(self) -> Path:
        """Root directory for rain gauge outputs."""
        return self.outputs_root / "rain_gauges"
    
    @property
    def rain_gauges_raw_dir(self) -> Path:
        """Raw collected rain gauge data directory."""
        return self.rain_gauges_dir / "raw"
    
    @property
    def rain_gauges_analyze_dir(self) -> Path:
        """Analysis outputs directory for rain gauges."""
        return self.rain_gauges_dir / "analyze"
    
    @property
    def rain_gauges_filtered_dir(self) -> Path:
        """
        Deprecated alias for rain_gauges_analyze_dir.
        
        Use rain_gauges_analyze_dir instead for consistency.
        """
        return self.rain_gauges_analyze_dir
    
    @property
    def rain_gauges_viz_dir(self) -> Path:
        """Visualization outputs directory for rain gauges."""
        return self.rain_gauges_dir / "visualizations"
    
    # =========================================================================
    # Rain Radar - Directories
    # =========================================================================
    
    @property
    def rain_radar_dir(self) -> Path:
        """Root directory for rain radar outputs."""
        return self.outputs_root / "rain_radar"
    
    @property
    def rain_radar_raw_dir(self) -> Path:
        """Raw collected rain radar data directory."""
        return self.rain_radar_dir / "raw"
    
    @property
    def rain_radar_data_dir(self) -> Path:
        """Per-catchment radar CSV files directory."""
        return self.rain_radar_raw_dir / "radar_data"
    
    @property
    def rain_radar_analyze_dir(self) -> Path:
        """Analysis outputs directory for rain radar."""
        return self.rain_radar_dir / "analyze"
    
    @property
    def rain_radar_ari_dir(self) -> Path:
        """ARI calculation results directory."""
        return self.rain_radar_dir / "ari"
    
    @property
    def rain_radar_historical_dir(self) -> Path:
        """Historical radar data directory (organized by date)."""
        return self.rain_radar_dir / "historical"
    
    @property
    def rain_radar_viz_dir(self) -> Path:
        """Visualization outputs directory for rain radar."""
        return self.rain_radar_dir / "visualizations"
    
    # =========================================================================
    # General Aliases
    # =========================================================================
    
    @property
    def viz_dir(self) -> Path:
        """
        Alias for rain_gauges_viz_dir (default visualization output).
        
        For backward compatibility with scripts that use viz_dir.
        """
        return self.rain_gauges_viz_dir
    
    # =========================================================================
    # Rain Gauges - File Paths
    # =========================================================================
    
    @property
    def rain_gauges_traces_alarms_json(self) -> Path:
        """Complete rain gauge data with traces and alarms (collection output)."""
        return self.rain_gauges_raw_dir / "rain_gauges_traces_alarms.json"
    
    @property
    def rain_gauges_json(self) -> Path:
        """Simple rain gauge list (basic collection output)."""
        return self.rain_gauges_raw_dir / "rain_gauges.json"
    
    @property
    def active_auckland_gauges_json(self) -> Path:
        """Filtered active Auckland gauges (analysis output)."""
        return self.rain_gauges_analyze_dir / "active_auckland_gauges.json"
    
    @property
    def alarm_summary_csv(self) -> Path:
        """Alarm summary CSV (simplified, essential columns only)."""
        return self.rain_gauges_analyze_dir / "alarm_summary.csv"
    
    @property
    def alarm_summary_full_csv(self) -> Path:
        """Alarm summary CSV (full details, all columns)."""
        return self.rain_gauges_analyze_dir / "alarm_summary_full.csv"
    
    @property
    def all_traces_csv(self) -> Path:
        """All traces CSV (complete trace inventory)."""
        return self.rain_gauges_analyze_dir / "all_traces.csv"
    
    @property
    def analysis_report_txt(self) -> Path:
        """Rain gauge analysis report (text summary)."""
        return self.rain_gauges_analyze_dir / "analysis_report.txt"
    
    # =========================================================================
    # Rain Radar - File Paths
    # =========================================================================
    
    @property
    def rain_radar_catchments_csv(self) -> Path:
        """Stormwater catchments list CSV."""
        return self.rain_radar_raw_dir / "catchments.csv"
    
    @property
    def rain_radar_catchments_json(self) -> Path:
        """Stormwater catchments list JSON."""
        return self.rain_radar_raw_dir / "catchments.json"
    
    @property
    def rain_radar_pixels_json(self) -> Path:
        """Pixel mappings JSON (catchment to pixels)."""
        return self.rain_radar_raw_dir / "pixels.json"
    
    @property
    def rain_radar_pixels_pkl(self) -> Path:
        """Pixel mappings pickle (fast loading)."""
        return self.rain_radar_raw_dir / "pixels.pkl"
    
    @property
    def rain_radar_qpe_sample_json(self) -> Path:
        """Sample QPE data JSON (for testing)."""
        return self.rain_radar_raw_dir / "qpe_sample.json"
    
    @property
    def rain_radar_collection_summary_json(self) -> Path:
        """Radar collection summary JSON."""
        return self.rain_radar_raw_dir / "collection_summary.json"
    
    @property
    def rain_radar_ari_summary_csv(self) -> Path:
        """ARI analysis summary CSV (per-catchment peaks)."""
        return self.rain_radar_ari_dir / "ari_summary.csv"
    
    @property
    def rain_radar_ari_exceedances_csv(self) -> Path:
        """ARI exceedances CSV (all exceedance records)."""
        return self.rain_radar_ari_dir / "ari_exceedances.csv"
    
    @property
    def rain_radar_ari_analysis_summary_csv(self) -> Path:
        """ARI analysis summary CSV (in analyze directory)."""
        return self.rain_radar_analyze_dir / "ari_analysis_summary.csv"
    
    @property
    def rain_radar_ari_analysis_exceedances_csv(self) -> Path:
        """ARI exceedances CSV (in analyze directory)."""
        return self.rain_radar_analyze_dir / "ari_exceedances.csv"
    
    @property
    def rain_radar_analysis_report_txt(self) -> Path:
        """Rain radar analysis report (text summary)."""
        return self.rain_radar_analyze_dir / "analysis_report.txt"
    
    # =========================================================================
    # Helper Methods
    # =========================================================================
    
    def create_all_directories(self) -> None:
        """
        Create all output directories if they don't exist.
        
        Useful for initialization or setup scripts.
        
        Example:
            >>> paths = PipelinePaths()
            >>> paths.create_all_directories()
            >>> assert paths.rain_gauges_raw_dir.exists()
        """
        directories = [
            # Rain Gauges
            self.rain_gauges_raw_dir,
            self.rain_gauges_analyze_dir,
            self.rain_gauges_viz_dir,
            
            # Rain Radar
            self.rain_radar_raw_dir,
            self.rain_radar_data_dir,
            self.rain_radar_analyze_dir,
            self.rain_radar_ari_dir,
            self.rain_radar_historical_dir,
            self.rain_radar_viz_dir,
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
    
    def get_historical_radar_dir(self, date_str: str) -> Path:
        """
        Get historical radar data directory for a specific date.
        
        Args:
            date_str: Date string in YYYY-MM-DD format
            
        Returns:
            Path to historical data directory for the date
            
        Example:
            >>> paths = PipelinePaths()
            >>> historical_dir = paths.get_historical_radar_dir("2024-05-09")
            >>> print(historical_dir)
            outputs/rain_radar/historical/2024-05-09/raw
        """
        return self.rain_radar_historical_dir / date_str / "raw"
    
    def get_catchment_radar_file(self, catchment_id: int, catchment_name: str) -> Path:
        """
        Get radar data file path for a specific catchment.
        
        Args:
            catchment_id: Catchment ID number
            catchment_name: Catchment name
            
        Returns:
            Path to catchment radar CSV file
            
        Example:
            >>> paths = PipelinePaths()
            >>> file_path = paths.get_catchment_radar_file(123, "Auckland_CBD")
            >>> print(file_path)
            outputs/rain_radar/raw/radar_data/123_Auckland_CBD.csv
        """
        filename = f"{catchment_id}_{catchment_name}.csv"
        return self.rain_radar_data_dir / filename
    
    def get_ari_file(self, catchment_id: int, catchment_name: str) -> Path:
        """
        Get ARI results file path for a specific catchment.
        
        Args:
            catchment_id: Catchment ID number
            catchment_name: Catchment name
            
        Returns:
            Path to catchment ARI CSV file
            
        Example:
            >>> paths = PipelinePaths()
            >>> ari_file = paths.get_ari_file(123, "Auckland_CBD")
            >>> print(ari_file)
            outputs/rain_radar/ari/ari_123_Auckland_CBD.csv
        """
        filename = f"ari_{catchment_id}_{catchment_name}.csv"
        return self.rain_radar_ari_dir / filename
    
    def __repr__(self) -> str:
        """String representation showing root directory."""
        return f"PipelinePaths(outputs_root='{self.outputs_root}')"