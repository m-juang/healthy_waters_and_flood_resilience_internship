"""
Pipeline Paths Module

Provides canonical directory structure and file paths for pipeline outputs.

Classes:
    PipelinePaths: Central path management for all pipeline outputs

Directory Structure (NEW - Date Range Format):
    outputs/
    ├── rain_gauges/
    │   └── 20250509-20250510/    # Date range (YYYYMMDD-YYYYMMDD)
    │       ├── raw/              # Raw collected data
    │       ├── analysis/         # Analysis outputs
    │       └── visualizations/   # HTML dashboards, charts
    └── rain_radar/
        └── 20250509-20250510/    # Date range (YYYYMMDD-YYYYMMDD)
            ├── raw/              # Raw radar data
            │   └── radar_data/   # Per-catchment CSVs
            ├── analysis/         # ARI analysis outputs
            ├── alarms/           # Alarm timeline outputs
            ├── validation/       # Validation outputs
            └── visualizations/   # Radar dashboards

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2026-01-23 (FIXED: Correct date range logic)
Version: 2.0.1
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Optional, Union


# Version info
__version__ = "2.0.1"


# =============================================================================
# Pipeline Paths Class
# =============================================================================

@dataclass(frozen=True)
class PipelinePaths:
    """
    Canonical directory structure for pipeline outputs.
    
    ALL data is organized by date range (YYYYMMDD-YYYYMMDD format).
    Use current date for real-time/current data.
    
    Attributes:
        outputs_root: Root directory for all outputs (default: "outputs")
        date: Date for data organization (defaults to today, used for single-day ranges)
        start_time: Start datetime for data range (overrides date if provided)
        end_time: End datetime for data range (overrides date if provided)
        
    Example:
        >>> from datetime import date, datetime
        >>> # Today's data (last 24 hours ending now)
        >>> paths = PipelinePaths()
        >>> print(paths.rain_gauges_raw_dir)
        outputs/rain_gauges/20260122-20260123/raw
        
        >>> # Historical data with specific date (full 24 hours)
        >>> paths = PipelinePaths(date="2025-05-09")
        >>> print(paths.rain_gauges_raw_dir)
        outputs/rain_gauges/20250509-20250510/raw
        
        >>> # Explicit date range
        >>> paths = PipelinePaths(
        ...     start_time=datetime(2025, 5, 9, 0, 0, 0),
        ...     end_time=datetime(2025, 5, 10, 0, 0, 0)
        ... )
        >>> print(paths.rain_radar_raw_dir)
        outputs/rain_radar/20250509-20250510/raw
    """
    
    # Root directory for all outputs
    outputs_root: Path = Path("outputs")
    
    # Date for data organization (defaults to today)
    date: Union[date, datetime, str, None] = None
    
    # Optional: explicit start/end times for data range
    start_time: Union[datetime, None] = None
    end_time: Union[datetime, None] = None
    
    def __post_init__(self) -> None:
        """Validate and convert attributes."""
        # Convert outputs_root to Path if string
        if not isinstance(self.outputs_root, Path):
            object.__setattr__(self, 'outputs_root', Path(self.outputs_root))
        
        # Handle date/time conversion
        if self.start_time is not None and self.end_time is not None:
            # Explicit date range provided - use it as is
            pass
            
        elif self.date is not None:
            # ✅ FIXED: Single date provided - convert to full 24-hour range for that date
            # Example: date="2026-01-22" → 2026-01-22 00:00:00 to 2026-01-23 00:00:00
            if isinstance(self.date, str):
                date_obj = datetime.strptime(self.date, '%Y-%m-%d').date()
            elif isinstance(self.date, datetime):
                date_obj = self.date.date()
            else:
                date_obj = self.date
            
            # Start: Beginning of the requested day (00:00:00)
            object.__setattr__(
                self, 
                'start_time', 
                datetime.combine(date_obj, datetime.min.time())
            )
            # End: Beginning of the next day (00:00:00) = End of requested day
            object.__setattr__(
                self, 
                'end_time', 
                self.start_time + timedelta(days=1)
            )
            
        else:
            # ✅ FIXED: No date provided - use ACTUAL last 24 hours ending NOW
            # Example: Run at 2026-01-23 15:30 → 2026-01-22 15:30 to 2026-01-23 15:30
            now = datetime.now()
            object.__setattr__(self, 'end_time', now)
            object.__setattr__(self, 'start_time', now - timedelta(days=1))
        
        # Keep date attribute for backward compatibility
        if self.date is None:
            # Use end_time's date as reference
            object.__setattr__(
                self, 
                'date', 
                self.end_time.date() if self.end_time else datetime.now().date()
            )
    
    def _get_date_path(self) -> Path:
        """Get date range path component in YYYYMMDD-YYYYMMDD format."""
        # Format: YYYYMMDD-YYYYMMDD
        start_str = self.start_time.strftime('%Y%m%d') if self.start_time else self.date.strftime('%Y%m%d')
        end_str = self.end_time.strftime('%Y%m%d') if self.end_time else self.date.strftime('%Y%m%d')
        return Path(f"{start_str}-{end_str}")
    
    # =========================================================================
    # Rain Gauges - Directories
    # =========================================================================
    
    @property
    def rain_gauges_dir(self) -> Path:
        """Root directory for rain gauge outputs (includes date path)."""
        return self.outputs_root / "rain_gauges" / self._get_date_path()
    
    @property
    def rain_gauges_raw_dir(self) -> Path:
        """Raw collected rain gauge data directory."""
        return self.rain_gauges_dir / "raw"
    
    @property
    def rain_gauges_analysis_dir(self) -> Path:
        """Analysis outputs directory for rain gauges."""
        return self.rain_gauges_dir / "analysis"
    
    @property
    def rain_gauges_analyze_dir(self) -> Path:
        """
        Deprecated alias for rain_gauges_analysis_dir (backward compatibility).
        
        Use rain_gauges_analysis_dir instead.
        """
        return self.rain_gauges_analysis_dir
    
    @property
    def rain_gauges_filtered_dir(self) -> Path:
        """
        Deprecated alias for rain_gauges_analysis_dir.
        
        Use rain_gauges_analysis_dir instead for consistency.
        """
        return self.rain_gauges_analysis_dir
    
    @property
    def rain_gauges_viz_dir(self) -> Path:
        """Visualization outputs directory for rain gauges."""
        return self.rain_gauges_dir / "visualizations"
    
    @property
    def rain_gauges_validation_dir(self) -> Path:
        """Validation outputs directory for rain gauges."""
        return self.rain_gauges_dir / "validation"
    
    # =========================================================================
    # Rain Radar - Directories
    # =========================================================================
    
    @property
    def rain_radar_dir(self) -> Path:
        """Root directory for rain radar outputs (includes date path)."""
        return self.outputs_root / "rain_radar" / self._get_date_path()
    
    @property
    def rain_radar_raw_dir(self) -> Path:
        """Raw collected rain radar data directory."""
        return self.rain_radar_dir / "raw"
    
    @property
    def rain_radar_data_dir(self) -> Path:
        """Per-catchment radar CSV files directory."""
        return self.rain_radar_raw_dir / "radar_data"
    
    @property
    def rain_radar_analysis_dir(self) -> Path:
        """Analysis outputs directory for rain radar."""
        return self.rain_radar_dir / "analysis"
    
    @property
    def rain_radar_analyze_dir(self) -> Path:
        """
        Deprecated alias for rain_radar_analysis_dir (backward compatibility).
        
        Use rain_radar_analysis_dir instead.
        """
        return self.rain_radar_analysis_dir
    
    @property
    def rain_radar_ari_dir(self) -> Path:
        """ARI calculation results directory (same as analysis for consistency)."""
        return self.rain_radar_analysis_dir
    
    @property
    def rain_radar_alarms_dir(self) -> Path:
        """Alarm timeline outputs directory."""
        return self.rain_radar_dir / "alarms"
    
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
            self.rain_gauges_analysis_dir,
            self.rain_gauges_viz_dir,
            
            # Rain Radar
            self.rain_radar_raw_dir,
            self.rain_radar_data_dir,
            self.rain_radar_analysis_dir,
            self.rain_radar_alarms_dir,
            self.rain_radar_viz_dir,
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
    
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
            outputs/rain_radar/20260122-20260123/raw/radar_data/123_Auckland_CBD.csv
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
            outputs/rain_radar/20260122-20260123/analyze/ari_123_Auckland_CBD.csv
        """
        filename = f"ari_{catchment_id}_{catchment_name}.csv"
        return self.rain_radar_ari_dir / filename
    
    def get_gauge_raw_path(self, date_str: Optional[str] = None) -> Path:
        """
        Get path to rain gauge raw data JSON file.
        
        Args:
            date_str: Optional date string (YYYY-MM-DD). If provided, returns path
                     for that date. If None, uses current instance's date.
                     
        Returns:
            Path to rain_gauges_traces_alarms.json file
            
        Example:
            >>> paths = PipelinePaths()
            >>> raw_path = paths.get_gauge_raw_path()
            >>> print(raw_path)
            outputs/rain_gauges/20260122-20260123/raw/rain_gauges_traces_alarms.json
            
            >>> raw_path = paths.get_gauge_raw_path("2025-05-09")
            >>> print(raw_path)
            outputs/rain_gauges/20250509-20250510/raw/rain_gauges_traces_alarms.json
        """
        if date_str:
            date_paths = PipelinePaths.for_date(date_str, outputs_root=self.outputs_root)
            return date_paths.rain_gauges_traces_alarms_json
        return self.rain_gauges_traces_alarms_json
    
    def get_gauge_analyze_dir(self, date_str: Optional[str] = None) -> Path:
        """
        Get path to rain gauge analysis directory.
        
        Args:
            date_str: Optional date string (YYYY-MM-DD). If provided, returns path
                     for that date. If None, uses current instance's date.
                     
        Returns:
            Path to analysis directory
            
        Example:
            >>> paths = PipelinePaths()
            >>> analyze_dir = paths.get_gauge_analyze_dir()
            >>> print(analyze_dir)
            outputs/rain_gauges/20260122-20260123/analysis
            
            >>> analyze_dir = paths.get_gauge_analyze_dir("2025-05-09")
            >>> print(analyze_dir)
            outputs/rain_gauges/20250509-20250510/analysis
        """
        if date_str:
            date_paths = PipelinePaths.for_date(date_str, outputs_root=self.outputs_root)
            return date_paths.rain_gauges_analysis_dir
        return self.rain_gauges_analysis_dir
    
    def get_gauge_viz_dir(self, date_str: Optional[str] = None) -> Path:
        """
        Get path to rain gauge visualization directory.
        
        Args:
            date_str: Optional date string (YYYY-MM-DD). If provided, returns path
                     for that date. If None, uses current instance's date.
                     
        Returns:
            Path to visualization directory
            
        Example:
            >>> paths = PipelinePaths()
            >>> viz_dir = paths.get_gauge_viz_dir()
            >>> print(viz_dir)
            outputs/rain_gauges/20260122-20260123/visualizations
            
            >>> viz_dir = paths.get_gauge_viz_dir("2025-05-09")
            >>> print(viz_dir)
            outputs/rain_gauges/20250509-20250510/visualizations
        """
        if date_str:
            date_paths = PipelinePaths.for_date(date_str, outputs_root=self.outputs_root)
            return date_paths.rain_gauges_viz_dir
        return self.rain_gauges_viz_dir
    
    @staticmethod
    def for_date(date_value: Union[str, date, datetime], outputs_root: Path = Path("outputs")) -> "PipelinePaths":
        """
        Create PipelinePaths instance for a specific date.
        
        Args:
            date_value: Date as string (YYYY-MM-DD), date, or datetime object
            outputs_root: Root directory for outputs
            
        Returns:
            PipelinePaths configured for the specified date
            
        Example:
            >>> # From string
            >>> paths = PipelinePaths.for_date("2025-05-09")
            >>> print(paths.rain_radar_raw_dir)
            outputs/rain_radar/20250509-20250510/raw
            
            >>> # From date object
            >>> from datetime import date
            >>> paths = PipelinePaths.for_date(date(2025, 5, 9))
            >>> print(paths.rain_gauges_raw_dir)
            outputs/rain_gauges/20250509-20250510/raw
        """
        return PipelinePaths(outputs_root=outputs_root, date=date_value)
    
    @staticmethod
    def for_date_range(
        start_time: datetime, 
        end_time: datetime, 
        outputs_root: Path = Path("outputs")
    ) -> "PipelinePaths":
        """
        Create PipelinePaths instance for a specific date range.
        
        Args:
            start_time: Start datetime for data range
            end_time: End datetime for data range
            outputs_root: Root directory for outputs
            
        Returns:
            PipelinePaths configured for the specified date range
            
        Example:
            >>> from datetime import datetime
            >>> start = datetime(2025, 5, 9, 0, 0, 0)
            >>> end = datetime(2025, 5, 10, 0, 0, 0)
            >>> paths = PipelinePaths.for_date_range(start, end)
            >>> print(paths.rain_radar_raw_dir)
            outputs/rain_radar/20250509-20250510/raw
        """
        return PipelinePaths(outputs_root=outputs_root, start_time=start_time, end_time=end_time)
    
    @staticmethod
    def for_today(outputs_root: Path = Path("outputs")) -> "PipelinePaths":
        """
        Create PipelinePaths instance for today's date.
        
        Args:
            outputs_root: Root directory for outputs
            
        Returns:
            PipelinePaths configured for today
            
        Example:
            >>> paths = PipelinePaths.for_today()
            >>> print(paths.rain_radar_raw_dir)
            outputs/rain_radar/20260122-20260123/raw
        """
        return PipelinePaths(outputs_root=outputs_root, date=datetime.now().date())
    
    def with_date(self, date_value: Union[str, date, datetime]) -> "PipelinePaths":
        """
        Create new PipelinePaths instance with different date.
        
        Args:
            date_value: New date as string (YYYY-MM-DD), date, or datetime
            
        Returns:
            New PipelinePaths instance with specified date
            
        Example:
            >>> paths = PipelinePaths()  # Today
            >>> historical = paths.with_date("2025-05-09")
            >>> print(historical.rain_radar_raw_dir)
            outputs/rain_radar/20250509-20250510/raw
        """
        return PipelinePaths(outputs_root=self.outputs_root, date=date_value)
    
    def get_date_str(self) -> str:
        """
        Get date as YYYY-MM-DD string.
        
        Returns:
            Date string in YYYY-MM-DD format
            
        Example:
            >>> paths = PipelinePaths.for_date("2025-05-09")
            >>> paths.get_date_str()
            '2025-05-09'
        """
        return self.date.strftime('%Y-%m-%d')
    
    def __repr__(self) -> str:
        """String representation showing root and date."""
        return f"PipelinePaths(outputs_root='{self.outputs_root}', date='{self.get_date_str()}')"


# =============================================================================
# Global Helper Functions
# =============================================================================

_global_paths_cache: dict[str, PipelinePaths] = {}


def get_paths(
    date_value: Union[str, date, datetime, None] = None,
    outputs_root: Optional[Path] = None,
    force_new: bool = False
) -> PipelinePaths:
    """
    Get PipelinePaths instance (cached by date for efficiency).
    
    Args:
        date_value: Date for paths (None = today)
        outputs_root: Optional custom root directory
        force_new: If True, create new instance instead of using cache
        
    Returns:
        PipelinePaths instance
        
    Example:
        >>> from moata_pipeline.common.paths import get_paths
        >>> # Today's paths
        >>> paths = get_paths()
        >>> print(paths.rain_gauges_raw_dir)
        outputs/rain_gauges/20260122-20260123/raw
        
        >>> # Historical paths
        >>> historical = get_paths(date_value="2025-05-09")
        >>> print(historical.rain_radar_raw_dir)
        outputs/rain_radar/20250509-20250510/raw
    """
    root = outputs_root or Path("outputs")
    
    # Create temporary instance to get date string for cache key
    temp_paths = PipelinePaths(outputs_root=root, date=date_value)
    cache_key = f"{root}:{temp_paths.get_date_str()}"
    
    if force_new:
        return temp_paths
    
    if cache_key not in _global_paths_cache:
        _global_paths_cache[cache_key] = temp_paths
    
    return _global_paths_cache[cache_key]


def clear_paths_cache() -> None:
    """
    Clear the global paths cache.
    
    Useful for testing or when switching between many dates.
    
    Example:
        >>> from moata_pipeline.common.paths import clear_paths_cache
        >>> clear_paths_cache()
    """
    global _global_paths_cache
    _global_paths_cache = {}