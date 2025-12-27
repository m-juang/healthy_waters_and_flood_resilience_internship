"""
Analysis Package

Provides analysis functions for rain gauge and radar data, including filtering,
alarm analysis, ARI calculations, and reporting.

Modules:
    runner: High-level analysis entry points
    filtering: Rain gauge filtering logic
    alarm_analysis: Alarm configuration analysis
    ari_calculator: ARI (Annual Recurrence Interval) calculations
    reporting: Analysis report generation
    radar_analysis: Radar QPE data analysis

Functions:
    run_filter_active_gauges: Filter and analyze rain gauge data
    run_radar_analysis: Analyze radar data and calculate ARI

Classes:
    FilterConfig: Configuration for gauge filtering
    ARICalculator: TP108-based ARI calculator

Exceptions:
    AnalysisRunnerError: Base analysis runner exception
    InputDataError: Invalid input data
    OutputError: Output generation failure
    ARICalculationError: ARI calculation error
    CoefficientsNotFoundError: TP108 coefficients missing
    RadarAnalysisError: Radar analysis error
    NoRadarDataError: No radar data found

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2024-12-28
Version: 1.0.0
"""

from moata_pipeline.analyze.runner import (
    run_filter_active_gauges,
    AnalysisRunnerError,
    InputDataError,
    OutputError,
)

from moata_pipeline.analyze.filtering import (
    FilterConfig,
    filter_gauges,
    is_auckland_gauge,
    get_rainfall_trace,
    is_gauge_active,
)

from moata_pipeline.analyze.alarm_analysis import (
    analyze_alarms,
)

from moata_pipeline.analyze.ari_calculator import (
    ARICalculator,
    DURATION_CONFIG,
    process_all_catchments,
    ARICalculationError,
    CoefficientsNotFoundError,
    InvalidDataError,
)

from moata_pipeline.analyze.reporting import (
    create_summary_report,
)

from moata_pipeline.analyze.radar_analysis import (
    run_radar_analysis,
    RadarAnalysisError,
    NoRadarDataError,
)


# Version info
__version__ = "1.0.0"
__author__ = "Auckland Council Internship Team (COMPSCI 778)"


# Public API
__all__ = [
    # High-level runners
    "run_filter_active_gauges",
    "run_radar_analysis",
    
    # Filtering
    "FilterConfig",
    "filter_gauges",
    "is_auckland_gauge",
    "get_rainfall_trace",
    "is_gauge_active",
    
    # Alarm analysis
    "analyze_alarms",
    
    # ARI calculator
    "ARICalculator",
    "DURATION_CONFIG",
    "process_all_catchments",
    
    # Reporting
    "create_summary_report",
    
    # Exceptions - Runner
    "AnalysisRunnerError",
    "InputDataError",
    "OutputError",
    
    # Exceptions - ARI
    "ARICalculationError",
    "CoefficientsNotFoundError",
    "InvalidDataError",
    
    # Exceptions - Radar
    "RadarAnalysisError",
    "NoRadarDataError",
    
    # Metadata
    "__version__",
    "__author__",
]