"""
Visualization Package

Provides HTML visualization generation for rain gauge data.

Modules:
    runner: Main visualization entry point
    cleaning: Data cleaning and preparation
    pages: Per-gauge HTML page generation
    report: Main report HTML generation

Functions:
    run_visual_report: Generate complete visualization report

Exceptions:
    VisualizationError: Base visualization exception

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2024-12-28
Version: 1.0.0
"""

from moata_pipeline.viz.runner import (
    run_visual_report,
    VisualizationError,
)

# Version info
__version__ = "1.0.0"
__author__ = "Auckland Council Internship Team (COMPSCI 778)"

# Public API
__all__ = [
    "run_visual_report",
    "VisualizationError",
    "__version__",
    "__author__",
]