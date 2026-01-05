"""
Rain Monitoring GUI Package

Modern GUI for Auckland Council Rain Monitoring System.
Built with CustomTkinter for a professional, modern appearance.

Supports CLI arguments for automation and pre-filled dates.

Usage:
    # Interactive GUI
    python -m rain_monitoring_gui
    
    # With pre-filled date
    python rain_monitoring_gui.py --date 2024-12-01
    
    # Headless mode (no GUI)
    python rain_monitoring_gui.py --date 2024-12-01 --no-gui

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2025-01-03
Version: 2.1.0
"""

from .main import ModernApp, main

__version__ = "2.1.0"
__all__ = ["ModernApp", "main"]