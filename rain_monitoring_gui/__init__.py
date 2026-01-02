"""
Rain Monitoring GUI Package

Modern GUI for Auckland Council Rain Monitoring System.
Built with CustomTkinter for a professional, modern appearance.

Usage:
    python -m rain_monitoring_gui

Or:
    from rain_monitoring_gui import main
    main()

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2025-01-02
Version: 2.0.0
"""

from .main import ModernApp, main

__version__ = "2.0.0"
__all__ = ["ModernApp", "main"]