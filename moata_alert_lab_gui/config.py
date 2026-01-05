"""
Configuration Module

Contains color palettes, theme settings, and application constants.

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2025-01-02
Version: 1.0.0
"""

from dataclasses import dataclass
from typing import Dict


# =============================================================================
# Application Constants
# =============================================================================

APP_TITLE = "Auckland Council - Moata AlertLab"
APP_VERSION = "1.0.0"
APP_SUBTITLE = "Auckland Council  •  Healthy Waters & Flood Resilience"
APP_FOOTER = "COMPSCI 778 Internship"

# Window settings
WINDOW_WIDTH = 1000
WINDOW_HEIGHT = 700
MIN_WIDTH = 900
MIN_HEIGHT = 650

# Timeouts for script execution (seconds)
TIMEOUT_RETRIEVE = 10800  # 3 hours
TIMEOUT_RADAR = 1800     # 30 minutes
TIMEOUT_DEFAULT = 600    # 10 minutes


# =============================================================================
# Color Palettes
# =============================================================================

LIGHT_COLORS: Dict[str, str] = {
    "primary": "#1E3A5F",
    "primary_light": "#2D4A6F",
    "accent": "#0EA5E9",
    "success": "#10B981",
    "warning": "#F59E0B",
    "danger": "#EF4444",
    "surface": "#FFFFFF",
    "background": "#CDD7E2",
    "text": "#1E293B",
    "text_secondary": "#64748B",
    "border": "#E2E8F0",
    "header_subtitle": "#B8C5D6",
    # Pipeline colors
    "gauge": "#0EA5E9",
    "radar": "#8B5CF6",
    # Step colors - Professional muted palette
    "step1": "#475569",
    "step2": "#475569",
    "step3": "#475569",
    "step4": "#64748B",
    "step5": "#64748B",
    # Decorative colors
    "deco1": "#e8eef4",
    "deco2": "#dde5ed",
}

DARK_COLORS: Dict[str, str] = {
    "primary": "#0F172A",
    "primary_light": "#1E293B",
    "accent": "#38BDF8",
    "success": "#34D399",
    "warning": "#FBBF24",
    "danger": "#F87171",
    "surface": "#1E293B",
    "background": "#0F172A",
    "text": "#F1F5F9",
    "text_secondary": "#94A3B8",
    "border": "#334155",
    "header_subtitle": "#94A3B8",
    # Pipeline colors (brighter for dark mode)
    "gauge": "#38BDF8",
    "radar": "#A78BFA",
    # Step colors - Professional muted palette for dark mode
    "step1": "#64748B",
    "step2": "#64748B",
    "step3": "#64748B",
    "step4": "#475569",
    "step5": "#475569",
    # Decorative colors
    "deco1": "#1a2536",
    "deco2": "#151d2b",
}


# =============================================================================
# Helper Functions
# =============================================================================

def darken_color(hex_color: str, factor: float = 0.85) -> str:
    """
    Darken a hex color by a factor.
    
    Args:
        hex_color: Hex color string (e.g., "#1E3A5F")
        factor: Darkening factor (0.0-1.0, lower = darker)
        
    Returns:
        Darkened hex color string
    """
    hex_color = hex_color.lstrip('#')
    rgb = tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))
    rgb = tuple(max(0, int(c * factor)) for c in rgb)
    return f"#{rgb[0]:02x}{rgb[1]:02x}{rgb[2]:02x}"