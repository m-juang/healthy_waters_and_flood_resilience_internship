"""
Alarms module for real-time monitoring.

This module provides alarm checking functionality for radar data,
implementing the 25% area threshold requirement.
"""

from .radar_alarm_checker import RadarAlarmChecker

__all__ = ["RadarAlarmChecker"]