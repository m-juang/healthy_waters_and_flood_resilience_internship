"""
Base Pipeline Module

Abstract base class for pipeline implementations.

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2025-01-02
Version: 1.0.0
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Callable, Dict, List, Tuple, TYPE_CHECKING
import customtkinter as ctk
from moata_pipeline.common.paths import PipelinePaths

if TYPE_CHECKING:
    from ..__main__ import ModernApp


class BasePipeline(ABC):
    """
    Abstract base class for pipeline implementations.
    
    Subclasses must implement:
    - name: Pipeline display name
    - icon: Pipeline emoji icon
    - color_key: Key in colors dict for accent color
    - features: List of feature descriptions
    - get_steps(): Return list of pipeline steps
    """
    
    def __init__(self, app: "ModernApp", initial_start_time=None, initial_end_time=None):
        """
        Initialize pipeline.
        
        Args:
            app: Parent application instance
            initial_start_time: Optional pre-filled start datetime
            initial_end_time: Optional pre-filled end datetime
        """
        self.app = app
        self.initial_start_time = initial_start_time
        self.initial_end_time = initial_end_time
        self.paths = PipelinePaths()
    
    def _get_date_from_user(self, title: str = "Select Date") -> str | None:
        """
        Simple date picker dialog.
        
        Returns:
            Date string in YYYY-MM-DD format or None if cancelled
        """
        date_str = ctk.CTkInputDialog(
            text="Enter date in YYYY-MM-DD format:\n\n(Leave blank for today)",
            title=title
        ).get_input()
        
        # Check if user cancelled (None return value)
        if date_str is None:
            return None
        
        # If empty string, use today's date
        if date_str.strip() == "":
            from datetime import datetime
            return datetime.now().strftime('%Y-%m-%d')
        
        return date_str
    
    @property
    @abstractmethod
    def name(self) -> str:
        """Pipeline display name."""
        pass
    
    @property
    @abstractmethod
    def icon(self) -> str:
        """Pipeline emoji icon."""
        pass
    
    @property
    @abstractmethod
    def color_key(self) -> str:
        """Key in colors dict for pipeline accent color."""
        pass
    
    @property
    @abstractmethod
    def features(self) -> List[str]:
        """List of feature descriptions for the card."""
        pass
    
    @property
    def subtitle(self) -> str:
        """Pipeline subtitle for the card."""
        return ""
    
    @property
    def color(self) -> str:
        """Get pipeline accent color."""
        return self.app.colors[self.color_key]
    
    @abstractmethod
    def get_steps(self) -> List[Tuple[str, str, str, Callable, str]]:
        """
        Get pipeline steps.
        
        Returns:
            List of tuples: (number, name, description, command, color_key)
        """
        pass
    
    # =========================================================================
    # Common Step Handlers (can be overridden)
    # =========================================================================
    
    @abstractmethod
    def run_retrieve(self) -> None:
        """Run retrieve step."""
        pass
    
    @abstractmethod
    def run_analyze(self) -> None:
        """Run analyze step."""
        pass
    
    @abstractmethod
    def run_visualize(self) -> None:
        """Run visualize step."""
        pass
    
    def run_validate(self) -> None:
        """
        Run validate step (optional - override if pipeline uses validation).
        
        Default implementation does nothing.
        Gauge pipeline overrides this for historical event validation.
        Radar pipeline doesn't use this (has real-time alarm checking instead).
        """
        pass
    
    def run_visualize_validation(self) -> None:
        """
        Run visualize validation step (optional - override if pipeline uses validation).
        
        Default implementation does nothing.
        Gauge pipeline overrides this to show validation results.
        Radar pipeline doesn't use this (has alarm visualization instead).
        """
        pass