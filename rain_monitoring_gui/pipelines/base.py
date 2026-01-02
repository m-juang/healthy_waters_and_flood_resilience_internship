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
    
    def __init__(self, app: "ModernApp"):
        """
        Initialize pipeline.
        
        Args:
            app: Parent application instance
        """
        self.app = app
    
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
    
    @abstractmethod
    def run_validate(self) -> None:
        """Run validate step."""
        pass
    
    @abstractmethod
    def run_visualize_validation(self) -> None:
        """Run visualize validation step."""
        pass