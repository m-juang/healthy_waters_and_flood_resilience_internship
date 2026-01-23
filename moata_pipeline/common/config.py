"""
Centralized configuration management for Moata AlertLab pipeline.

This module provides configuration constants and environment variable support
for deployment flexibility. All magic numbers and hardcoded values should be
defined here.

Version: 1.0.0
Author: Auckland Council Moata AlertLab Team
Date: January 2026

SOLID Principles:
- Single Responsibility: Configuration management only
- Open/Closed: Extendable via environment variables
- Dependency Inversion: Other modules depend on this abstraction

Example:
    >>> from moata_pipeline.common.config import Config
    >>> print(Config.AUCKLAND_PROJECT_ID)
    594
    >>> print(Config.RAIN_GAUGE_ASSET_TYPE)
    25
"""

import os
from typing import Optional


class Config:
    """
    Centralized configuration for Moata AlertLab pipeline.
    
    All magic numbers and project-specific constants are defined here.
    Values can be overridden via environment variables for different deployments.
    
    Attributes:
        AUCKLAND_PROJECT_ID: Auckland Council project ID in Moata
        RAIN_GAUGE_ASSET_TYPE: Asset type ID for rain gauges
        RADAR_CATCHMENT_ASSET_TYPE: Asset type ID for radar catchments
        QPE_TRACESET_ID: Traceset ID for QPE (Quantitative Precipitation Estimation)
        DEFAULT_TIMEOUT: Default HTTP request timeout in seconds
        MAX_RETRIES: Maximum number of API retry attempts
        BATCH_SIZE: Default batch size for API requests
    
    Environment Variables:
        MOATA_PROJECT_ID: Override AUCKLAND_PROJECT_ID
        MOATA_TIMEOUT: Override DEFAULT_TIMEOUT
        MOATA_MAX_RETRIES: Override MAX_RETRIES
        MOATA_BATCH_SIZE: Override BATCH_SIZE
    
    Example:
        >>> # Use default values
        >>> config = Config()
        >>> project_id = Config.AUCKLAND_PROJECT_ID
        
        >>> # Override via environment variable
        >>> os.environ['MOATA_PROJECT_ID'] = '123'
        >>> project_id = Config.get_project_id()  # Returns 123
    """
    
    # =========================================================================
    # PROJECT & ASSET TYPE CONSTANTS
    # =========================================================================
    
    # Auckland Council project ID in Moata system
    AUCKLAND_PROJECT_ID: int = 594
    
    # Asset type IDs (from Moata asset type configuration)
    RAIN_GAUGE_ASSET_TYPE: int = 100
    RADAR_CATCHMENT_ASSET_TYPE: int = 3541
    
    # Traceset IDs
    QPE_TRACESET_ID: int = 3
    
    # =========================================================================
    # API & HTTP CONFIGURATION
    # =========================================================================
    
    # Default timeout for HTTP requests (seconds)
    DEFAULT_TIMEOUT: int = 30
    
    # Maximum retry attempts for failed API calls
    MAX_RETRIES: int = 3
    
    # Default batch size for batched API requests
    BATCH_SIZE: int = 10
    
    # Rate limiting configuration
    RATE_LIMIT_CALLS: int = 100
    RATE_LIMIT_PERIOD: int = 60  # seconds
    
    # =========================================================================
    # FILE & OUTPUT CONFIGURATION
    # =========================================================================
    
    # Default output directory name
    DEFAULT_OUTPUT_DIR: str = "outputs"
    
    # Subdirectory names
    GAUGE_OUTPUT_SUBDIR: str = "rain_gauges"
    RADAR_OUTPUT_SUBDIR: str = "rain_radar"
    VISUALIZATION_SUBDIR: str = "visualizations"
    
    # File naming patterns
    RAW_DATA_SUBDIR: str = "raw"
    ANALYZED_SUBDIR: str = "analyze"
    HISTORICAL_SUBDIR: str = "historical"
    
    # =========================================================================
    # ENVIRONMENT VARIABLE OVERRIDES
    # =========================================================================
    
    @classmethod
    def get_project_id(cls) -> int:
        """
        Get project ID with environment variable override support.
        
        Returns:
            int: Project ID (from env var or default)
        
        Example:
            >>> os.environ['MOATA_PROJECT_ID'] = '999'
            >>> Config.get_project_id()
            999
        """
        return int(os.getenv('MOATA_PROJECT_ID', cls.AUCKLAND_PROJECT_ID))
    
    @classmethod
    def get_timeout(cls) -> int:
        """
        Get HTTP timeout with environment variable override support.
        
        Returns:
            int: Timeout in seconds
        """
        return int(os.getenv('MOATA_TIMEOUT', cls.DEFAULT_TIMEOUT))
    
    @classmethod
    def get_max_retries(cls) -> int:
        """
        Get max retries with environment variable override support.
        
        Returns:
            int: Maximum retry attempts
        """
        return int(os.getenv('MOATA_MAX_RETRIES', cls.MAX_RETRIES))
    
    @classmethod
    def get_batch_size(cls) -> int:
        """
        Get batch size with environment variable override support.
        
        Returns:
            int: Batch size for API requests
        """
        return int(os.getenv('MOATA_BATCH_SIZE', cls.BATCH_SIZE))
    
    @classmethod
    def get_output_dir(cls, base_dir: Optional[str] = None) -> str:
        """
        Get output directory path.
        
        Args:
            base_dir: Optional base directory (defaults to DEFAULT_OUTPUT_DIR)
        
        Returns:
            str: Output directory path
        """
        return base_dir or cls.DEFAULT_OUTPUT_DIR
    
    @classmethod
    def is_production(cls) -> bool:
        """
        Check if running in production environment.
        
        Returns:
            bool: True if MOATA_ENV=production
        
        Example:
            >>> os.environ['MOATA_ENV'] = 'production'
            >>> Config.is_production()
            True
        """
        return os.getenv('MOATA_ENV', '').lower() == 'production'
    
    @classmethod
    def get_all_settings(cls) -> dict:
        """
        Get all configuration settings as a dictionary.
        
        Returns:
            dict: All configuration values
        
        Example:
            >>> settings = Config.get_all_settings()
            >>> print(settings['project_id'])
            594
        """
        return {
            'project_id': cls.get_project_id(),
            'rain_gauge_asset_type': cls.RAIN_GAUGE_ASSET_TYPE,
            'radar_catchment_asset_type': cls.RADAR_CATCHMENT_ASSET_TYPE,
            'qpe_traceset_id': cls.QPE_TRACESET_ID,
            'timeout': cls.get_timeout(),
            'max_retries': cls.get_max_retries(),
            'batch_size': cls.get_batch_size(),
            'output_dir': cls.DEFAULT_OUTPUT_DIR,
            'is_production': cls.is_production(),
        }


# Convenience constants for backward compatibility
AUCKLAND_PROJECT_ID = Config.AUCKLAND_PROJECT_ID
RAIN_GAUGE_ASSET_TYPE = Config.RAIN_GAUGE_ASSET_TYPE
RADAR_CATCHMENT_ASSET_TYPE = Config.RADAR_CATCHMENT_ASSET_TYPE
QPE_TRACESET_ID = Config.QPE_TRACESET_ID


__all__ = [
    'Config',
    'AUCKLAND_PROJECT_ID',
    'RAIN_GAUGE_ASSET_TYPE',
    'RADAR_CATCHMENT_ASSET_TYPE',
    'QPE_TRACESET_ID',
]
