"""
Validation Utilities Module

Provides reusable validation functions for inputs, configuration, and data.
Centralizes common validation logic to reduce code duplication.

Functions:
    validate_path_exists: Check if file/directory path exists
    validate_positive_number: Validate numeric value is positive
    validate_date_string: Validate and parse date string
    validate_log_level: Validate logging level string
    validate_dataframe_not_empty: Ensure DataFrame has data
    validate_required_env_vars: Check required environment variables
    validate_ari_threshold: Validate ARI threshold value
    validate_proportion: Validate proportion/percentage value (0-1)
    
Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2026-01-21
Version: 1.0.0
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import pandas as pd


# Version info
__version__ = "1.0.0"


# Valid log levels
VALID_LOG_LEVELS = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


# =============================================================================
# Path Validation
# =============================================================================

def validate_path_exists(
    path: Union[str, Path],
    must_be_file: bool = False,
    must_be_dir: bool = False,
    error_message: Optional[str] = None
) -> Path:
    """
    Validate that a path exists and optionally check if it's a file or directory.
    
    Args:
        path: Path to validate (string or Path object)
        must_be_file: If True, path must be a file
        must_be_dir: If True, path must be a directory
        error_message: Custom error message to use
        
    Returns:
        Path object of validated path
        
    Raises:
        FileNotFoundError: If path doesn't exist
        ValueError: If path type doesn't match requirements
        
    Example:
        >>> validate_path_exists("data/inputs/tp108_stats.csv", must_be_file=True)
        Path('data/inputs/tp108_stats.csv')
    """
    path = Path(path) if isinstance(path, str) else path
    
    if not path.exists():
        msg = error_message or f"Path does not exist: {path}"
        raise FileNotFoundError(msg)
    
    if must_be_file and not path.is_file():
        msg = error_message or f"Path must be a file: {path}"
        raise ValueError(msg)
    
    if must_be_dir and not path.is_dir():
        msg = error_message or f"Path must be a directory: {path}"
        raise ValueError(msg)
    
    return path


# =============================================================================
# Numeric Validation
# =============================================================================

def validate_positive_number(
    value: Union[int, float],
    name: str = "value",
    allow_zero: bool = False,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None
) -> Union[int, float]:
    """
    Validate that a number is positive and within optional bounds.
    
    Args:
        value: Number to validate
        name: Name of the value (for error messages)
        allow_zero: If True, zero is considered valid
        min_value: Minimum allowed value (inclusive)
        max_value: Maximum allowed value (inclusive)
        
    Returns:
        Validated number
        
    Raises:
        TypeError: If value is not a number
        ValueError: If value doesn't meet requirements
        
    Example:
        >>> validate_positive_number(5.0, "threshold")
        5.0
        >>> validate_positive_number(0, "count", allow_zero=True)
        0
    """
    if not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be a number, got {type(value).__name__}")
    
    if allow_zero:
        if value < 0:
            raise ValueError(f"{name} must be non-negative, got {value}")
    else:
        if value <= 0:
            raise ValueError(f"{name} must be positive, got {value}")
    
    if min_value is not None and value < min_value:
        raise ValueError(f"{name} must be >= {min_value}, got {value}")
    
    if max_value is not None and value > max_value:
        raise ValueError(f"{name} must be <= {max_value}, got {value}")
    
    return value


def validate_proportion(
    value: float,
    name: str = "proportion",
    allow_zero: bool = True,
    allow_one: bool = True
) -> float:
    """
    Validate that a value is a valid proportion (0.0 to 1.0).
    
    Args:
        value: Proportion to validate
        name: Name of the value (for error messages)
        allow_zero: If True, 0.0 is valid
        allow_one: If True, 1.0 is valid
        
    Returns:
        Validated proportion
        
    Raises:
        TypeError: If value is not a number
        ValueError: If value is not in valid range
        
    Example:
        >>> validate_proportion(0.25, "threshold")
        0.25
        >>> validate_proportion(0.0, "minimum")
        0.0
    """
    if not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be a number, got {type(value).__name__}")
    
    min_val = 0.0 if allow_zero else 0.0 + 1e-10
    max_val = 1.0 if allow_one else 1.0 - 1e-10
    
    if value < min_val or value > max_val:
        range_str = f"[{min_val}, {max_val}]" if allow_zero and allow_one else \
                    f"({min_val}, {max_val})" if not allow_zero and not allow_one else \
                    f"[{min_val}, {max_val})" if allow_zero else f"({min_val}, {max_val}]"
        raise ValueError(f"{name} must be in range {range_str}, got {value}")
    
    return value


def validate_ari_threshold(
    threshold: float,
    name: str = "ARI threshold"
) -> float:
    """
    Validate ARI threshold value.
    
    Common thresholds: 2.0, 5.0, 10.0, 20.0, 50.0, 100.0
    
    Args:
        threshold: ARI threshold in years
        name: Name of the value (for error messages)
        
    Returns:
        Validated threshold
        
    Raises:
        ValueError: If threshold is invalid
        
    Example:
        >>> validate_ari_threshold(5.0)
        5.0
    """
    return validate_positive_number(
        threshold,
        name=name,
        allow_zero=False,
        min_value=1.0,
        max_value=1000.0
    )


# =============================================================================
# Date/Time Validation
# =============================================================================

def validate_date_string(
    date_str: str,
    format: str = "%Y-%m-%d",
    name: str = "date"
) -> datetime:
    """
    Validate and parse a date string.
    
    Args:
        date_str: Date string to validate
        format: Expected date format (default: YYYY-MM-DD)
        name: Name of the value (for error messages)
        
    Returns:
        Parsed datetime object
        
    Raises:
        ValueError: If date string is invalid
        
    Example:
        >>> dt = validate_date_string("2025-01-15")
        >>> dt.year
        2025
    """
    try:
        return datetime.strptime(date_str, format)
    except ValueError as e:
        raise ValueError(
            f"{name} must be in format '{format}', got '{date_str}': {e}"
        )


# =============================================================================
# Configuration Validation
# =============================================================================

def validate_log_level(level: str) -> str:
    """
    Validate logging level string.
    
    Args:
        level: Log level string (case-insensitive)
        
    Returns:
        Validated log level (uppercase)
        
    Raises:
        ValueError: If log level is invalid
        
    Example:
        >>> validate_log_level("info")
        'INFO'
        >>> validate_log_level("DEBUG")
        'DEBUG'
    """
    level_upper = level.upper()
    
    if level_upper not in VALID_LOG_LEVELS:
        raise ValueError(
            f"Invalid log level: '{level}'. "
            f"Valid levels: {', '.join(VALID_LOG_LEVELS)}"
        )
    
    return level_upper


def validate_required_env_vars(
    var_names: List[str],
    error_on_missing: bool = True
) -> Dict[str, Optional[str]]:
    """
    Validate that required environment variables are set.
    
    Args:
        var_names: List of required environment variable names
        error_on_missing: If True, raise error on missing vars
        
    Returns:
        Dictionary mapping variable names to values
        
    Raises:
        EnvironmentError: If required variables are missing (when error_on_missing=True)
        
    Example:
        >>> env_vars = validate_required_env_vars(
        ...     ["MOATA_CLIENT_ID", "MOATA_CLIENT_SECRET"]
        ... )
        >>> print(env_vars["MOATA_CLIENT_ID"])
        'your-client-id'
    """
    result = {}
    missing = []
    
    for var_name in var_names:
        value = os.getenv(var_name)
        result[var_name] = value
        
        if value is None:
            missing.append(var_name)
    
    if missing and error_on_missing:
        raise EnvironmentError(
            f"Missing required environment variables: {', '.join(missing)}\\n"
            f"Please set these variables in your .env file or environment."
        )
    
    return result


# =============================================================================
# DataFrame Validation
# =============================================================================

def validate_dataframe_not_empty(
    df: pd.DataFrame,
    name: str = "DataFrame",
    min_rows: int = 1
) -> pd.DataFrame:
    """
    Validate that a DataFrame is not empty.
    
    Args:
        df: DataFrame to validate
        name: Name of the DataFrame (for error messages)
        min_rows: Minimum required number of rows
        
    Returns:
        Validated DataFrame
        
    Raises:
        TypeError: If df is not a DataFrame
        ValueError: If DataFrame is empty or has too few rows
        
    Example:
        >>> df = pd.DataFrame({"a": [1, 2, 3]})
        >>> validate_dataframe_not_empty(df, "results")
        <DataFrame>
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"{name} must be a DataFrame, got {type(df).__name__}")
    
    if len(df) == 0:
        raise ValueError(f"{name} is empty (0 rows)")
    
    if len(df) < min_rows:
        raise ValueError(
            f"{name} has only {len(df)} rows, requires at least {min_rows}"
        )
    
    return df


def validate_dataframe_columns(
    df: pd.DataFrame,
    required_columns: List[str],
    name: str = "DataFrame"
) -> pd.DataFrame:
    """
    Validate that a DataFrame has all required columns.
    
    Args:
        df: DataFrame to validate
        required_columns: List of required column names
        name: Name of the DataFrame (for error messages)
        
    Returns:
        Validated DataFrame
        
    Raises:
        TypeError: If df is not a DataFrame
        ValueError: If required columns are missing
        
    Example:
        >>> df = pd.DataFrame({"a": [1], "b": [2]})
        >>> validate_dataframe_columns(df, ["a", "b"], "results")
        <DataFrame>
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"{name} must be a DataFrame, got {type(df).__name__}")
    
    missing = [col for col in required_columns if col not in df.columns]
    
    if missing:
        raise ValueError(
            f"{name} missing required columns: {missing}\\n"
            f"Available columns: {df.columns.tolist()}"
        )
    
    return df


# =============================================================================
# Combined Validation
# =============================================================================

def validate_dataframe(
    df: pd.DataFrame,
    name: str = "DataFrame",
    required_columns: Optional[List[str]] = None,
    min_rows: int = 1,
    check_null_columns: Optional[List[str]] = None
) -> pd.DataFrame:
    """
    Comprehensive DataFrame validation combining multiple checks.
    
    Args:
        df: DataFrame to validate
        name: Name of the DataFrame (for error messages)
        required_columns: List of required column names
        min_rows: Minimum required number of rows
        check_null_columns: Columns to check for null values
        
    Returns:
        Validated DataFrame
        
    Raises:
        TypeError: If df is not a DataFrame
        ValueError: If validation fails
        
    Example:
        >>> df = pd.DataFrame({
        ...     "id": [1, 2, 3],
        ...     "value": [10.5, 20.3, 30.1]
        ... })
        >>> validate_dataframe(
        ...     df,
        ...     name="results",
        ...     required_columns=["id", "value"],
        ...     min_rows=1
        ... )
        <DataFrame>
    """
    # Type check
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"{name} must be a DataFrame, got {type(df).__name__}")
    
    # Empty check
    validate_dataframe_not_empty(df, name, min_rows)
    
    # Column check
    if required_columns:
        validate_dataframe_columns(df, required_columns, name)
    
    # Null check
    if check_null_columns:
        for col in check_null_columns:
            if col not in df.columns:
                raise ValueError(f"{name} missing column for null check: {col}")
            
            null_count = df[col].isna().sum()
            if null_count > 0:
                raise ValueError(
                    f"{name} has {null_count} null values in column '{col}'"
                )
    
    return df


# =============================================================================
# Validation Result Helpers
# =============================================================================

class ValidationResult:
    """
    Container for validation results with detailed error tracking.
    
    Attributes:
        valid: Whether validation passed
        errors: List of error messages
        warnings: List of warning messages
        
    Example:
        >>> result = ValidationResult()
        >>> result.add_error("Missing required field")
        >>> result.add_warning("Data may be incomplete")
        >>> if not result.valid:
        ...     print(result.get_summary())
    """
    
    def __init__(self) -> None:
        """Initialize validation result container."""
        self.valid: bool = True
        self.errors: List[str] = []
        self.warnings: List[str] = []
    
    def add_error(self, message: str) -> None:
        """
        Add an error message and mark validation as failed.
        
        Args:
            message: Error message
        """
        self.errors.append(message)
        self.valid = False
    
    def add_warning(self, message: str) -> None:
        """
        Add a warning message (doesn't affect validity).
        
        Args:
            message: Warning message
        """
        self.warnings.append(message)
    
    def get_summary(self) -> str:
        """
        Get formatted summary of validation results.
        
        Returns:
            Formatted string with errors and warnings
        """
        lines = []
        
        if self.errors:
            lines.append("ERRORS:")
            for i, err in enumerate(self.errors, 1):
                lines.append(f"  {i}. {err}")
        
        if self.warnings:
            if lines:
                lines.append("")
            lines.append("WARNINGS:")
            for i, warn in enumerate(self.warnings, 1):
                lines.append(f"  {i}. {warn}")
        
        return "\\n".join(lines) if lines else "No errors or warnings"
    
    def raise_if_invalid(self) -> None:
        """
        Raise ValueError if validation failed.
        
        Raises:
            ValueError: If validation failed (valid=False)
        """
        if not self.valid:
            raise ValueError(f"Validation failed:\\n{self.get_summary()}")
