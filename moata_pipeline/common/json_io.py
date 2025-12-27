"""
JSON Input/Output Utilities

Provides utilities for reading and writing JSON files with proper error handling,
automatic directory creation, and support for wrapped JSON formats.

Functions:
    - read_json: Read JSON from file
    - write_json: Write JSON to file (creates directories if needed)
    - read_json_maybe_wrapped: Read JSON that may be wrapped in {"data": ...}
    - read_json_safe: Read JSON with comprehensive error handling
    - write_json_pretty: Write JSON with custom formatting

Usage:
    from moata_pipeline.common.json_io import read_json, write_json
    
    # Read JSON
    data = read_json(Path("data.json"))
    
    # Write JSON (auto-creates directories)
    write_json(Path("outputs/results.json"), {"status": "complete"})
    
    # Handle wrapped JSON
    data = read_json_maybe_wrapped(Path("api_response.json"))

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2024-12-28
"""

import json
import logging
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)


class JSONReadError(Exception):
    """Raised when JSON file cannot be read."""
    pass


class JSONWriteError(Exception):
    """Raised when JSON file cannot be written."""
    pass


def read_json(path: Path) -> Any:
    """
    Read JSON data from file.
    
    Args:
        path: Path to JSON file
        
    Returns:
        Parsed JSON data (typically dict or list)
        
    Raises:
        FileNotFoundError: If file doesn't exist
        JSONReadError: If file cannot be parsed as JSON
        
    Example:
        >>> data = read_json(Path("config.json"))
        >>> print(data['setting'])
    """
    if not isinstance(path, Path):
        path = Path(path)
    
    if not path.exists():
        raise FileNotFoundError(f"JSON file not found: {path}")
    
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        raise JSONReadError(
            f"Invalid JSON in {path}: {e}"
        ) from e
    except Exception as e:
        raise JSONReadError(
            f"Failed to read JSON from {path}: {e}"
        ) from e


def write_json(
    path: Path,
    data: Any,
    indent: int = 2,
    ensure_ascii: bool = False
) -> None:
    """
    Write JSON data to file.
    
    Automatically creates parent directories if they don't exist.
    
    Args:
        path: Path to JSON file
        data: Data to write (must be JSON-serializable)
        indent: Number of spaces for indentation (default: 2)
        ensure_ascii: Whether to escape non-ASCII characters (default: False)
        
    Raises:
        JSONWriteError: If data cannot be serialized or written
        
    Example:
        >>> data = {"name": "Auckland", "stations": 200}
        >>> write_json(Path("outputs/summary.json"), data)
    """
    if not isinstance(path, Path):
        path = Path(path)
    
    # Create parent directories
    path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        with path.open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=indent, ensure_ascii=ensure_ascii)
    except TypeError as e:
        raise JSONWriteError(
            f"Data is not JSON-serializable: {e}"
        ) from e
    except Exception as e:
        raise JSONWriteError(
            f"Failed to write JSON to {path}: {e}"
        ) from e
    
    logger.debug(f"Wrote JSON to {path}")


def read_json_maybe_wrapped(path: Path) -> Any:
    """
    Read JSON that may be wrapped in {"data": ...} format.
    
    Some APIs return data wrapped like:
        {"data": [...actual data...]}
    
    This function automatically unwraps such responses.
    
    Args:
        path: Path to JSON file
        
    Returns:
        Unwrapped data if wrapped, otherwise original data
        
    Example:
        >>> # File contains: {"data": [1, 2, 3]}
        >>> result = read_json_maybe_wrapped(Path("wrapped.json"))
        >>> print(result)
        [1, 2, 3]
        
        >>> # File contains: [1, 2, 3]
        >>> result = read_json_maybe_wrapped(Path("plain.json"))
        >>> print(result)
        [1, 2, 3]
    """
    obj = read_json(path)
    
    # Check if wrapped
    if isinstance(obj, dict) and "data" in obj:
        return obj["data"]
    
    return obj


def read_json_safe(
    path: Path,
    default: Any = None
) -> Any:
    """
    Read JSON with error handling, returning default on failure.
    
    Args:
        path: Path to JSON file
        default: Value to return if reading fails (default: None)
        
    Returns:
        Parsed JSON data, or default value if reading fails
        
    Example:
        >>> # File exists and is valid
        >>> data = read_json_safe(Path("config.json"))
        
        >>> # File doesn't exist - returns None
        >>> data = read_json_safe(Path("missing.json"))
        >>> print(data)
        None
        
        >>> # File doesn't exist - returns custom default
        >>> data = read_json_safe(Path("missing.json"), default={})
        >>> print(data)
        {}
    """
    try:
        return read_json(path)
    except (FileNotFoundError, JSONReadError) as e:
        logger.debug(f"Failed to read JSON from {path}: {e}")
        return default


def write_json_pretty(
    path: Path,
    data: Any,
    indent: int = 4,
    sort_keys: bool = True
) -> None:
    """
    Write JSON with pretty formatting for human readability.
    
    Args:
        path: Path to JSON file
        data: Data to write
        indent: Indentation spaces (default: 4 for readability)
        sort_keys: Whether to sort dictionary keys (default: True)
        
    Example:
        >>> data = {"z": 3, "a": 1, "m": 2}
        >>> write_json_pretty(Path("sorted.json"), data)
        # Result (sorted and indented):
        # {
        #     "a": 1,
        #     "m": 2,
        #     "z": 3
        # }
    """
    if not isinstance(path, Path):
        path = Path(path)
    
    path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        with path.open("w", encoding="utf-8") as f:
            json.dump(
                data,
                f,
                indent=indent,
                sort_keys=sort_keys,
                ensure_ascii=False
            )
    except Exception as e:
        raise JSONWriteError(
            f"Failed to write pretty JSON to {path}: {e}"
        ) from e
    
    logger.debug(f"Wrote pretty JSON to {path}")


def validate_json_structure(
    data: Any,
    required_keys: Optional[list[str]] = None,
    expected_type: Optional[type] = None
) -> bool:
    """
    Validate JSON data structure.
    
    Args:
        data: JSON data to validate
        required_keys: List of required keys (for dict validation)
        expected_type: Expected root type (dict, list, etc.)
        
    Returns:
        True if valid, False otherwise
        
    Example:
        >>> data = {"name": "Test", "value": 42}
        >>> validate_json_structure(data, required_keys=["name", "value"])
        True
        
        >>> validate_json_structure(data, expected_type=dict)
        True
        
        >>> validate_json_structure(data, required_keys=["missing_key"])
        False
    """
    # Check expected type
    if expected_type is not None:
        if not isinstance(data, expected_type):
            logger.warning(
                f"Expected type {expected_type.__name__}, got {type(data).__name__}"
            )
            return False
    
    # Check required keys (only for dicts)
    if required_keys is not None:
        if not isinstance(data, dict):
            logger.warning(
                f"Cannot check required_keys on non-dict type: {type(data).__name__}"
            )
            return False
        
        missing_keys = [k for k in required_keys if k not in data]
        if missing_keys:
            logger.warning(f"Missing required keys: {missing_keys}")
            return False
    
    return True