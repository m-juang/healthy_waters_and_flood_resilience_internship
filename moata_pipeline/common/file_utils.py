"""
File System Utilities

Provides utilities for file and directory operations with proper error handling.

Functions:
    - ensure_dir: Create directory if it doesn't exist
    - clean_filename: Sanitize filename for safe file system use
    - get_file_size: Get file size in human-readable format
    - list_files: List files in directory with optional filtering
    - copy_file_safe: Copy file with error handling
    - move_file_safe: Move file with error handling
    - delete_file_safe: Delete file with error handling

Usage:
    from moata_pipeline.common.file_utils import (
        ensure_dir,
        clean_filename,
        get_file_size
    )
    
    # Create directory
    ensure_dir(Path("outputs/reports"))
    
    # Clean filename
    safe_name = clean_filename("Report: 2025-01-15 (Final).csv")
    # Result: "Report_2025-01-15_Final.csv"
    
    # Get file size
    size = get_file_size(Path("large_file.csv"))
    # Result: "15.3 MB"

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2024-12-28
"""

import logging
import re
import shutil
from pathlib import Path
from typing import List, Optional, Callable

logger = logging.getLogger(__name__)


def ensure_dir(p: Path) -> None:
    """
    Create directory if it doesn't exist (including parent directories).
    
    Safe to call multiple times - won't raise error if directory exists.
    
    Args:
        p: Path to directory
        
    Example:
        >>> ensure_dir(Path("outputs/rain_gauges/analyze"))
        >>> # Creates: outputs/ and outputs/rain_gauges/ and outputs/rain_gauges/analyze/
    """
    if not isinstance(p, Path):
        p = Path(p)
    
    p.mkdir(parents=True, exist_ok=True)
    logger.debug(f"Ensured directory exists: {p}")


def clean_filename(filename: str, replacement: str = "_") -> str:
    """
    Sanitize filename for safe file system use.
    
    Removes or replaces characters that are problematic in filenames:
    - Path separators (/ \\)
    - Special characters (: * ? " < > |)
    - Control characters
    - Leading/trailing whitespace and dots
    
    Args:
        filename: Original filename
        replacement: Character to replace invalid chars with (default: "_")
        
    Returns:
        Sanitized filename safe for all file systems
        
    Example:
        >>> clean_filename("Report: 2025-01-15 (Final).csv")
        'Report_2025-01-15_Final.csv'
        
        >>> clean_filename("File/with\\slashes.txt")
        'File_with_slashes.txt'
        
        >>> clean_filename("  .hidden  ")
        'hidden'
    """
    # Remove path separators and special characters
    cleaned = re.sub(r'[<>:"/\\|?*]', replacement, filename)
    
    # Remove control characters
    cleaned = re.sub(r'[\x00-\x1f\x7f]', '', cleaned)
    
    # Replace multiple spaces/underscores with single
    cleaned = re.sub(r'[ _]+', replacement, cleaned)
    
    # Remove leading/trailing whitespace, dots, and underscores
    cleaned = cleaned.strip(' ._')
    
    # Ensure not empty
    if not cleaned:
        cleaned = "file"
    
    return cleaned


def get_file_size(path: Path, unit: str = "auto") -> str:
    """
    Get file size in human-readable format.
    
    Args:
        path: Path to file
        unit: Size unit ("auto", "B", "KB", "MB", "GB")
              "auto" automatically selects appropriate unit
        
    Returns:
        Formatted file size string (e.g., "15.3 MB")
        
    Raises:
        FileNotFoundError: If file doesn't exist
        
    Example:
        >>> get_file_size(Path("data.csv"))
        '2.5 MB'
        
        >>> get_file_size(Path("small.txt"))
        '1.2 KB'
        
        >>> get_file_size(Path("data.csv"), unit="KB")
        '2560.0 KB'
    """
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")
    
    if not path.is_file():
        raise ValueError(f"Path is not a file: {path}")
    
    size_bytes = path.stat().st_size
    
    # Define units
    units_map = {
        "B": 1,
        "KB": 1024,
        "MB": 1024 ** 2,
        "GB": 1024 ** 3,
        "TB": 1024 ** 4,
    }
    
    if unit == "auto":
        # Auto-select unit
        if size_bytes < 1024:
            unit = "B"
        elif size_bytes < 1024 ** 2:
            unit = "KB"
        elif size_bytes < 1024 ** 3:
            unit = "MB"
        elif size_bytes < 1024 ** 4:
            unit = "GB"
        else:
            unit = "TB"
    
    if unit not in units_map:
        raise ValueError(f"Invalid unit: {unit}. Use: B, KB, MB, GB, TB, or auto")
    
    size = size_bytes / units_map[unit]
    
    # Format based on unit
    if unit == "B":
        return f"{int(size)} {unit}"
    else:
        return f"{size:.1f} {unit}"


def list_files(
    directory: Path,
    pattern: str = "*",
    recursive: bool = False,
    filter_fn: Optional[Callable[[Path], bool]] = None
) -> List[Path]:
    """
    List files in directory with optional filtering.
    
    Args:
        directory: Directory to search
        pattern: Glob pattern (default: "*" for all files)
        recursive: Whether to search subdirectories (default: False)
        filter_fn: Optional filter function (receives Path, returns bool)
        
    Returns:
        List of matching file paths
        
    Example:
        >>> # All CSV files in directory
        >>> csv_files = list_files(Path("data"), pattern="*.csv")
        
        >>> # All files recursively
        >>> all_files = list_files(Path("outputs"), recursive=True)
        
        >>> # Files larger than 1MB
        >>> large_files = list_files(
        ...     Path("data"),
        ...     filter_fn=lambda p: p.stat().st_size > 1024**2
        ... )
    """
    if not directory.exists():
        logger.warning(f"Directory not found: {directory}")
        return []
    
    if not directory.is_dir():
        logger.warning(f"Path is not a directory: {directory}")
        return []
    
    # Get files matching pattern
    if recursive:
        files = directory.rglob(pattern)
    else:
        files = directory.glob(pattern)
    
    # Filter to only files (not directories)
    files = [f for f in files if f.is_file()]
    
    # Apply custom filter if provided
    if filter_fn:
        files = [f for f in files if filter_fn(f)]
    
    return sorted(files)


def copy_file_safe(
    src: Path,
    dst: Path,
    overwrite: bool = False
) -> bool:
    """
    Copy file with error handling.
    
    Args:
        src: Source file path
        dst: Destination file path
        overwrite: Whether to overwrite existing file (default: False)
        
    Returns:
        True if successful, False otherwise
        
    Example:
        >>> copy_file_safe(
        ...     Path("data/source.csv"),
        ...     Path("backup/source.csv")
        ... )
        True
    """
    try:
        if not src.exists():
            logger.error(f"Source file not found: {src}")
            return False
        
        if dst.exists() and not overwrite:
            logger.warning(f"Destination exists (use overwrite=True): {dst}")
            return False
        
        # Ensure destination directory exists
        ensure_dir(dst.parent)
        
        # Copy file
        shutil.copy2(src, dst)
        logger.info(f"Copied {src} → {dst}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to copy {src} → {dst}: {e}")
        return False


def move_file_safe(
    src: Path,
    dst: Path,
    overwrite: bool = False
) -> bool:
    """
    Move file with error handling.
    
    Args:
        src: Source file path
        dst: Destination file path
        overwrite: Whether to overwrite existing file (default: False)
        
    Returns:
        True if successful, False otherwise
        
    Example:
        >>> move_file_safe(
        ...     Path("temp/data.csv"),
        ...     Path("outputs/data.csv")
        ... )
        True
    """
    try:
        if not src.exists():
            logger.error(f"Source file not found: {src}")
            return False
        
        if dst.exists() and not overwrite:
            logger.warning(f"Destination exists (use overwrite=True): {dst}")
            return False
        
        # Ensure destination directory exists
        ensure_dir(dst.parent)
        
        # Move file
        shutil.move(str(src), str(dst))
        logger.info(f"Moved {src} → {dst}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to move {src} → {dst}: {e}")
        return False


def delete_file_safe(path: Path, confirm: bool = True) -> bool:
    """
    Delete file with error handling and optional confirmation.
    
    Args:
        path: File path to delete
        confirm: Whether file must exist (raises warning if missing)
        
    Returns:
        True if deleted (or didn't exist), False on error
        
    Example:
        >>> delete_file_safe(Path("temp/old_data.csv"))
        True
    """
    try:
        if not path.exists():
            if confirm:
                logger.warning(f"File not found (already deleted?): {path}")
            return True
        
        if not path.is_file():
            logger.error(f"Path is not a file: {path}")
            return False
        
        path.unlink()
        logger.info(f"Deleted {path}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to delete {path}: {e}")
        return False


def get_directory_size(directory: Path) -> int:
    """
    Get total size of all files in directory (recursive).
    
    Args:
        directory: Directory path
        
    Returns:
        Total size in bytes
        
    Example:
        >>> size_bytes = get_directory_size(Path("outputs"))
        >>> size_mb = size_bytes / (1024 ** 2)
        >>> print(f"Directory size: {size_mb:.1f} MB")
    """
    if not directory.exists() or not directory.is_dir():
        return 0
    
    total_size = 0
    for file_path in directory.rglob("*"):
        if file_path.is_file():
            try:
                total_size += file_path.stat().st_size
            except (PermissionError, OSError):
                # Skip files we can't access
                pass
    
    return total_size