"""Output management for atomic writes and file handling."""
import json
import shutil
import time
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from .base import BaseCollector


def _robust_rmtree(path: Path, max_retries: int = 3, delay: float = 0.5) -> bool:
    """
    Robustly remove directory tree with retries for Windows file locking.
    
    Args:
        path: Directory to remove
        max_retries: Maximum retry attempts
        delay: Delay between retries in seconds
        
    Returns:
        True if removed successfully, False otherwise
    """
    for attempt in range(max_retries):
        try:
            if path.exists():
                # On Windows, try to release file handles first
                import gc
                gc.collect()
                
                shutil.rmtree(path, ignore_errors=False)
            return True
        except PermissionError:
            if attempt < max_retries - 1:
                time.sleep(delay)
            else:
                # Last resort: try ignoring errors
                try:
                    shutil.rmtree(path, ignore_errors=True)
                    return not path.exists()
                except:
                    return False
        except Exception:
            return False
    return False


class OutputManager(BaseCollector):
    """
    Manages file output with atomic writes.
    
    Single Responsibility: File I/O operations only
    
    Responsibilities:
    - Setup temp directories for atomic writes
    - Write JSON/CSV files
    - Finalize output (move from temp to final location)
    - Cleanup temp directories
    
    Atomic Write Strategy:
    1. Write to temporary directory first
    2. Only move to final location after successful write
    3. Prevents corruption of existing data if operation fails
    
    Example:
        >>> manager = OutputManager(client, base_dir=Path("outputs/rain_gauges"))
        >>> manager.setup_temp_dir()
        >>> manager.write_json(data, "gauges.json")
        >>> manager.finalize_output()  # Moves temp to final location
    """
    
    def __init__(
        self,
        client: Any,  # Don't need HTTP client here, but keep for consistency
        base_dir: Optional[Path] = None,
        enable_atomic: bool = True
    ) -> None:
        """
        Initialize output manager.
        
        Args:
            client: HTTP client (unused, for consistency with BaseCollector)
            base_dir: Base output directory
            enable_atomic: Whether to use atomic writes (temp dir)
        """
        super().__init__(client)
        
        self._base_dir = Path(base_dir) if base_dir else None
        self._enable_atomic = enable_atomic and self._base_dir is not None
        self._temp_dir: Optional[Path] = None
        
        if self._enable_atomic and self._base_dir:
            self._temp_dir = self._base_dir / "_temp"
    
    def setup_temp_dir(self) -> None:
        """
        Setup temporary directory for atomic writes.
        
        Creates temp directory and cleans any existing files.
        
        Example:
            >>> manager.setup_temp_dir()
            >>> # Write to temp dir
            >>> manager.finalize_output()  # Move to final location
        """
        if not self._enable_atomic or self._temp_dir is None:
            self._logger.debug("Atomic writes disabled, skipping temp dir setup")
            return
        
        self._logger.info(f"Setting up temp directory: {self._temp_dir}")
        
        # Clean existing temp dir if present (with retry for Windows file locking)
        if self._temp_dir.exists():
            self._logger.warning(f"Temp dir already exists, cleaning: {self._temp_dir}")
            if not _robust_rmtree(self._temp_dir):
                # If still can't delete, try using unique temp dir name
                import uuid
                unique_suffix = uuid.uuid4().hex[:8]
                self._temp_dir = self._base_dir / f"_temp_{unique_suffix}"
                self._logger.warning(f"Using unique temp dir: {self._temp_dir}")
        
        # Create temp dir
        self._temp_dir.mkdir(parents=True, exist_ok=True)
        self._logger.info(f"✓ Temp directory ready: {self._temp_dir}")
    
    def cleanup_temp_dir(self) -> None:
        """
        Remove temporary directory and all contents.
        
        Use after finalize_output() or on error cleanup.
        
        Example:
            >>> try:
            ...     manager.setup_temp_dir()
            ...     # ... write operations ...
            ...     manager.finalize_output()
            ... finally:
            ...     manager.cleanup_temp_dir()  # Clean up even on error
        """
        if not self._enable_atomic or self._temp_dir is None:
            return
        
        if not self._temp_dir.exists():
            self._logger.debug("Temp dir does not exist, nothing to clean")
            return
        
        self._logger.info(f"Cleaning up temp directory: {self._temp_dir}")
        
        try:
            if _robust_rmtree(self._temp_dir):
                self._logger.info("✓ Temp directory removed")
            else:
                self._logger.warning("Temp directory could not be fully removed (may be locked)")
        except Exception as e:
            self._logger.error(f"Failed to remove temp dir: {e}")
    
    def finalize_output(self) -> None:
        """
        Move files from temp directory to final location.
        
        This is the atomic commit - only happens after all writes succeed.
        
        Raises:
            RuntimeError: If temp dir doesn't exist or base dir is not set
            
        Example:
            >>> manager.setup_temp_dir()
            >>> manager.write_json(data, "output.json")
            >>> manager.finalize_output()  # Commit
            >>> manager.cleanup_temp_dir()  # Clean up temp
        """
        if not self._enable_atomic or self._temp_dir is None or self._base_dir is None:
            self._logger.debug("Atomic writes disabled, nothing to finalize")
            return
        
        if not self._temp_dir.exists():
            raise RuntimeError(f"Temp directory does not exist: {self._temp_dir}")
        
        self._logger.info("Finalizing output (moving from temp to final location)...")
        self._logger.info(f"  Temp: {self._temp_dir}")
        self._logger.info(f"  Final: {self._base_dir}")
        
        # Ensure base dir exists
        self._base_dir.mkdir(parents=True, exist_ok=True)
        
        # Move all files from temp to base
        moved_count = 0
        for temp_file in self._temp_dir.rglob("*"):
            if temp_file.is_file():
                # Calculate relative path and target
                rel_path = temp_file.relative_to(self._temp_dir)
                target_file = self._base_dir / rel_path
                
                # Create parent dirs if needed
                target_file.parent.mkdir(parents=True, exist_ok=True)
                
                # Move file
                shutil.move(str(temp_file), str(target_file))
                moved_count += 1
                self._logger.debug(f"  Moved: {rel_path}")
        
        self._logger.info(f"✓ Finalized {moved_count} files")
    
    def write_json(
        self,
        data: Any,
        filename: str,
        indent: int = 2
    ) -> Path:
        """
        Write data to JSON file.
        
        Writes to temp dir if atomic writes enabled, otherwise to base dir.
        
        Args:
            data: Data to serialize to JSON
            filename: Output filename
            indent: JSON indentation (default: 2)
            
        Returns:
            Path to written file
            
        Raises:
            ValueError: If no output directory configured
            
        Example:
            >>> path = manager.write_json({"key": "value"}, "data.json")
            >>> print(f"Wrote to {path}")
        """
        if self._enable_atomic and self._temp_dir:
            output_dir = self._temp_dir
        elif self._base_dir:
            output_dir = self._base_dir
        else:
            raise ValueError("No output directory configured")
        
        output_path = output_dir / filename
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        self._logger.info(f"Writing JSON: {output_path}")
        
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=indent, default=str)
        
        self._logger.info(f"✓ Wrote JSON ({output_path.stat().st_size:,} bytes)")
        
        return output_path
    
    def get_output_path(self, filename: str) -> Path:
        """
        Get output path for a file (respects atomic write strategy).
        
        Args:
            filename: Filename
            
        Returns:
            Path where file should be written
            
        Raises:
            ValueError: If no output directory configured
        """
        if self._enable_atomic and self._temp_dir:
            return self._temp_dir / filename
        elif self._base_dir:
            return self._base_dir / filename
        else:
            raise ValueError("No output directory configured")
