"""
Output Writer Module

Provides classes for writing data to various output formats (JSON, CSV, TXT).

Classes:
    JsonOutputWriter: Write data to JSON files
    CsvOutputWriter: Write DataFrame to CSV files
    TextOutputWriter: Write text to files

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2024-12-28
Version: 1.0.0
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd


# Version info
__version__ = "1.0.0"


# =============================================================================
# Custom Exceptions
# =============================================================================

class OutputWriterError(Exception):
    """Base exception for output writer errors."""
    pass


class WriteError(OutputWriterError):
    """Raised when file writing fails."""
    pass


# =============================================================================
# JSON Output Writer
# =============================================================================

class JsonOutputWriter:
    """
    Write data to JSON files with automatic directory creation.
    
    Attributes:
        _out_dir: Output directory path
        
    Example:
        >>> writer = JsonOutputWriter(Path("outputs/data"))
        >>> writer.write_json("results.json", {"status": "success"})
        PosixPath('outputs/data/results.json')
    """
    
    def __init__(self, out_dir: Path) -> None:
        """
        Initialize JSON writer.
        
        Args:
            out_dir: Output directory path
            
        Raises:
            TypeError: If out_dir is not a Path
        """
        if not isinstance(out_dir, Path):
            out_dir = Path(out_dir)
        
        self._out_dir = out_dir
    
    def ensure_dir(self) -> None:
        """
        Create output directory if it doesn't exist.
        
        Creates parent directories as needed.
        """
        self._out_dir.mkdir(parents=True, exist_ok=True)
    
    def write_json(
        self,
        filename: str,
        data: Any,
        indent: int = 2,
        ensure_ascii: bool = False,
    ) -> Path:
        """
        Write data to JSON file.
        
        Args:
            filename: Output filename
            data: Data to write (must be JSON-serializable)
            indent: JSON indentation spaces (default: 2)
            ensure_ascii: Whether to escape non-ASCII characters
            
        Returns:
            Path to written file
            
        Raises:
            WriteError: If writing fails or data not serializable
        """
        self.ensure_dir()
        
        path = self._out_dir / filename
        
        try:
            json_str = json.dumps(
                data,
                indent=indent,
                ensure_ascii=ensure_ascii,
                default=str,  # Convert non-serializable objects to strings
            )
            path.write_text(json_str, encoding="utf-8")
            return path
            
        except TypeError as e:
            raise WriteError(
                f"Failed to serialize data to JSON: {e}\n"
                f"Data type: {type(data).__name__}"
            ) from e
        except Exception as e:
            raise WriteError(
                f"Failed to write JSON file {path}: {e}"
            ) from e
    
    def write_rain_gauges(self, gauges: List[Dict[str, Any]]) -> Path:
        """
        Write rain gauges list to rain_gauges.json.
        
        Args:
            gauges: List of gauge dictionaries
            
        Returns:
            Path to written file
        """
        if not isinstance(gauges, list):
            raise TypeError(f"gauges must be a list, got {type(gauges).__name__}")
        
        return self.write_json("rain_gauges.json", gauges)
    
    def write_combined(self, all_data: List[Dict[str, Any]]) -> Path:
        """
        Write complete gauge data with traces and alarms.
        
        Args:
            all_data: List of gauge data with traces and alarms
            
        Returns:
            Path to written file
        """
        if not isinstance(all_data, list):
            raise TypeError(f"all_data must be a list, got {type(all_data).__name__}")
        
        return self.write_json("rain_gauges_traces_alarms.json", all_data)
    
    def write_catchments(self, catchments: List[Dict[str, Any]]) -> Path:
        """
        Write stormwater catchments to catchments.json.
        
        Args:
            catchments: List of catchment dictionaries
            
        Returns:
            Path to written file
        """
        if not isinstance(catchments, list):
            raise TypeError(f"catchments must be a list, got {type(catchments).__name__}")
        
        return self.write_json("catchments.json", catchments)
    
    def write_summary(self, summary_data: Dict[str, Any]) -> Path:
        """
        Write summary data to summary.json.
        
        Args:
            summary_data: Summary dictionary
            
        Returns:
            Path to written file
        """
        if not isinstance(summary_data, dict):
            raise TypeError(f"summary_data must be a dict, got {type(summary_data).__name__}")
        
        return self.write_json("summary.json", summary_data)


# =============================================================================
# CSV Output Writer
# =============================================================================

class CsvOutputWriter:
    """
    Write DataFrames to CSV files with automatic directory creation.
    
    Attributes:
        _out_dir: Output directory path
        
    Example:
        >>> import pandas as pd
        >>> writer = CsvOutputWriter(Path("outputs/data"))
        >>> df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        >>> writer.write_csv("data.csv", df)
        PosixPath('outputs/data/data.csv')
    """
    
    def __init__(self, out_dir: Path) -> None:
        """
        Initialize CSV writer.
        
        Args:
            out_dir: Output directory path
        """
        if not isinstance(out_dir, Path):
            out_dir = Path(out_dir)
        
        self._out_dir = out_dir
    
    def ensure_dir(self) -> None:
        """Create output directory if it doesn't exist."""
        self._out_dir.mkdir(parents=True, exist_ok=True)
    
    def write_csv(
        self,
        filename: str,
        df: pd.DataFrame,
        index: bool = False,
        **kwargs,
    ) -> Path:
        """
        Write DataFrame to CSV file.
        
        Args:
            filename: Output filename
            df: DataFrame to write
            index: Whether to write index column (default: False)
            **kwargs: Additional arguments passed to df.to_csv()
            
        Returns:
            Path to written file
            
        Raises:
            TypeError: If df is not a DataFrame
            WriteError: If writing fails
        """
        if not isinstance(df, pd.DataFrame):
            raise TypeError(f"df must be a DataFrame, got {type(df).__name__}")
        
        self.ensure_dir()
        path = self._out_dir / filename
        
        try:
            df.to_csv(path, index=index, **kwargs)
            return path
        except Exception as e:
            raise WriteError(f"Failed to write CSV file {path}: {e}") from e
    
    def write_summary(self, df: pd.DataFrame) -> Path:
        """
        Write summary DataFrame to summary.csv.
        
        Args:
            df: Summary DataFrame
            
        Returns:
            Path to written file
        """
        return self.write_csv("summary.csv", df)
    
    def write_alarms(self, df: pd.DataFrame) -> Path:
        """
        Write alarms DataFrame to alarm_summary.csv.
        
        Args:
            df: Alarms DataFrame
            
        Returns:
            Path to written file
        """
        return self.write_csv("alarm_summary.csv", df)


# =============================================================================
# Text Output Writer
# =============================================================================

class TextOutputWriter:
    """
    Write text to files with automatic directory creation.
    
    Attributes:
        _out_dir: Output directory path
        
    Example:
        >>> writer = TextOutputWriter(Path("outputs/reports"))
        >>> writer.write_text("report.txt", "Analysis complete!")
        PosixPath('outputs/reports/report.txt')
    """
    
    def __init__(self, out_dir: Path) -> None:
        """
        Initialize text writer.
        
        Args:
            out_dir: Output directory path
        """
        if not isinstance(out_dir, Path):
            out_dir = Path(out_dir)
        
        self._out_dir = out_dir
    
    def ensure_dir(self) -> None:
        """Create output directory if it doesn't exist."""
        self._out_dir.mkdir(parents=True, exist_ok=True)
    
    def write_text(
        self,
        filename: str,
        content: str,
        encoding: str = "utf-8",
    ) -> Path:
        """
        Write text content to file.
        
        Args:
            filename: Output filename
            content: Text content to write
            encoding: Text encoding (default: utf-8)
            
        Returns:
            Path to written file
            
        Raises:
            WriteError: If writing fails
        """
        self.ensure_dir()
        path = self._out_dir / filename
        
        try:
            path.write_text(content, encoding=encoding)
            return path
        except Exception as e:
            raise WriteError(f"Failed to write text file {path}: {e}") from e
    
    def write_report(self, content: str) -> Path:
        """
        Write analysis report to analysis_report.txt.
        
        Args:
            content: Report content
            
        Returns:
            Path to written file
        """
        return self.write_text("analysis_report.txt", content)
    
    def write_log(self, content: str) -> Path:
        """
        Write log content to log.txt.
        
        Args:
            content: Log content
            
        Returns:
            Path to written file
        """
        return self.write_text("log.txt", content)
    
    def append_text(
        self,
        filename: str,
        content: str,
        encoding: str = "utf-8",
    ) -> Path:
        """
        Append text content to existing file.
        
        Args:
            filename: Output filename
            content: Text content to append
            encoding: Text encoding (default: utf-8)
            
        Returns:
            Path to file
            
        Raises:
            WriteError: If appending fails
        """
        self.ensure_dir()
        path = self._out_dir / filename
        
        try:
            with path.open("a", encoding=encoding) as f:
                f.write(content)
            return path
        except Exception as e:
            raise WriteError(f"Failed to append to text file {path}: {e}") from e