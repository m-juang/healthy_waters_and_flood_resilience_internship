"""
Logging Configuration Module

Centralized logging setup for the Auckland Council Rain Monitoring System.
Configures logging format, level, and handlers.

Usage:
    from moata_pipeline.logging_setup import setup_logging
    import logging
    
    setup_logging("INFO")
    logger = logging.getLogger(__name__)
    logger.info("Message here")

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2024-12-28
"""

import logging
import sys
from pathlib import Path


def setup_logging(level: str = "INFO", log_file: str | None = None) -> None:
    """
    Configure logging for the application.
    
    Sets up console logging with optional file logging. Uses a standardized
    format across all modules.
    
    Args:
        level: Logging level as string (DEBUG, INFO, WARNING, ERROR, CRITICAL).
               Case-insensitive. Defaults to INFO.
        log_file: Optional path to log file. If provided, logs will be written
                 to both console and file. Defaults to None (console only).
    
    Example:
        >>> setup_logging("DEBUG")
        >>> logger = logging.getLogger(__name__)
        >>> logger.debug("Debug message")
        
        >>> setup_logging("INFO", "outputs/logs/app.log")
        >>> logger = logging.getLogger(__name__)
        >>> logger.info("Message logged to console and file")
    
    Notes:
        - Multiple calls to setup_logging() will reconfigure the root logger
        - Log format: "YYYY-MM-DD HH:MM:SS [LEVEL] message"
        - Console output uses specified level
        - File output (if enabled) captures all levels >= DEBUG
    """
    # Convert string level to logging constant
    lvl = getattr(logging, level.upper(), logging.INFO)
    
    # Get root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)  # Capture everything, handlers will filter
    
    # Remove existing handlers to avoid duplicates
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Create formatter
    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # Console handler (stdout)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(lvl)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # File handler (optional)
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(log_path, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)  # File captures everything
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
        
        root_logger.info(f"📝 Logging to file: {log_path}")


def get_logger(name: str | None = None) -> logging.Logger:
    """
    Get a logger instance.
    
    Convenience function to get a logger. If name is None, returns the root logger.
    
    Args:
        name: Logger name, typically __name__ of the calling module.
              If None, returns root logger.
    
    Returns:
        Logger instance
        
    Example:
        >>> logger = get_logger(__name__)
        >>> logger.info("Message from module")
    """
    return logging.getLogger(name)