"""
Script Utilities Module

Shared utilities for CLI scripts to reduce code duplication.
Provides common functions for argument parsing, logging setup, and execution flow.

Functions:
    setup_script_logger: Configure logging for scripts
    create_base_arg_parser: Create base argument parser with common options
    add_data_source_args: Add data source arguments (current/date)
    add_logging_args: Add logging arguments
    parse_date_arg: Parse and validate date argument
    handle_script_error: Standard error handling for scripts
    print_script_header: Print formatted script header
    print_script_footer: Print formatted script footer
    
Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2026-01-21
Version: 1.0.0
"""

from __future__ import annotations

import argparse
import logging
import sys
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from moata_pipeline.logging_setup import setup_logging
from moata_pipeline.common.validation import (
    validate_date_string,
    validate_log_level,
)


# Version info
__version__ = "1.0.0"


# =============================================================================
# Logging Setup
# =============================================================================

def setup_script_logger(
    log_level: str = "INFO",
    script_name: Optional[str] = None
) -> logging.Logger:
    """
    Configure logging for a script and return logger.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        script_name: Name of script (for logger name)
        
    Returns:
        Configured logger instance
        
    Example:
        >>> logger = setup_script_logger("INFO", "retrieve_gauges")
        >>> logger.info("Script started")
    """
    # Validate log level
    log_level = validate_log_level(log_level)
    
    # Setup logging
    setup_logging(log_level)
    
    # Get logger
    if script_name:
        logger = logging.getLogger(script_name)
    else:
        logger = logging.getLogger(__name__)
    
    return logger


# =============================================================================
# Argument Parsing
# =============================================================================

def create_base_arg_parser(
    description: str,
    script_name: str,
    version: str = "1.0.0",
    epilog: Optional[str] = None
) -> argparse.ArgumentParser:
    """
    Create base argument parser with common structure.
    
    Args:
        description: Script description
        script_name: Name of the script
        version: Script version
        epilog: Additional help text at the end
        
    Returns:
        Configured ArgumentParser
        
    Example:
        >>> parser = create_base_arg_parser(
        ...     description="Retrieve rain gauge data",
        ...     script_name="retrieve_gauges.py",
        ...     version="1.2.0"
        ... )
    """
    parser = argparse.ArgumentParser(
        description=description,
        prog=script_name,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=epilog
    )
    
    # Add version argument
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {version}"
    )
    
    return parser


def add_data_source_args(
    parser: argparse.ArgumentParser,
    support_current: bool = True,
    support_date: bool = True,
    support_date_range: bool = False
) -> None:
    """
    Add data source arguments to parser (current/date/date-range).
    
    Args:
        parser: ArgumentParser to modify
        support_current: Add --current flag
        support_date: Add --date argument
        support_date_range: Add --from-date and --to-date arguments
        
    Example:
        >>> parser = argparse.ArgumentParser()
        >>> add_data_source_args(parser)
        >>> args = parser.parse_args(["--date", "2025-01-15"])
    """
    source_group = parser.add_argument_group('Data Source Options')
    
    if support_current or support_date or support_date_range:
        source_mutex = source_group.add_mutually_exclusive_group()
        
        if support_current:
            source_mutex.add_argument(
                "--current",
                action="store_true",
                help="Process current/recent data (last 24-48 hours)"
            )
        
        if support_date:
            source_mutex.add_argument(
                "--date",
                metavar="YYYY-MM-DD",
                help="Process specific historical date. Example: --date 2025-05-09"
            )
        
        if support_date_range:
            source_mutex.add_argument(
                "--from-date",
                metavar="YYYY-MM-DD",
                help="Start date for date range. Use with --to-date"
            )
    
    if support_date_range:
        source_group.add_argument(
            "--to-date",
            metavar="YYYY-MM-DD",
            help="End date for date range. Use with --from-date"
        )


def add_logging_args(
    parser: argparse.ArgumentParser,
    default_level: str = "INFO"
) -> None:
    """
    Add logging arguments to parser.
    
    Args:
        parser: ArgumentParser to modify
        default_level: Default log level
        
    Example:
        >>> parser = argparse.ArgumentParser()
        >>> add_logging_args(parser)
        >>> args = parser.parse_args(["--log-level", "DEBUG"])
    """
    log_group = parser.add_argument_group('Logging Options')
    
    log_group.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default=default_level,
        help=f"Set logging level (default: {default_level}). "
             "Use DEBUG for verbose output."
    )


def parse_date_arg(
    date_str: str,
    arg_name: str = "date"
) -> datetime:
    """
    Parse and validate date argument.
    
    Args:
        date_str: Date string in YYYY-MM-DD format
        arg_name: Argument name (for error messages)
        
    Returns:
        Parsed datetime object (at midnight)
        
    Raises:
        argparse.ArgumentTypeError: If date is invalid
        
    Example:
        >>> dt = parse_date_arg("2025-01-15")
        >>> dt.year
        2025
    """
    try:
        return validate_date_string(date_str, "%Y-%m-%d", arg_name)
    except ValueError as e:
        raise argparse.ArgumentTypeError(str(e))


# =============================================================================
# Script Execution Helpers
# =============================================================================

def print_script_header(
    title: str,
    version: str,
    logger: logging.Logger,
    width: int = 80
) -> None:
    """
    Print formatted script header.
    
    Args:
        title: Script title
        version: Script version
        logger: Logger to use
        width: Width of separator line
        
    Example:
        >>> logger = logging.getLogger(__name__)
        >>> print_script_header("Rain Gauge Retrieval", "1.2.0", logger)
    """
    logger.info("=" * width)
    logger.info(f"{title} - v{version}")
    logger.info("=" * width)


def print_script_footer(
    logger: logging.Logger,
    success: bool = True,
    width: int = 80
) -> None:
    """
    Print formatted script footer.
    
    Args:
        logger: Logger to use
        success: Whether script completed successfully
        width: Width of separator line
        
    Example:
        >>> logger = logging.getLogger(__name__)
        >>> print_script_footer(logger, success=True)
    """
    logger.info("=" * width)
    if success:
        logger.info("✓ Script completed successfully")
    else:
        logger.error("✗ Script completed with errors")
    logger.info("=" * width)


def handle_script_error(
    error: Exception,
    logger: logging.Logger,
    exit_code: int = 1,
    show_traceback: bool = False
) -> int:
    """
    Handle script errors with consistent formatting.
    
    Args:
        error: Exception that occurred
        logger: Logger to use
        exit_code: Exit code to return
        show_traceback: Whether to show full traceback
        
    Returns:
        Exit code
        
    Example:
        >>> try:
        ...     risky_operation()
        ... except Exception as e:
        ...     return handle_script_error(e, logger)
    """
    logger.error(f"Error: {error}")
    
    if show_traceback:
        logger.error("Full traceback:")
        logger.error(traceback.format_exc())
    
    print_script_footer(logger, success=False)
    
    return exit_code


def handle_keyboard_interrupt(
    logger: logging.Logger,
    message: str = "Script interrupted by user (Ctrl+C)"
) -> int:
    """
    Handle keyboard interrupt (Ctrl+C) consistently.
    
    Args:
        logger: Logger to use
        message: Custom message to display
        
    Returns:
        Exit code 130 (standard for SIGINT)
        
    Example:
        >>> try:
        ...     long_running_operation()
        ... except KeyboardInterrupt:
        ...     return handle_keyboard_interrupt(logger)
    """
    logger.warning("")
    logger.warning(message)
    print_script_footer(logger, success=False)
    return 130


# =============================================================================
# Data Mode Detection
# =============================================================================

def detect_data_mode(
    args: argparse.Namespace,
    logger: logging.Logger
) -> tuple[str, Optional[datetime], Optional[datetime]]:
    """
    Detect data mode from parsed arguments (current/historical).
    
    Args:
        args: Parsed command-line arguments
        logger: Logger for output
        
    Returns:
        Tuple of (mode, start_time, end_time)
            mode: "current" or "historical"
            start_time: Start datetime (None for current mode)
            end_time: End datetime (None for current mode)
        
    Example:
        >>> args = parser.parse_args(["--date", "2025-05-09"])
        >>> mode, start, end = detect_data_mode(args, logger)
        >>> print(mode)
        'historical'
    """
    # Check for explicit current mode
    if hasattr(args, 'current') and args.current:
        logger.info("Mode: Current (last 24-48 hours)")
        return "current", None, None
    
    # Check for date
    if hasattr(args, 'date') and args.date:
        logger.info("Mode: Historical (single date)")
        start_time = parse_date_arg(args.date, "--date")
        end_time = start_time + timedelta(days=1)
        logger.info(f"Date: {args.date}")
        return "historical", start_time, end_time
    
    # Check for date range
    if hasattr(args, 'from_date') and args.from_date:
        if not hasattr(args, 'to_date') or not args.to_date:
            raise ValueError("--from-date requires --to-date")
        
        logger.info("Mode: Historical (date range)")
        start_time = parse_date_arg(args.from_date, "--from-date")
        end_time = parse_date_arg(args.to_date, "--to-date")
        
        if end_time <= start_time:
            raise ValueError("--to-date must be after --from-date")
        
        logger.info(f"Date range: {args.from_date} to {args.to_date}")
        return "historical", start_time, end_time
    
    # Default to current mode
    logger.info("Mode: Current (default)")
    return "current", None, None


# =============================================================================
# Script Execution Wrapper
# =============================================================================

def run_script(
    main_func: Callable[[], int],
    script_name: str,
    script_version: str,
    logger: Optional[logging.Logger] = None
) -> int:
    """
    Wrapper for script main function with consistent error handling.
    
    Args:
        main_func: Main script function to execute (returns int exit code)
        script_name: Name of the script
        script_version: Version string
        logger: Optional logger (will create one if not provided)
        
    Returns:
        Exit code (0=success, 1=error, 130=interrupted)
        
    Example:
        >>> def main() -> int:
        ...     print("Running script...")
        ...     return 0
        >>> 
        >>> if __name__ == "__main__":
        ...     sys.exit(run_script(main, "my_script.py", "1.0.0"))
    """
    if logger is None:
        logger = logging.getLogger(script_name)
    
    try:
        # Run main function
        exit_code = main_func()
        return exit_code if exit_code is not None else 0
        
    except KeyboardInterrupt:
        return handle_keyboard_interrupt(logger)
        
    except Exception as e:
        return handle_script_error(e, logger, show_traceback=True)


# =============================================================================
# Configuration Helpers
# =============================================================================

def print_configuration_summary(
    config: Dict[str, Any],
    logger: logging.Logger,
    title: str = "Configuration"
) -> None:
    """
    Print formatted configuration summary.
    
    Args:
        config: Configuration dictionary
        logger: Logger to use
        title: Title for the configuration section
        
    Example:
        >>> config = {
        ...     "mode": "historical",
        ...     "date": "2025-05-09",
        ...     "threshold": 5.0
        ... }
        >>> print_configuration_summary(config, logger)
    """
    logger.info(f"\\n{title}:")
    logger.info("-" * 40)
    
    for key, value in config.items():
        # Format key (convert underscore to space, capitalize)
        key_formatted = key.replace("_", " ").title()
        
        # Format value
        if isinstance(value, Path):
            value_str = str(value)
        elif isinstance(value, datetime):
            value_str = value.strftime("%Y-%m-%d %H:%M:%S")
        elif isinstance(value, float):
            value_str = f"{value:.2f}"
        elif isinstance(value, bool):
            value_str = "Yes" if value else "No"
        else:
            value_str = str(value)
        
        logger.info(f"  {key_formatted:25s}: {value_str}")
    
    logger.info("-" * 40)


def confirm_action(
    prompt: str,
    default: bool = True
) -> bool:
    """
    Prompt user for confirmation (y/n).
    
    Args:
        prompt: Prompt message
        default: Default answer if user presses Enter
        
    Returns:
        True if user confirmed, False otherwise
        
    Example:
        >>> if confirm_action("Delete all files?", default=False):
        ...     delete_files()
    """
    default_str = "[Y/n]" if default else "[y/N]"
    
    while True:
        response = input(f"{prompt} {default_str}: ").strip().lower()
        
        if not response:
            return default
        
        if response in ['y', 'yes']:
            return True
        elif response in ['n', 'no']:
            return False
        else:
            print("Please answer 'y' or 'n'")


# =============================================================================
# Date Range Helpers - Auto-detect 24-hour backward
# =============================================================================

def get_default_historical_date(
    logger: logging.Logger,
    pipeline_type: str = "radar"
) -> Optional[str]:
    """
    Auto-detect most recent historical date for a pipeline.
    
    This looks for the most recent data directory and returns its date.
    If no data found, returns None (which triggers current mode).
    
    Args:
        logger: Logger for output
        pipeline_type: "radar" or "gauge"
        
    Returns:
        Date string in YYYY-MM-DD format, or None if no data found
        
    Example:
        >>> date_str = get_default_historical_date(logger, "radar")
        >>> if date_str:
        ...     print(f"Using historical date: {date_str}")
    """
    if pipeline_type == "radar":
        base_path = Path("outputs/rain_radar")
    elif pipeline_type == "gauge":
        base_path = Path("outputs/rain_gauges")
    else:
        logger.warning(f"Unknown pipeline type: {pipeline_type}")
        return None
    
    if not base_path.exists():
        logger.debug(f"Base path not found: {base_path}")
        return None
    
    # Look for YYYY/MM/DD/raw or YYYY/MM/DD/analyze directories
    date_dirs = sorted(
        base_path.glob("????/??/??"),
        reverse=True  # Most recent first
    )
    
    if not date_dirs:
        logger.debug(f"No date directories found in {base_path}")
        return None
    
    # Return most recent date
    most_recent = date_dirs[0]
    year = most_recent.parent.parent.name
    month = most_recent.parent.name
    day = most_recent.name
    date_str = f"{year}-{month}-{day}"
    
    logger.info(f"✓ Found most recent data: {date_str}")
    return date_str


def resolve_analysis_date(
    args: argparse.Namespace,
    logger: logging.Logger,
    pipeline_type: str = "radar",
    default_lookback_hours: int = 24
) -> tuple[str, str]:
    """
    Resolve which date to use for analysis/visualization.
    
    When no --date is specified, auto-detects most recent historical date.
    This implements the "24 hours backward" pattern where:
    - If no args: use most recent historical date (e.g., "2026-01-20")
    - If --date 2026-01-21: use that date (data stored in 2026/01/20 folder)
    - If --current: use today
    
    Args:
        args: Parsed arguments
        logger: Logger for output
        pipeline_type: "radar" or "gauge"
        default_lookback_hours: Hours to look back (default: 24)
        
    Returns:
        Tuple of (mode, date_str)
            mode: "current" or "historical"
            date_str: Date in YYYY-MM-DD format
        
    Example:
        >>> args = parser.parse_args([])  # No arguments
        >>> mode, date_str = resolve_analysis_date(args, logger, "radar")
        >>> print(f"Mode: {mode}, Date: {date_str}")
        Mode: historical, Date: 2026-01-20
    """
    # Explicit current mode
    if hasattr(args, 'current') and args.current:
        logger.info("Mode: Current (explicit)")
        return "current", datetime.now().strftime("%Y-%m-%d")
    
    # Explicit date provided
    if hasattr(args, 'date') and args.date:
        logger.info(f"Mode: Historical (date specified: {args.date})")
        return "historical", args.date
    
    # Auto-detect most recent date
    logger.info("Auto-detecting most recent historical data...")
    detected_date = get_default_historical_date(logger, pipeline_type)
    
    if detected_date:
        logger.info(f"Using historical date: {detected_date}")
        return "historical", detected_date
    else:
        logger.info("No historical data found, falling back to current mode")
        return "current", datetime.now().strftime("%Y-%m-%d")
