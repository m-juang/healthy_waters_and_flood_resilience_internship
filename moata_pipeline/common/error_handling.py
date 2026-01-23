"""
Error Handling Utilities Module

Provides enhanced error handling utilities, custom exceptions, and error
recovery strategies for the Moata pipeline.

Classes:
    PipelineError: Base exception for pipeline errors
    ConfigurationError: Configuration-related errors
    DataError: Data validation/processing errors  
    APIError: API interaction errors
    ErrorContext: Context manager for enhanced error handling
    
Functions:
    safe_execute: Execute function with automatic error handling
    retry_on_failure: Retry decorator with exponential backoff
    log_error_details: Log detailed error information
    create_error_report: Create formatted error report
    
Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2026-01-21
Version: 1.0.0
"""

from __future__ import annotations

import functools
import logging
import sys
import time
import traceback
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, TypeVar, Union


# Version info
__version__ = "1.0.0"


# Type variable for generic functions
T = TypeVar('T')


# =============================================================================
# Custom Exceptions
# =============================================================================

class PipelineError(Exception):
    """
    Base exception for pipeline errors.
    
    All custom pipeline exceptions should inherit from this.
    
    Attributes:
        message: Error message
        context: Additional context information
        timestamp: When the error occurred
    """
    
    def __init__(
        self,
        message: str,
        context: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Initialize pipeline error.
        
        Args:
            message: Error message
            context: Additional context (e.g., file path, line number)
        """
        super().__init__(message)
        self.message = message
        self.context = context or {}
        self.timestamp = datetime.now()
    
    def __str__(self) -> str:
        """Get formatted error message."""
        base_msg = self.message
        
        if self.context:
            context_str = ", ".join(
                f"{k}={v}" for k, v in self.context.items()
            )
            return f"{base_msg} [{context_str}]"
        
        return base_msg
    
    def get_details(self) -> Dict[str, Any]:
        """
        Get detailed error information.
        
        Returns:
            Dictionary with error details
        """
        return {
            "type": self.__class__.__name__,
            "message": self.message,
            "context": self.context,
            "timestamp": self.timestamp.isoformat(),
        }


class ConfigurationError(PipelineError):
    """
    Configuration-related errors.
    
    Raised when configuration is invalid or missing.
    
    Example:
        >>> raise ConfigurationError(
        ...     "Missing API credentials",
        ...     context={"file": ".env"}
        ... )
    """
    pass


class DataError(PipelineError):
    """
    Data validation or processing errors.
    
    Raised when data is invalid, missing, or corrupted.
    
    Example:
        >>> raise DataError(
        ...     "Invalid CSV format",
        ...     context={"file": "data.csv", "row": 42}
        ... )
    """
    pass


class APIError(PipelineError):
    """
    API interaction errors.
    
    Raised when API requests fail.
    
    Attributes:
        status_code: HTTP status code
        response_text: Response text from API
    
    Example:
        >>> raise APIError(
        ...     "Failed to fetch data",
        ...     context={"endpoint": "/assets", "status": 404}
        ... )
    """
    
    def __init__(
        self,
        message: str,
        status_code: Optional[int] = None,
        response_text: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Initialize API error.
        
        Args:
            message: Error message
            status_code: HTTP status code
            response_text: Response text from API
            context: Additional context
        """
        context = context or {}
        
        if status_code is not None:
            context["status_code"] = status_code
        
        if response_text is not None:
            context["response_text"] = response_text[:200]  # Truncate
        
        super().__init__(message, context)
        self.status_code = status_code
        self.response_text = response_text


class ResourceNotFoundError(PipelineError):
    """
    Resource not found errors.
    
    Raised when a required resource (file, endpoint, etc.) is not found.
    
    Example:
        >>> raise ResourceNotFoundError(
        ...     "Configuration file not found",
        ...     context={"path": "/path/to/config.yaml"}
        ... )
    """
    pass


class ValidationError(PipelineError):
    """
    Data validation errors.
    
    Raised when validation checks fail.
    
    Example:
        >>> raise ValidationError(
        ...     "ARI threshold must be positive",
        ...     context={"value": -1.0, "expected": "> 0"}
        ... )
    """
    pass


# =============================================================================
# Error Context Manager
# =============================================================================

@contextmanager
def error_context(
    operation: str,
    logger: Optional[logging.Logger] = None,
    context: Optional[Dict[str, Any]] = None,
    reraise: bool = True,
    error_class: Type[PipelineError] = PipelineError
):
    """
    Context manager for enhanced error handling.
    
    Automatically logs errors and adds context information.
    
    Args:
        operation: Description of operation being performed
        logger: Logger to use (creates one if not provided)
        context: Additional context to include in errors
        reraise: Whether to reraise exceptions
        error_class: Custom error class to wrap exceptions in
        
    Yields:
        Context for the operation
        
    Example:
        >>> with error_context("Loading configuration", logger):
        ...     config = load_config()
    """
    if logger is None:
        logger = logging.getLogger(__name__)
    
    context = context or {}
    
    try:
        yield
        
    except Exception as e:
        # Log the error
        logger.error(f"Error during '{operation}': {e}")
        
        if context:
            logger.error(f"Context: {context}")
        
        # Wrap in custom exception if requested
        if reraise:
            if isinstance(e, PipelineError):
                raise
            else:
                raise error_class(
                    f"Failed to {operation}: {e}",
                    context=context
                ) from e


# =============================================================================
# Safe Execution
# =============================================================================

def safe_execute(
    func: Callable[..., T],
    *args: Any,
    default: Optional[T] = None,
    logger: Optional[logging.Logger] = None,
    operation_name: Optional[str] = None,
    **kwargs: Any
) -> Optional[T]:
    """
    Execute function with automatic error handling.
    
    Returns default value on error instead of raising exception.
    Useful for non-critical operations.
    
    Args:
        func: Function to execute
        *args: Positional arguments for function
        default: Default value to return on error
        logger: Logger for error messages
        operation_name: Name of operation (for logging)
        **kwargs: Keyword arguments for function
        
    Returns:
        Function result or default value on error
        
    Example:
        >>> result = safe_execute(
        ...     risky_operation,
        ...     arg1, arg2,
        ...     default=None,
        ...     logger=logger,
        ...     operation_name="data processing"
        ... )
    """
    if logger is None:
        logger = logging.getLogger(__name__)
    
    if operation_name is None:
        operation_name = func.__name__
    
    try:
        return func(*args, **kwargs)
        
    except Exception as e:
        logger.warning(
            f"Error in {operation_name}: {e}. Using default value: {default}"
        )
        return default


# =============================================================================
# Retry Logic
# =============================================================================

def retry_on_failure(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
    logger: Optional[logging.Logger] = None
) -> Callable:
    """
    Decorator for automatic retry with exponential backoff.
    
    Args:
        max_attempts: Maximum number of attempts
        delay: Initial delay between retries (seconds)
        backoff: Backoff multiplier for each retry
        exceptions: Tuple of exceptions to catch and retry
        logger: Logger for retry messages
        
    Returns:
        Decorated function
        
    Example:
        >>> @retry_on_failure(max_attempts=3, delay=1.0)
        ... def fetch_data(url):
        ...     return requests.get(url).json()
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            nonlocal logger
            
            if logger is None:
                logger = logging.getLogger(func.__module__)
            
            current_delay = delay
            
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                    
                except exceptions as e:
                    if attempt == max_attempts:
                        logger.error(
                            f"{func.__name__} failed after {max_attempts} attempts: {e}"
                        )
                        raise
                    
                    logger.warning(
                        f"{func.__name__} failed (attempt {attempt}/{max_attempts}): {e}. "
                        f"Retrying in {current_delay:.1f}s..."
                    )
                    
                    time.sleep(current_delay)
                    current_delay *= backoff
            
            # Should never reach here
            raise RuntimeError(f"Unexpected retry logic error in {func.__name__}")
        
        return wrapper
    return decorator


# =============================================================================
# Error Logging
# =============================================================================

def log_error_details(
    error: Exception,
    logger: logging.Logger,
    context: Optional[Dict[str, Any]] = None,
    include_traceback: bool = True
) -> None:
    """
    Log detailed error information.
    
    Args:
        error: Exception to log
        logger: Logger to use
        context: Additional context information
        include_traceback: Whether to include full traceback
        
    Example:
        >>> try:
        ...     risky_operation()
        ... except Exception as e:
        ...     log_error_details(e, logger, context={"file": "data.csv"})
    """
    # Log basic error info
    logger.error(f"Error: {error}")
    logger.error(f"Error type: {type(error).__name__}")
    
    # Log context if provided
    if context:
        logger.error("Context:")
        for key, value in context.items():
            logger.error(f"  {key}: {value}")
    
    # Log custom error details if available
    if isinstance(error, PipelineError):
        details = error.get_details()
        logger.error(f"Error details: {details}")
    
    # Log traceback if requested
    if include_traceback:
        logger.error("Traceback:")
        logger.error(traceback.format_exc())


def create_error_report(
    error: Exception,
    context: Optional[Dict[str, Any]] = None,
    include_traceback: bool = True
) -> str:
    """
    Create formatted error report.
    
    Args:
        error: Exception to report
        context: Additional context information
        include_traceback: Whether to include full traceback
        
    Returns:
        Formatted error report string
        
    Example:
        >>> try:
        ...     risky_operation()
        ... except Exception as e:
        ...     report = create_error_report(e, context={"file": "data.csv"})
        ...     with open("error.txt", "w") as f:
        ...         f.write(report)
    """
    lines = []
    
    # Header
    lines.append("=" * 80)
    lines.append("ERROR REPORT")
    lines.append("=" * 80)
    lines.append(f"Timestamp: {datetime.now().isoformat()}")
    lines.append(f"Error Type: {type(error).__name__}")
    lines.append(f"Error Message: {error}")
    lines.append("")
    
    # Context
    if context:
        lines.append("Context:")
        for key, value in context.items():
            lines.append(f"  {key}: {value}")
        lines.append("")
    
    # Custom error details
    if isinstance(error, PipelineError):
        details = error.get_details()
        lines.append("Error Details:")
        for key, value in details.items():
            lines.append(f"  {key}: {value}")
        lines.append("")
    
    # Traceback
    if include_traceback:
        lines.append("Traceback:")
        lines.append(traceback.format_exc())
    
    # Footer
    lines.append("=" * 80)
    
    return "\\n".join(lines)


# =============================================================================
# Error Recovery
# =============================================================================

def save_error_report(
    error: Exception,
    output_path: Path,
    context: Optional[Dict[str, Any]] = None
) -> None:
    """
    Save error report to file.
    
    Args:
        error: Exception to report
        output_path: Path to save report
        context: Additional context information
        
    Example:
        >>> try:
        ...     risky_operation()
        ... except Exception as e:
        ...     save_error_report(e, Path("errors/error_report.txt"))
    """
    report = create_error_report(error, context, include_traceback=True)
    
    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Write report
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(report)


# =============================================================================
# Exception Chain Analysis
# =============================================================================

def get_exception_chain(error: Exception) -> List[Exception]:
    """
    Get full exception chain from an error.
    
    Args:
        error: Exception to analyze
        
    Returns:
        List of exceptions in the chain (from root to current)
        
    Example:
        >>> try:
        ...     nested_operation()
        ... except Exception as e:
        ...     chain = get_exception_chain(e)
        ...     for exc in chain:
        ...         print(f"  -> {type(exc).__name__}: {exc}")
    """
    chain = []
    current = error
    
    while current is not None:
        chain.append(current)
        current = current.__cause__ or current.__context__
    
    return chain


def get_root_cause(error: Exception) -> Exception:
    """
    Get root cause of an exception chain.
    
    Args:
        error: Exception to analyze
        
    Returns:
        Root cause exception
        
    Example:
        >>> try:
        ...     nested_operation()
        ... except Exception as e:
        ...     root = get_root_cause(e)
        ...     print(f"Root cause: {root}")
    """
    chain = get_exception_chain(error)
    return chain[-1] if chain else error
