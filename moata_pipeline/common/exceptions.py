"""
Unified exception hierarchy for the pipeline.

All custom exceptions inherit from PipelineError, making it easy to:
- Catch all pipeline-related errors
- Log consistently
- Handle errors at appropriate levels

Exception Hierarchy:
    PipelineError (base)
    ├── CollectionError
    │   ├── APIError
    │   │   ├── RateLimitError
    │   │   └── AuthenticationError
    │   └── GeometryError
    ├── AnalysisError
    │   ├── InputDataError
    │   └── CalculationError
    ├── ValidationError
    │   ├── PathValidationError
    │   └── DataValidationError
    ├── StorageError
    │   ├── CacheError
    │   └── OutputError
    └── VisualizationError
        ├── ChartGenerationError
        └── DashboardError

Example:
    >>> from moata_pipeline.common.exceptions import PipelineError, CollectionError
    >>> 
    >>> try:
    ...     collect_data()
    ... except CollectionError as e:
    ...     logger.error(f"Collection failed: {e}")
    ...     logger.debug(f"Context: {e.context}")
    ... except PipelineError as e:
    ...     logger.error(f"Pipeline error: {e}")
"""


class PipelineError(Exception):
    """
    Base exception for all pipeline errors.
    
    All custom exceptions in the pipeline should inherit from this class.
    This allows catching all pipeline-related errors with a single except clause.
    
    Attributes:
        message: Human-readable error message
        context: Additional context dict (optional)
    
    Example:
        >>> raise PipelineError(
        ...     "Failed to process data",
        ...     context={"file": "data.csv", "line": 42}
        ... )
    """
    
    def __init__(self, message: str, context: dict = None):
        """
        Initialize pipeline error.
        
        Args:
            message: Human-readable error message
            context: Additional context information (optional)
        """
        super().__init__(message)
        self.message = message
        self.context = context or {}
    
    def __str__(self) -> str:
        """Return formatted error message with context."""
        if self.context:
            context_str = ", ".join(f"{k}={v}" for k, v in self.context.items())
            return f"{self.message} [{context_str}]"
        return self.message
    
    def __repr__(self) -> str:
        """Return detailed error representation."""
        return f"{self.__class__.__name__}({self.message!r}, context={self.context!r})"


# ============================================================================
# Data Collection Errors
# ============================================================================

class CollectionError(PipelineError):
    """
    Base exception for data collection errors.
    
    Raised when data collection from external sources fails.
    Examples: API failures, network errors, data format issues.
    """
    pass


class APIError(CollectionError):
    """
    Raised when API request fails.
    
    This includes HTTP errors, timeout errors, and invalid responses
    from the Moata API or other external APIs.
    
    Example:
        >>> raise APIError(
        ...     "Failed to fetch assets",
        ...     context={"status_code": 500, "endpoint": "/assets"}
        ... )
    """
    pass


class RateLimitError(APIError):
    """
    Raised when API rate limit is exceeded.
    
    Should include retry_after in context if available.
    
    Example:
        >>> raise RateLimitError(
        ...     "API rate limit exceeded",
        ...     context={"retry_after": 60}
        ... )
    """
    pass


class AuthenticationError(APIError):
    """
    Raised when authentication fails.
    
    This includes invalid credentials, expired tokens, or
    permission denied errors.
    """
    pass


class GeometryError(CollectionError):
    """
    Raised when geometry processing fails.
    
    This includes invalid GeoJSON, coordinate transformation errors,
    or spatial operations that fail.
    
    Example:
        >>> raise GeometryError(
        ...     "Invalid polygon geometry",
        ...     context={"asset_id": 123, "reason": "self-intersecting"}
        ... )
    """
    pass


# ============================================================================
# Data Analysis Errors
# ============================================================================

class AnalysisError(PipelineError):
    """
    Base exception for data analysis errors.
    
    Raised when data processing or analysis operations fail.
    """
    pass


class InputDataError(AnalysisError):
    """
    Raised when input data is invalid or missing.
    
    This includes missing required columns, wrong data types,
    or empty datasets.
    
    Example:
        >>> raise InputDataError(
        ...     "Missing required column",
        ...     context={"column": "rainfall", "file": "data.csv"}
        ... )
    """
    pass


class CalculationError(AnalysisError):
    """
    Raised when calculation/computation fails.
    
    This includes mathematical errors, division by zero,
    or numerical instability.
    
    Example:
        >>> raise CalculationError(
        ...     "ARI calculation failed",
        ...     context={"gauge_id": 123, "reason": "no valid data points"}
        ... )
    """
    pass


# ============================================================================
# Validation Errors
# ============================================================================

class ValidationError(PipelineError):
    """
    Base exception for validation errors.
    
    Raised when input validation fails (user input, configuration, etc.).
    """
    pass


class PathValidationError(ValidationError):
    """
    Raised when path validation fails.
    
    This includes non-existent files, invalid directories,
    or permission errors.
    
    Example:
        >>> raise PathValidationError(
        ...     "Input file not found",
        ...     context={"path": "/data/input.csv"}
        ... )
    """
    pass


class DataValidationError(ValidationError):
    """
    Raised when data validation fails.
    
    This includes value out of range, invalid format,
    or constraint violations.
    
    Example:
        >>> raise DataValidationError(
        ...     "Invalid date format",
        ...     context={"value": "2024-13-45", "expected": "YYYY-MM-DD"}
        ... )
    """
    pass


# ============================================================================
# Storage Errors
# ============================================================================

class StorageError(PipelineError):
    """
    Base exception for storage/IO errors.
    
    Raised when file operations or storage access fails.
    """
    pass


class CacheError(StorageError):
    """
    Raised when cache operations fail.
    
    This includes cache read/write errors, serialization failures,
    or cache corruption.
    
    Example:
        >>> raise CacheError(
        ...     "Failed to write cache",
        ...     context={"cache_key": "assets_594", "error": "disk full"}
        ... )
    """
    pass


class OutputError(StorageError):
    """
    Raised when output generation fails.
    
    This includes CSV writing errors, JSON serialization failures,
    or file permission issues.
    
    Example:
        >>> raise OutputError(
        ...     "Failed to write output CSV",
        ...     context={"file": "output.csv", "error": "permission denied"}
        ... )
    """
    pass


# ============================================================================
# Visualization Errors
# ============================================================================

class VisualizationError(PipelineError):
    """
    Base exception for visualization errors.
    
    Raised when chart or dashboard generation fails.
    """
    pass


class ChartGenerationError(VisualizationError):
    """
    Raised when chart generation fails.
    
    This includes matplotlib errors, plotly failures,
    or invalid chart data.
    
    Example:
        >>> raise ChartGenerationError(
        ...     "Failed to generate histogram",
        ...     context={"chart_type": "histogram", "data_length": 0}
        ... )
    """
    pass


class DashboardError(VisualizationError):
    """
    Raised when dashboard generation fails.
    
    This includes HTML generation errors, template rendering failures,
    or missing chart components.
    
    Example:
        >>> raise DashboardError(
        ...     "Failed to render dashboard",
        ...     context={"template": "validation.html", "missing_data": ["charts"]}
        ... )
    """
    pass
