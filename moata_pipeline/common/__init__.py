"""
Common utilities shared across the moata_pipeline package.

Keep this package dependency-light and free of domain/business logic.
"""

# Explicit re-exports (avoid `import *` to keep static analysis accurate)

from .constants import (
    TOKEN_URL,
    BASE_API_URL,
    OAUTH_SCOPE,
    DEFAULT_PROJECT_ID,
    DEFAULT_RAIN_GAUGE_ASSET_TYPE_ID,
    DEFAULT_REQUESTS_PER_SECOND,
    DEFAULT_TIMEOUT_SECONDS,
    TOKEN_TTL_SECONDS,
    TOKEN_REFRESH_BUFFER_SECONDS,
    INACTIVE_THRESHOLD_MONTHS,
    DEFAULT_EXCLUDE_KEYWORD,
    DEFAULT_RADAR_PROPORTION_THRESHOLD,
)

from .paths import PipelinePaths

from .json_io import (
    read_json_maybe_wrapped,
    write_json,
)

from .text_utils import safe_filename

from .time_utils import (
    months_ago,
    now_like,
    parse_datetime,
    format_date_for_display,
)

from .file_utils import ensure_dir

from .html_utils import df_to_html_table

# Keep typing_utils optional: only export if actually used externally
from .typing_utils import (
    JsonList,
    GaugeEntry,
)

# Protocols for dependency injection and testing
from .protocols import (
    HTTPClientProtocol,
    AuthProviderProtocol,
    DataCollectorProtocol,
    DataAnalyzerProtocol,
    ReportGeneratorProtocol,
    CacheManagerProtocol,
    ValidationProtocol,
    OutputWriterProtocol,
)

# Unified exception hierarchy
from .exceptions import (
    PipelineError,
    CollectionError,
    APIError,
    RateLimitError,
    AuthenticationError,
    GeometryError,
    AnalysisError,
    InputDataError,
    CalculationError,
    ValidationError,
    PathValidationError,
    DataValidationError,
    StorageError,
    CacheError,
    OutputError,
    VisualizationError,
    ChartGenerationError,
    DashboardError,
)

# Validation utilities
from .validation import (
    validate_path_exists,
    validate_positive_number,
    validate_proportion,
    validate_ari_threshold,
    validate_date_string,
    validate_log_level,
    validate_required_env_vars,
    validate_dataframe_not_empty,
    validate_dataframe_columns,
    validate_dataframe,
    ValidationResult,
)

# ========== NEW: Spatial utilities for pixel weighting ==========
from .spatial_utils import (
    calculate_pixel_overlap_weights,
    estimate_pixel_area_weights_simple,
    save_pixel_weights,
    load_pixel_weights,
)
# =================================================================

__all__ = [
    # constants
    "TOKEN_URL",
    "BASE_API_URL",
    "OAUTH_SCOPE",
    "DEFAULT_PROJECT_ID",
    "DEFAULT_RAIN_GAUGE_ASSET_TYPE_ID",
    "DEFAULT_REQUESTS_PER_SECOND",
    "DEFAULT_TIMEOUT_SECONDS",
    "TOKEN_TTL_SECONDS",
    "TOKEN_REFRESH_BUFFER_SECONDS",
    "INACTIVE_THRESHOLD_MONTHS",
    "DEFAULT_EXCLUDE_KEYWORD",
    # paths
    "PipelinePaths",
    # json io
    "read_json_maybe_wrapped",
    "write_json",
    # text
    "safe_filename",
    # time
    "months_ago",
    "now_like",
    "parse_datetime",
    "format_date_for_display",
    # file utils
    "ensure_dir",
    # html
    "df_to_html_table",
    # typing (optional)
    "JsonList",
    "GaugeEntry",
    # validation
    "validate_path_exists",
    "validate_positive_number",
    "validate_proportion",
    "validate_ari_threshold",
    "validate_date_string",
    "validate_log_level",
    "validate_required_env_vars",
    "validate_dataframe_not_empty",
    "validate_dataframe_columns",
    "validate_dataframe",
    "ValidationResult",
    # protocols
    "HTTPClientProtocol",
    "AuthProviderProtocol",
    "DataCollectorProtocol",
    "DataAnalyzerProtocol",
    "ReportGeneratorProtocol",
    "CacheManagerProtocol",
    "ValidationProtocol",
    "OutputWriterProtocol",
    # exceptions
    "PipelineError",
    "CollectionError",
    "APIError",
    "RateLimitError",
    "AuthenticationError",
    "GeometryError",
    "AnalysisError",
    "InputDataError",
    "CalculationError",
    "ValidationError",
    "PathValidationError",
    "DataValidationError",
    "StorageError",
    "CacheError",
    "OutputError",
    "VisualizationError",
    "ChartGenerationError",
    "DashboardError",
    # ========== NEW: Spatial utilities ==========
    "calculate_pixel_overlap_weights",
    "estimate_pixel_area_weights_simple",
    "save_pixel_weights",
    "load_pixel_weights",
    # ============================================
]