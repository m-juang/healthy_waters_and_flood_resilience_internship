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
]
