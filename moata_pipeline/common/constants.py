from __future__ import annotations

# =========================
# Moata endpoints
# =========================

TOKEN_URL = (
    "https://moata.b2clogin.com/"
    "moata.onmicrosoft.com/B2C_1A_CLIENTCREDENTIALSFLOW/oauth2/v2.0/token"
)

BASE_API_URL = "https://api.moata.io/ae/v1"
OAUTH_SCOPE = "https://moata.onmicrosoft.com/moata.io/.default"

# =========================
# Defaults (safe values)
# =========================

DEFAULT_PROJECT_ID = 594
DEFAULT_RAIN_GAUGE_ASSET_TYPE_ID = 100

# Rate limit safety: 800 requests / 5 minutes (Sam)
DEFAULT_REQUESTS_PER_SECOND = 2.0
DEFAULT_TIMEOUT_SECONDS = 60

# Token refresh behaviour
TOKEN_TTL_SECONDS = 3600
TOKEN_REFRESH_BUFFER_SECONDS = 300

# Filtering defaults
INACTIVE_THRESHOLD_MONTHS = 3

DEFAULT_EXCLUDE_KEYWORD = "northland|waikato"

