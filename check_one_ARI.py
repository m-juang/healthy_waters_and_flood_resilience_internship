from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path.cwd() / ".env")

from moata_pipeline.common.constants import (
    TOKEN_URL,
    BASE_API_URL,
    OAUTH_SCOPE,
    TOKEN_TTL_SECONDS,
    TOKEN_REFRESH_BUFFER_SECONDS,
    DEFAULT_REQUESTS_PER_SECOND,
)
from moata_pipeline.moata.auth import MoataAuth
from moata_pipeline.moata.http import MoataHttp
from moata_pipeline.moata.client import MoataClient


print("START check_one_ARI_raw")

# =====================
# KONFIG 1 CASE
# =====================
ASSET_ID = 3160974
ALARM_TIME_UTC = "2025-05-09 05:05:00"

# Window ±24 jam
WINDOW_HOURS = 24

# =====================
# SETUP CLIENT (SAMA DENGAN PIPELINE)
# =====================
client_id = os.getenv("MOATA_CLIENT_ID")
client_secret = os.getenv("MOATA_CLIENT_SECRET")

auth = MoataAuth(
    token_url=TOKEN_URL,
    scope=OAUTH_SCOPE,
    client_id=client_id,
    client_secret=client_secret,
    verify_ssl=False,
    ttl_seconds=TOKEN_TTL_SECONDS,
    refresh_buffer_seconds=TOKEN_REFRESH_BUFFER_SECONDS,
)

http = MoataHttp(
    get_token_fn=auth.get_token,
    base_url=BASE_API_URL,
    requests_per_second=DEFAULT_REQUESTS_PER_SECOND,
    verify_ssl=False,
)

client = MoataClient(http=http)
print("Client ready")

# =====================
# STEP 1: AMBIL TRACE RAINFALL (RAW)
# =====================
traces = client.get_traces_for_asset(ASSET_ID)
print("Available rainfall-like traces:")
for t in traces:
    desc = (t.get("description") or "").lower()
    if "rain" in desc:
        print(f"  id={t.get('id')} desc={t.get('description')}")


rain_trace = next(
    (t for t in traces if (t.get("description") or "").strip().lower() == "raw rain"),
    None,
)


if not rain_trace:
    print("UNVERIFIABLE: Rainfall trace not found")
    raise SystemExit

trace_id = rain_trace["id"]
print(f"Using Rainfall trace_id = {trace_id}")

# =====================
# STEP 2: WINDOW WAKTU
# =====================
center = datetime.fromisoformat(ALARM_TIME_UTC).replace(tzinfo=timezone.utc)
from_time = (center - timedelta(hours=WINDOW_HOURS)).isoformat().replace("+00:00", "Z")
to_time = (center + timedelta(hours=WINDOW_HOURS)).isoformat().replace("+00:00", "Z")

print(f"Time window: {from_time} → {to_time}")

# =====================
# STEP 3: AMBIL RAW DATA
# =====================
print("Fetching RAW rainfall data...")

data = http.get(
    f"traces/{trace_id}/data",
    params={
        "from": from_time,
        "to": to_time,
    },
    allow_404=True,
)


if not data:
    print("UNVERIFIABLE: No rainfall data returned")
    raise SystemExit

items = data if isinstance(data, list) else data.get("items", [])
values = [i.get("value") for i in items if i.get("value") is not None]

if not values:
    print("UNVERIFIABLE: Empty rainfall values")
    raise SystemExit

# =====================
# STEP 4: ANALISIS SEDERHANA
# =====================
max_rain = max(values)
total_rain = sum(values)

print(f"Max rainfall (5-min): {max_rain:.2f} mm")
print(f"Total rainfall (window): {total_rain:.2f} mm")

# =====================
# STEP 5: INTERPRETASI
# =====================
if total_rain == 0:
    print("RESULT: NOT_SUPPORTED (tidak ada hujan signifikan)")
else:
    print("RESULT: SUPPORTED (ada hujan signifikan di sekitar alarm)")

print("END check_one_ARI_raw")
