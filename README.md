# Healthy Waters and Flood Resilience Internship

A Python project for collecting, filtering, and analyzing rain gauge data from the Moata API to support healthy waters and flood resilience initiatives in Auckland.

## Overview

This project interfaces with the Moata Analytical Engine API to:
1. **Fetch rain gauge assets** from the Auckland Rainfall project
2. **Collect trace (time series) data** for each gauge
3. **Identify and filter active gauges** (within the last 3 months of data)
4. **Analyze alarm configurations** (overflow and recency alarms)
5. **Generate summary reports** of active Auckland rain gauges

### Key Features

- **Sequential API processing** - Respects rate limits (800 requests/5 minutes)
- **Secure credential management** - Uses environment variables via `.env` file
- **Alarm monitoring** - Tracks both overflow (threshold) and recency (data staleness) alarms
- **Active gauge filtering** - Identifies gauges with recent data
- **Comprehensive reporting** - Generates CSV and JSON summaries

## Project Structure

```
.
├── moata_data_retriever.py          # Main script: fetches data from Moata API
├── filter_active_rain_gauges.py     # Analysis script: filters and summarizes data
├── debug_alarms.py                  # Debug utility: inspects alarm data structures
├── .env.example                     # Template for environment variables
├── .gitignore                       # Git configuration (excludes .env and build artifacts)
├── moata_output/                    # Output directory (raw API data)
│   ├── rain_gauges.json            # All 264 rain gauges
│   ├── rain_gauges_traces_alarms.json  # Detailed data: gauges + traces + alarms
│   └── rain_gauges.json            # Rainfall traces (efficient endpoint)
└── moata_filtered/                 # Output directory (analyzed/filtered data)
    ├── active_auckland_gauges.json # Active Auckland gauges only
    ├── alarm_summary.csv           # Alarm configuration summary
    ├── alarm_summary.json          # Alarm summary (JSON format)
    └── analysis_report.txt         # Human-readable analysis report
```

## Installation

### Prerequisites

- Python 3.8+
- pip or conda package manager
- Moata API credentials (client ID and secret)

### Setup

1. **Clone the repository:**
   ```bash
   git clone https://github.com/m-juang/healthy_waters_and_flood_resilience_internship.git
   cd healthy_waters_and_flood_resilience_internship
   ```

2. **Create a virtual environment:**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\Activate.ps1
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```
   
   Or install manually:
   ```bash
   pip install requests pandas python-dateutil python-dotenv urllib3
   ```

4. **Configure credentials:**
   - Copy `.env.example` to `.env`
   - Add your Moata API credentials:
     ```
     MOATA_CLIENT_ID=your_client_id_here
     MOATA_CLIENT_SECRET=your_client_secret_here
     ```
   - **Important:** Never commit `.env` to version control (it's in `.gitignore`)

## Usage

### Step 1: Fetch Data from Moata API

```bash
python moata_data_retriever.py
```

**What it does:**
- Obtains an OAuth2 access token (valid for 1 hour)
- Fetches all 264 rain gauge assets from project 594
- For each gauge, collects its traces (time series data)
- Attempts to fetch overflow alarms for each trace
- Saves raw data to `moata_output/` directory

**Output files:**
- `moata_output/rain_gauges.json` - All assets
- `moata_output/rain_gauges_traces_alarms.json` - Complete data structure

**Execution time:** ~5-10 minutes (sequential processing, respects rate limits)

### Step 2: Filter and Analyze Data

```bash
python filter_active_rain_gauges.py
```

**What it does:**
- Filters out Northland gauges (keeps only Auckland)
- Identifies active gauges (with data from the last 3 months)
- Extracts the primary 'Rainfall' trace for each gauge
- Analyzes alarm configurations
- Generates summary tables and reports

**Output files:**
- `moata_filtered/active_auckland_gauges.json` - Filtered gauge list
- `moata_filtered/alarm_summary.csv` - Alarm data in table format
- `moata_filtered/alarm_summary.json` - Alarm data (JSON)
- `moata_filtered/analysis_report.txt` - Human-readable summary

**Execution time:** <1 minute

### Step 3: Review Results

Check the generated reports:
```bash
cat moata_filtered/analysis_report.txt
```

## Data Model

### Rain Gauge (Asset)
```json
{
  "id": 12345,
  "name": "Awhitu",
  "assetTypeId": 100,
  "description": "Rain gauge in Awhitu..."
}
```

### Trace (Time Series)
```json
{
  "id": 1482302,
  "assetId": 12345,
  "description": "Rainfall 60 min window sum - Mirror",
  "resolution": 3600,
  "hasAlarms": true,
  "telemeteredMaximumTime": "2025-12-10T08:30:00Z"
}
```

### Alarms

Two types of alarms are tracked:

1. **Overflow Alarms** (OverflowMonitoring)
   - Triggered when trace value exceeds a configured threshold
   - Fields: `threshold`, `severity`, `description`
   - Endpoint: `/alarms/overflow/detailed-info-by-trace`

2. **Recency Alarms** (DataRecency)
   - Triggered when data hasn't been updated for X amount of time
   - Indicated by `trace.hasAlarms = true`
   - Configuration: `maxLookbackOverride` (staleness window)
   - Used for the main 'Rainfall' traces

## API Information

### Base URL
```
https://api.moata.io/ae/v1
```

### Authentication
- **Type:** OAuth2 Client Credentials
- **Token URL:** `https://moata.b2clogin.com/moata.onmicrosoft.com/B2C_1A_CLIENTCREDENTIALSFLOW/oauth2/v2.0/token`
- **Token Validity:** 1 hour
- **Rate Limit:** 800 requests per 5 minutes

### Key Endpoints Used

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/projects/{projectId}/assets` | GET | Fetch rain gauge assets |
| `/assets/traces` | GET | Get traces for a gauge |
| `/alarms/overflow/detailed-info-by-trace` | GET | Fetch overflow alarms |
| `/projects/{projectId}/traces_info` | GET | Efficient bulk trace fetching |

## Filtering Criteria

### Active Gauges

A gauge is considered **active** if:
- ✓ Located in Auckland (name doesn't contain "Northland")
- ✓ Has a 'Rainfall' trace
- ✓ `telemeteredMaximumTime` is within the last 3 months

### Inactive Gauges

Excluded from analysis:
- Northland gauges
- Gauges without a 'Rainfall' trace
- Gauges with no data in the last 3 months

## Results Summary

### Typical Output

```
Total gauges in dataset: 264
✓ Active Auckland gauges: ~180
✗ Inactive gauges: ~70
✗ Northland gauges: ~14
✗ No Rainfall trace: ~0

Active gauge traces: ~4,400+
Traces with alarms: ~1,070+
Overflow alarms: ~50+
```

(Exact numbers vary based on real-time data availability)

## Alarm Fields

The `alarm_summary.csv` includes:

| Column | Description | Values |
|--------|-------------|--------|
| `gauge_id` | Asset ID | Integer |
| `gauge_name` | Gauge name | String |
| `last_data` | Most recent data timestamp | Date (YYYY-MM-DD) |
| `trace_id` | Trace ID | Integer |
| `trace_name` | Trace description | String |
| `alarm_id` | Alarm ID | Integer or null |
| `alarm_name` | Alarm description | String |
| `alarm_type` | Alarm type | `OverflowMonitoring` or `DataRecency` |
| `threshold` | Threshold value | Number or null |
| `severity` | Alarm severity | Integer or null |
| `enabled` | Is alarm active | Boolean or null |

## Error Handling

### Common Issues

**1. `RuntimeError: MOATA_CLIENT_ID and MOATA_CLIENT_SECRET must be set`**
- Solution: Ensure `.env` file exists in the project root with valid credentials
- Check that credentials don't have a UTF-8 BOM prefix

**2. `requests.exceptions.HTTPError: 401 Client Error: Unauthorized`**
- Solution: Verify credentials are correct
- Check if token has expired (obtain a new one by running the script again)

**3. `FileNotFoundError: moata_output/rain_gauges_traces_alarms.json`**
- Solution: Run `moata_data_retriever.py` first to fetch raw data

**4. `ImportError: No module named 'pandas'`**
- Solution: Install missing packages: `pip install -r requirements.txt`

## Development

### Running Tests/Debug

```bash
# Inspect raw API data structure
python debug_alarms.py

# Check what alarms are in the dataset
python -c "import json; data = json.load(open('moata_output/rain_gauges_traces_alarms.json')); print(f'Traces with alarms: {sum(1 for g in data for t in g[\"traces\"] if t[\"trace\"].get(\"hasAlarms\"))}')"
```

### Modifying Filtering Logic

Edit the filtering parameters in `filter_active_rain_gauges.py`:

```python
# Change inactive threshold (default: 3 months)
INACTIVE_THRESHOLD_MONTHS = 3

# Change filter logic
def is_auckland_gauge(gauge_name: str) -> bool:
    return "northland" not in gauge_name.lower()
```

## GitHub Repository

Project is hosted at: https://github.com/m-juang/healthy_waters_and_flood_resilience_internship

### Git Workflow

```bash
# Check current branch
git branch

# View recent commits
git log --oneline -10

# Push changes
git add .
git commit -m "Description of changes"
git push origin main
```

**Important:** Always use `git push` from within the project directory to ensure correct authentication.

## Requirements

- **Python 3.8+**
- **requests** (2.32.5+) - HTTP client
- **pandas** (2.3.3+) - Data analysis
- **python-dateutil** (2.9.0+) - Date parsing
- **python-dotenv** (1.2.1+) - Environment variable loading
- **urllib3** (2.6.1+) - HTTP utilities

See `requirements.txt` for full dependency list.

## License

This project is part of the Auckland Council's Healthy Waters and Flood Resilience initiative.

## Contact

For questions about this project, please contact the internship supervisor or refer to the documentation in the GitHub repository.

## Changelog

### v1.0.0 (2025-12-11)

**Features:**
- Initial implementation of rain gauge data collection
- OAuth2 authentication with Moata API
- Active gauge filtering (Auckland + 3-month data threshold)
- Alarm configuration analysis
- CSV and JSON report generation
- Comprehensive error handling

**Key Components:**
- `moata_data_retriever.py` - Data collection (264 gauges, 4,400+ traces)
- `filter_active_rain_gauges.py` - Analysis and filtering
- Environment variable support for secure credential management
- Sequential API processing respecting rate limits

**Known Limitations:**
- Detailed alarm endpoint (`/alarms/detailed-alarms`) may require elevated permissions
- Recency alarm thresholds must be inferred from `hasAlarms` flag (detailed config not always accessible)

---

**Last Updated:** December 11, 2025
