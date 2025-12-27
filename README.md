# Auckland Council Rain Monitoring System

A comprehensive Python pipeline for collecting, analyzing, and visualizing Auckland Council's rain monitoring data from the Moata API, including rain gauges and rain radar (QPE) data.

> **Project Type**: Internal Auckland Council internship project (COMPSCI 778)  
> **Focus Area**: Healthy Waters and Flood Resilience  
> **Status**: Active Development

---

## 📋 Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Project Structure](#project-structure)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
  - [Rain Gauge Pipeline](#rain-gauge-pipeline)
  - [Rain Radar Pipeline](#rain-radar-pipeline)
  - [Documentation Generation](#documentation-generation)
- [Key Concepts](#key-concepts)
- [Data Management](#data-management)
- [API Configuration](#api-configuration)
- [Performance & Limitations](#performance--limitations)
- [Troubleshooting](#troubleshooting)
- [FAQ](#faq)
- [Testing](#testing)
- [Dependencies](#dependencies)
- [Contributing](#contributing)
- [Support](#support)
- [License](#license)

---

## Overview

This project provides an end-to-end data pipeline for Auckland Council's rainfall monitoring infrastructure. It automates the collection of rainfall data from both point-based gauges and spatial radar systems, performs sophisticated ARI (Annual Recurrence Interval) analysis, validates alarm configurations, and generates interactive HTML dashboards for operational insights.

### What This System Does

- **Collects** rain gauge and radar data from Moata API with OAuth2 authentication
- **Analyzes** alarm configurations and calculates ARI using TP108 methodology
- **Validates** ARI alarms against historical and real-time data
- **Visualizes** results with interactive HTML dashboards
- **Generates** comprehensive Word documentation for reporting

### Use Cases

1. **Operational Monitoring**: Track real-time rainfall across Auckland's monitoring network
2. **Alarm Validation**: Verify that configured alarms match actual rainfall patterns
3. **Flood Risk Assessment**: Identify areas experiencing rare rainfall events
4. **Historical Analysis**: Analyze past rainfall events for infrastructure planning
5. **Reporting**: Generate documentation for stakeholders and decision-makers

---

## Features

### Core Capabilities

✅ **Automated Data Collection**
- OAuth2-authenticated API access with automatic token refresh
- Rate-limited requests (respects API constraints)
- Batch processing for large datasets
- Error recovery and retry logic

✅ **Advanced Analysis**
- ARI calculation using TP108 coefficients (8 durations: 10m - 24h)
- Spatial analysis for radar data (catchment-level aggregation)
- Alarm threshold validation
- Statistical filtering and data quality checks

✅ **Rich Visualizations**
- Interactive HTML dashboards with embedded charts
- Per-gauge detailed analysis pages
- Temporal rainfall patterns
- ARI exceedance heatmaps
- Validation comparison reports

✅ **Data Quality**
- Automatic outlier detection
- Missing data handling
- Timestamp validation
- Coordinate verification for spatial data

---

## Project Structure

```
internship-project/
│
├── 📁 data/                               # Input reference data
│   └── inputs/
│       ├── raingauge_ari_alarms.csv       # Historical alarm events (from Sam)
│       └── tp108_stats.csv                # TP108 ARI coefficients per pixel
│
├── 📁 moata_pipeline/                     # Main package (all source code)
│   ├── __init__.py
│   ├── logging_setup.py                   # Centralized logging configuration
│   │
│   ├── 📁 analyze/                        # Analysis modules
│   │   ├── __init__.py
│   │   ├── alarm_analysis.py              # Rain gauge alarm analysis
│   │   ├── ari_calculator.py              # ARI calculation from radar data
│   │   ├── filtering.py                   # Gauge filtering logic
│   │   ├── radar_analysis.py              # Radar ARI batch processing
│   │   ├── reporting.py                   # Report generation
│   │   └── runner.py                      # Analysis entry points
│   │
│   ├── 📁 collect/                        # Data collection
│   │   ├── __init__.py
│   │   ├── collector.py                   # RainGaugeCollector, RadarDataCollector
│   │   └── runner.py                      # Collection entry points
│   │
│   ├── 📁 common/                         # Shared utilities
│   │   ├── __init__.py
│   │   ├── constants.py                   # API URLs, project IDs
│   │   ├── dataframe_utils.py             # Pandas helper functions
│   │   ├── file_utils.py                  # File I/O operations
│   │   ├── html_utils.py                  # HTML generation utilities
│   │   ├── iter_utils.py                  # Iterator tools (chunk() function)
│   │   ├── json_io.py                     # JSON read/write with error handling
│   │   ├── output_writer.py               # Centralized output management
│   │   ├── paths.py                       # Output path management
│   │   ├── text_utils.py                  # String utilities (safe_filename())
│   │   ├── time_utils.py                  # Datetime utilities (iso_z(), parse_datetime())
│   │   └── typing_utils.py                # Type conversion (safe_int(), safe_float())
│   │
│   ├── 📁 moata/                          # Moata API client
│   │   ├── __init__.py
│   │   ├── auth.py                        # OAuth2 authentication & token management
│   │   ├── client.py                      # High-level API methods
│   │   ├── endpoints.py                   # API endpoint definitions
│   │   └── http.py                        # HTTP client with rate limiting
│   │
│   └── 📁 viz/                            # Visualization
│       ├── __init__.py
│       ├── cleaning.py                    # Rain gauge data cleaning
│       ├── pages.py                       # Per-gauge HTML pages
│       ├── radar_cleaning.py              # Radar data cleaning
│       ├── radar_report.py                # Radar HTML dashboard
│       ├── radar_runner.py                # Radar visualization runner
│       ├── report.py                      # Rain gauge HTML report
│       └── runner.py                      # Rain gauge visualization runner
│
├── 📁 outputs/                            # Generated outputs (Git-ignored, see .gitignore)
│   ├── 📁 documentation/
│   │   └── Rain_Monitoring_System_Documentation.docx
│   │
│   ├── 📁 rain_gauges/                    # ~50-200MB per collection
│   │   ├── raw/                           # Raw API JSON responses
│   │   ├── analyze/                       # Analysis results (CSV, JSON)
│   │   ├── validation_viz/                # Validation visualizations (HTML)
│   │   ├── visualizations/                # Dashboard visualizations (HTML)
│   │   └── ari_alarm_validation.csv       # Validation results summary
│   │
│   └── 📁 rain_radar/                     # ~1-5GB per historical date
│       ├── raw/                           # Current (last 24h) data
│       │   ├── catchments/                # Catchment boundary GeoJSON
│       │   ├── pixel_mappings/            # Pixel-to-catchment mappings
│       │   ├── radar_data/                # Raw radar timeseries
│       │   └── collection_summary.json    # Collection metadata
│       ├── analyze/                       # Current data analysis
│       ├── historical/                    # Historical data organized by date
│       │   └── YYYY-MM-DD/                # e.g., 2025-05-09/
│       │       ├── raw/
│       │       ├── analyze/
│       │       ├── dashboard/
│       │       ├── validation_viz/
│       │       └── ari_alarm_validation.csv
│       └── visualizations/
│
├── 📄 Configuration & Documentation
├── .env                                   # API credentials (Git-ignored, REQUIRED)
├── .env.example                           # Template for .env
├── .gitignore                             # Excludes outputs/, .env, __pycache__
├── .gitattributes                         # Git LFS configuration (for large files)
├── README.md                              # This file
├── requirements.txt                       # Python dependencies
│
├── 🚀 Entry Points - Rain Gauges
├── retrieve_rain_gauges.py                # 1. Collect rain gauge data
├── analyze_rain_gauges.py                 # 2. Analyze and filter gauges
├── visualize_rain_gauges.py               # 3. Generate gauge dashboard
├── validate_ari_alarms_rain_gauges.py     # 4. Validate gauge alarms
├── visualize_ari_alarms_rain_gauges.py    # 5. Visualize gauge validation
│
├── 🚀 Entry Points - Rain Radar
├── retrieve_rain_radar.py                 # 1. Collect radar data
├── analyze_rain_radar.py                  # 2. Calculate ARI from radar
├── visualize_rain_radar.py                # 3. Generate radar dashboard
├── validate_ari_alarms_rain_radar.py      # 4. Validate radar alarms
├── visualize_ari_alarms_rain_radar.py     # 5. Visualize radar validation
│
└── generate_documentation.py              # Generate Word documentation
```

### Directory Size Expectations

| Directory | Typical Size | Notes |
|-----------|--------------|-------|
| `outputs/rain_gauges/raw/` | 50-200 MB | ~200 gauges × 30 days |
| `outputs/rain_radar/raw/` | 500 MB - 1 GB | Last 24h, all catchments |
| `outputs/rain_radar/historical/YYYY-MM-DD/` | 1-5 GB | Full day, ~200 catchments |
| Total `outputs/` | 2-10 GB | After several collections |

---

## Installation

### Prerequisites

- **Python**: 3.10 or higher
- **Operating System**: Windows, macOS, or Linux
- **Memory**: Minimum 4GB RAM (8GB recommended for radar processing)
- **Storage**: 10GB free space for outputs
- **Network**: Access to Moata API (internal Auckland Council network or VPN)
- **Credentials**: OAuth2 client credentials for Moata API

### Setup Steps

#### 1. Clone Repository

```bash
git clone <repository-url>
cd internship-project
```

#### 2. Create Virtual Environment

**Windows (PowerShell):**
```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

**Windows (Command Prompt):**
```cmd
python -m venv .venv
.venv\Scripts\activate.bat
```

**macOS/Linux:**
```bash
python3 -m venv .venv
source .venv/bin/activate
```

#### 3. Install Dependencies

```bash
# Upgrade pip first
pip install --upgrade pip

# Install all required packages
pip install -r requirements.txt
```

**Note**: If you encounter errors with `shapely` on Windows, you may need to install it separately using a wheel file from [Christoph Gohlke's collection](https://www.lfd.uci.edu/~gohlke/pythonlibs/).

#### 4. Configure Environment Variables

```bash
# Copy template
cp .env.example .env

# Edit .env with your credentials
# Use your preferred text editor (notepad, nano, vim, etc.)
```

See [Configuration](#configuration) section for detailed `.env` setup.

#### 5. Verify Installation

```bash
# Test imports
python -c "import moata_pipeline; print('✓ Installation successful')"

# Check if credentials are loaded
python -c "from dotenv import load_dotenv; import os; load_dotenv(); print('✓ Credentials found' if os.getenv('MOATA_CLIENT_ID') else '✗ Missing credentials')"
```

---

## Configuration

### Environment Variables

Create a `.env` file in the project root with the following variables:

```bash
# Moata API OAuth2 Credentials (REQUIRED)
MOATA_CLIENT_ID=your_oauth_client_id_here
MOATA_CLIENT_SECRET=your_oauth_secret_here

# Optional: Logging Level (can also be set via --log-level flag)
# LOG_LEVEL=INFO  # Options: DEBUG, INFO, WARNING, ERROR, CRITICAL
```

**Note:** API URLs (`https://api.moata.io`) are configured in the code. If you need to change them (e.g., for testing), edit `moata_pipeline/common/constants.py`.

### How to Get Credentials

1. Contact your Auckland Council supervisor or project manager
2. Request **Moata API OAuth2 credentials** for the rain monitoring project
3. You will receive:
   - **Client ID** (alphanumeric string)
   - **Client Secret** (long alphanumeric string)
4. Copy these into your `.env` file
5. Never commit `.env` to Git (already in `.gitignore`)

### Security Best Practices

⚠️ **IMPORTANT**: 
- Never share your `.env` file or credentials
- Never commit `.env` to version control
- Rotate credentials if compromised
- Use different credentials for development vs. production

---

## Usage

### Rain Gauge Pipeline

The rain gauge pipeline processes data from ~200 point-based rainfall gauges across Auckland.

#### Complete Workflow

```bash
# 1. Collect data from Moata API
python retrieve_rain_gauges.py
# → Fetches last 30 days of data
# → Outputs to: outputs/rain_gauges/raw/
# → Duration: ~5-10 minutes

# Advanced options:
python retrieve_rain_gauges.py --log-level DEBUG   # Verbose logging
python retrieve_rain_gauges.py --help              # Show all options

# 2. Analyze and filter gauges
python analyze_rain_gauges.py
# → Applies quality filters
# → Calculates ARI for each gauge
# → Outputs to: outputs/rain_gauges/analyze/
# → Duration: ~2-3 minutes

# Advanced options:
python analyze_rain_gauges.py --inactive-months 6          # Custom inactivity threshold
python analyze_rain_gauges.py --exclude-keyword "backup"   # Custom exclusion filter
python analyze_rain_gauges.py --log-level DEBUG            # Verbose logging

# 3. Generate interactive dashboard
python visualize_rain_gauges.py
# → Creates HTML dashboard with charts
# → Outputs to: outputs/rain_gauges/visualizations/
# → Duration: ~3-5 minutes

# Advanced options:
python visualize_rain_gauges.py --csv path/to/analysis.csv   # Custom input
python visualize_rain_gauges.py --out custom/output/dir/     # Custom output
python visualize_rain_gauges.py --log-level DEBUG            # Verbose logging

# 4. Validate alarm configurations (optional)
python validate_ari_alarms_rain_gauges.py
# → Requires: data/inputs/raingauge_ari_alarms.csv
# → Compares configured alarms vs. actual data
# → Outputs to: outputs/rain_gauges/ari_alarm_validation.csv
# → Duration: ~1-2 minutes

# 5. Visualize validation results (optional)
python visualize_ari_alarms_rain_gauges.py
# → Creates validation dashboard
# → Outputs to: outputs/rain_gauges/validation_viz/
# → Duration: ~2-3 minutes
```

#### Output Files

After running the complete pipeline:

```
outputs/rain_gauges/
├── raw/
│   ├── rain_gauges_YYYYMMDD_HHMMSS.json       # Raw API response
│   └── collection_summary.json                 # Collection metadata
├── analyze/
│   ├── rain_gauge_analysis_YYYYMMDD.csv       # Filtered gauges
│   ├── rain_gauge_ari_results_YYYYMMDD.csv    # ARI calculations
│   └── analysis_summary.json                   # Analysis stats
├── visualizations/
│   ├── dashboard.html                          # Main dashboard
│   └── gauges/                                 # Per-gauge pages
│       ├── GAUGE001.html
│       ├── GAUGE002.html
│       └── ...
├── validation_viz/
│   └── validation_dashboard.html               # Validation comparison
└── ari_alarm_validation.csv                    # Validation results
```

#### Command-Line Options

| Script | Options | Description |
|--------|---------|-------------|
| `retrieve_rain_gauges.py` | `--log-level LEVEL` | Set logging level (DEBUG/INFO/WARNING/ERROR) |
| | `--help` | Show usage and examples |
| `analyze_rain_gauges.py` | `--inactive-months N` | Inactivity threshold in months (default: 3) |
| | `--exclude-keyword WORD` | Exclude gauges with keyword (default: "test") |
| | `--log-level LEVEL` | Set logging level |
| | `--help` | Show usage and examples |
| `visualize_rain_gauges.py` | `--csv PATH` | Custom input CSV (auto-detects if omitted) |
| | `--out DIR` | Custom output directory |
| | `--log-level LEVEL` | Set logging level |
| | `--help` | Show usage and examples |
| `validate_ari_alarms_rain_gauges.py` | *(to be documented)* | *(options pending)* |
| `visualize_ari_alarms_rain_gauges.py` | *(to be documented)* | *(options pending)* |

---

### Rain Radar Pipeline

The radar pipeline processes spatial rainfall data from QPE (Quantitative Precipitation Estimation) radar.

#### Complete Workflow

**Option A: Current Data (Last 24 Hours)**

```bash
# 1. Collect current radar data
python retrieve_rain_radar.py
# → Fetches last 24 hours
# → Outputs to: outputs/rain_radar/raw/
# → Duration: ~15-20 minutes (200 catchments × API calls)

# 2. Calculate ARI
python analyze_rain_radar.py --current
# → Processes current data
# → Outputs to: outputs/rain_radar/analyze/
# → Duration: ~10-15 minutes

# 3. Visualize
python visualize_rain_radar.py
# → Auto-detects current data
# → Outputs to: outputs/rain_radar/visualizations/
# → Duration: ~5-7 minutes
```

**Option B: Historical Date**

```bash
# 1. Collect historical data for specific date
python retrieve_rain_radar.py --date 2025-05-09
# → Fetches all data for 2025-05-09
# → Outputs to: outputs/rain_radar/historical/2025-05-09/raw/
# → Duration: ~20-30 minutes

# 2. Calculate ARI for that date
python analyze_rain_radar.py --date 2025-05-09
# → Outputs to: outputs/rain_radar/historical/2025-05-09/analyze/
# → Duration: ~10-15 minutes

# 3. Generate dashboard
python visualize_rain_radar.py --date 2025-05-09
# → Outputs to: outputs/rain_radar/historical/2025-05-09/dashboard/
# → Duration: ~5-7 minutes

# 4. Validate alarms (optional)
python validate_ari_alarms_rain_radar.py --date 2025-05-09
# → Outputs to: outputs/rain_radar/historical/2025-05-09/ari_alarm_validation.csv
# → Duration: ~2-3 minutes

# 5. Visualize validation (optional)
python visualize_ari_alarms_rain_radar.py --date 2025-05-09
# → Outputs to: outputs/rain_radar/historical/2025-05-09/validation_viz/
# → Duration: ~3-4 minutes
```

#### Auto-Detection Mode

The analysis and visualization scripts can auto-detect the most recent data:

```bash
# Analyze most recent data (current or historical)
python analyze_rain_radar.py

# Visualize most recent data
python visualize_rain_radar.py
```

#### Command-Line Options

| Script | Options | Description |
|--------|---------|-------------|
| `retrieve_rain_radar.py` | `--date YYYY-MM-DD` | Fetch historical date |
| | *(no args)* | Fetch last 24 hours |
| `analyze_rain_radar.py` | `--date YYYY-MM-DD` | Analyze specific date |
| | `--current` | Analyze current data only |
| | *(no args)* | Auto-detect latest |
| `visualize_rain_radar.py` | `--date YYYY-MM-DD` | Visualize specific date |
| | *(no args)* | Auto-detect latest |

---

### Documentation Generation

Generate a comprehensive Word document for reporting:

```bash
python generate_documentation.py
# → Creates: outputs/documentation/Rain_Monitoring_System_Documentation.docx
# → Includes: System overview, methodology, results, recommendations
# → Duration: ~30 seconds
```

**Requirements**: 
- `python-docx` library (included in `requirements.txt`)
- Template sections defined in `moata_pipeline/common/`

---

## Key Concepts

### ARI (Annual Recurrence Interval)

The **Annual Recurrence Interval** (ARI) indicates how rare a rainfall event is. An ARI of 10 years means that, statistically, a rainfall event of this magnitude occurs once every 10 years on average.

#### Calculation Method

This project uses the **TP108 methodology** (Auckland Regional Council Technical Publication 108):

```
ARI = exp(m × D + b)
```

Where:
- **D** = Rainfall depth (mm) for a specific duration
- **m**, **b** = Regression coefficients (location-specific, from `tp108_stats.csv`)
- **exp()** = Exponential function

#### Example Calculation

For a location with coefficients `m = 0.045`, `b = 1.2`:
- Observed rainfall: 50mm in 1 hour
- ARI = exp(0.045 × 50 + 1.2) = exp(3.45) ≈ **31.5 years**

This means a 50mm/hour rainfall event occurs approximately once every 31.5 years at this location.

#### Durations Analyzed

The system calculates ARI for 8 standard durations:

| Duration | Code | Use Case |
|----------|------|----------|
| 10 minutes | 10m | Flash flooding, urban drainage |
| 20 minutes | 20m | Storm surge intensity |
| 30 minutes | 30m | Short-term runoff |
| 1 hour | 60m | Infrastructure design standard |
| 2 hours | 2h | Moderate storm events |
| 6 hours | 6h | Extended rainfall |
| 12 hours | 12h | All-day storms |
| 24 hours | 24h | Multi-day weather patterns |

---

### Alarm Thresholds

The system uses different thresholds for gauges vs. radar due to their spatial characteristics:

#### Rain Gauge Alarms

- **Threshold**: ARI ≥ 5 years at a single gauge
- **Logic**: Point-based measurement
- **Use**: Immediate local flooding risk

#### Rain Radar Alarms

- **Threshold**: ≥30% of catchment area with ARI ≥ 5 years
- **Logic**: Spatial proportion (areal coverage)
- **Use**: Catchment-wide flooding risk

#### Why Different Thresholds?

- **Gauges**: Measure exact rainfall at one point → lower threshold needed
- **Radar**: Averages over pixels → requires broader spatial coverage to indicate significant risk

---

### Data Quality Filters

The analysis pipeline applies these filters to ensure data reliability:

#### Rain Gauge Filters

1. **Temporal Coverage**: At least 80% non-null values
2. **Recency**: Data within last 30 days
3. **Value Range**: 0 ≤ rainfall ≤ 500 mm/hour (outlier removal)
4. **Coordinate Validity**: Valid latitude/longitude within Auckland region

#### Radar Data Filters

1. **Pixel Coverage**: At least 50% of catchment pixels have data
2. **Temporal Completeness**: No more than 10% missing timestamps
3. **Spatial Consistency**: Adjacent pixels show correlated values
4. **ARI Validity**: Calculated ARI > 0 and < 1000 years

---

## Data Management

### Storage Strategy

#### Git Repository

**Included in Git:**
- Source code (`moata_pipeline/`)
- Entry point scripts (`*.py`)
- Input reference data (`data/inputs/`)
- Documentation (`README.md`, etc.)

**Excluded from Git** (via `.gitignore`):
- All outputs (`outputs/`)
- API credentials (`.env`)
- Python cache (`__pycache__/`, `*.pyc`)
- Virtual environment (`.venv/`)

#### Large File Handling

For sharing large datasets (>100MB):

```bash
# Option 1: Use Git LFS (if configured)
git lfs track "outputs/**/*.json"
git lfs track "outputs/**/*.csv"
git add .gitattributes
git add outputs/rain_radar/raw/
git commit -m "Add radar data with Git LFS"
git push

# Option 2: Use external storage (recommended)
# - Upload to Auckland Council OneDrive
# - Share link in project documentation
# - Keep Git repository lightweight
```

### Backup Recommendations

1. **Daily Backups**: Sync `outputs/` to OneDrive or network drive
2. **Code Backups**: Push to Git after each significant change
3. **Input Data**: Keep copies of `raingauge_ari_alarms.csv` and `tp108_stats.csv`
4. **Credentials**: Store `.env` backup in secure password manager

### Disk Space Management

```bash
# Check outputs directory size
du -sh outputs/

# Remove old radar data (keep last 7 days)
python -c "
from pathlib import Path
from datetime import datetime, timedelta
cutoff = datetime.now() - timedelta(days=7)
for date_dir in Path('outputs/rain_radar/historical').iterdir():
    if date_dir.is_dir():
        date = datetime.strptime(date_dir.name, '%Y-%m-%d')
        if date < cutoff:
            print(f'Remove {date_dir}?')
"

# Compress old outputs
tar -czf outputs_backup_$(date +%Y%m%d).tar.gz outputs/
```

---

## API Configuration

### Moata API Parameters

These parameters are configured in `moata_pipeline/common/constants.py`:

| Parameter | Value | Description |
|-----------|-------|-------------|
| **Project ID** | 594 | Auckland Council rain monitoring project |
| **Rain Gauge Asset Type** | 25 | Rain gauge sensor type ID |
| **Stormwater Catchment Asset Type** | 3541 | Catchment boundary type ID |
| **Radar Collection ID** | 1 | QPE radar data collection |
| **Radar TraceSet ID** | 3 | Timeseries data traceset |

### Rate Limiting

The API client implements automatic rate limiting:

- **Requests per minute**: 100 (conservative limit)
- **Retry logic**: Exponential backoff (1s, 2s, 4s, 8s)
- **Max retries**: 5 attempts
- **Timeout**: 30 seconds per request

### Authentication Flow

```
1. Client requests access token using client_id + client_secret
2. Moata OAuth2 server returns access token (valid 1 hour)
3. Client includes token in all API requests: Authorization: Bearer <token>
4. Token automatically refreshed when expired
5. Refresh token valid for 30 days
```

### API Endpoints Used

| Endpoint | Purpose | Rate Impact |
|----------|---------|-------------|
| `/oauth2/token` | Authentication | Low (once per session) |
| `/projects/{id}/assets` | List gauges/catchments | Medium (~5-10 calls) |
| `/assets/{id}/traces` | Get trace metadata | Medium (~200 calls) |
| `/traces/{id}/data` | Fetch timeseries data | High (~1000+ calls) |

---

## Performance & Limitations

### Expected Runtime

| Task | Duration | Bottleneck |
|------|----------|------------|
| Rain gauge collection | 5-10 min | API rate limit |
| Rain gauge analysis | 2-3 min | DataFrame operations |
| Rain gauge visualization | 3-5 min | HTML generation |
| Radar collection (current) | 15-20 min | API rate limit |
| Radar collection (historical) | 20-30 min | API rate limit + data volume |
| Radar analysis | 10-15 min | ARI calculations |
| Radar visualization | 5-7 min | HTML generation |

### Memory Requirements

| Task | RAM Usage | Peak |
|------|-----------|------|
| Rain gauge processing | ~500 MB | 1 GB |
| Radar current data | ~1 GB | 2 GB |
| Radar historical data | ~2 GB | 4 GB |

**Recommendation**: Minimum 4GB system RAM, 8GB preferred for radar processing.

### Known Limitations

#### Data Availability

- **Historical radar data**: Available from January 1, 2024 onwards
- **Rain gauge data**: 30-day rolling window (older data archived)
- **TP108 coefficients**: Only available for Auckland region pixels

#### API Constraints

- **Rate limit**: 100 requests/minute (shared across all Auckland Council users)
- **Timeout**: 30 seconds per request (some large responses may fail)
- **Concurrent access**: Cannot run multiple radar collections in parallel

#### Technical Constraints

- **Single-threaded**: No parallel processing (to respect rate limits)
- **Memory-intensive**: Large historical datasets require 4+ GB RAM
- **Disk I/O**: Writing JSON files can be slow on network drives

#### Data Quality

- **Missing data**: Some gauges have <50% uptime
- **Outliers**: Occasional sensor errors produce impossible values (filtered out)
- **Spatial gaps**: Not all catchments have complete radar coverage
- **Temporal resolution**: Radar data at 10-minute intervals (not continuous)

### Optimization Tips

```bash
# 1. Use SSD for outputs directory (faster I/O)
# Move outputs to local SSD, symlink from project
mv outputs /path/to/ssd/outputs
ln -s /path/to/ssd/outputs outputs

# 2. Process specific date ranges
python retrieve_rain_gauges.py --start-date 2024-12-01 --end-date 2024-12-07

# 3. Run overnight for large historical analyses
nohup python retrieve_rain_radar.py --date 2024-11-15 > radar_log.txt 2>&1 &

# 4. Use incremental processing
# Instead of re-analyzing all data, only process new data
```

---

## Troubleshooting

### Common Issues

#### 1. Authentication Errors

**Error:**
```
requests.exceptions.HTTPError: 401 Client Error: Unauthorized
```

**Solutions:**
```bash
# Check .env file exists and has correct credentials
cat .env

# Verify credentials are loaded
python -c "from dotenv import load_dotenv; import os; load_dotenv(); print(os.getenv('MOATA_CLIENT_ID'))"

# Test authentication
python -c "from moata_pipeline.moata.auth import get_access_token; print(get_access_token())"

# If still failing, request new credentials from supervisor
```

#### 2. Rate Limiting

**Error:**
```
requests.exceptions.HTTPError: 429 Too Many Requests
```

**Solutions:**
```bash
# Wait 1 minute, then retry
sleep 60 && python retrieve_rain_radar.py

# Check if another process is using the API
# (Only one collection should run at a time)

# Reduce concurrent operations in code
# Edit moata_pipeline/moata/http.py and decrease batch size
```

#### 3. Memory Errors

**Error:**
```
MemoryError: Unable to allocate array
```

**Solutions:**
```bash
# Close other applications to free RAM

# Process smaller date ranges
python retrieve_rain_radar.py --date 2024-12-01  # Single day only

# Increase system swap space (Linux/macOS)
sudo swapon --show

# Use a machine with more RAM
```

#### 4. File Size Errors (Git)

**Error:**
```
remote: error: File outputs/rain_radar/raw/data.json is 150 MB; exceeds GitHub's file size limit of 100 MB
```

**Solutions:**
```bash
# Option 1: Use Git LFS (recommended for large files)
git lfs install
git lfs track "outputs/**/*.json"
git add .gitattributes
git add outputs/
git commit -m "Track large files with Git LFS"
git push

# Option 2: Remove large files from Git (use external storage)
git rm --cached outputs/rain_radar/raw/data.json
echo "outputs/" >> .gitignore
git commit -m "Remove large outputs from repo"

# Option 3: Upload to OneDrive instead
# Then share link in documentation
```

#### 5. Missing Dependencies

**Error:**
```
ModuleNotFoundError: No module named 'shapely'
```

**Solutions:**
```bash
# Reinstall requirements
pip install -r requirements.txt

# If shapely fails on Windows, install from wheel
pip install shapely-1.8.5-cp310-cp310-win_amd64.whl

# Verify installation
python -c "import shapely; print(shapely.__version__)"
```

#### 6. Empty Outputs

**Issue:** Scripts run successfully but produce no outputs.

**Solutions:**
```bash
# Check if data exists for the requested date
python retrieve_rain_radar.py --date 2023-12-01  # Before radar was available

# Verify filters aren't too strict
# Edit moata_pipeline/analyze/filtering.py and adjust thresholds

# Check logs for warnings
tail -100 rain_pipeline.log

# Run with debug logging
LOG_LEVEL=DEBUG python analyze_rain_gauges.py
```

#### 7. Slow Performance

**Issue:** Scripts take much longer than expected.

**Solutions:**
```bash
# Check network speed
ping api.moata.io

# Verify not running on slow network drive
df -h .  # Check if on network mount

# Use local disk for outputs
export OUTPUT_DIR=/path/to/local/disk

# Monitor resource usage
# Windows: Task Manager
# Linux/macOS: htop or top
```

### Debug Mode

Enable detailed logging:

```bash
# Set in .env
LOG_LEVEL=DEBUG

# Or set temporarily
LOG_LEVEL=DEBUG python retrieve_rain_gauges.py

# View logs
tail -f rain_pipeline.log
```

### Getting Help

If issues persist:

1. **Check logs**: `outputs/logs/` or `rain_pipeline.log`
2. **Review error message**: Note exact error text
3. **Contact supervisor**: Provide error message and logs
4. **Submit issue**: Use project issue tracker (if available)

---

## FAQ

### General

**Q: How often should I run the pipeline?**
A: For operational monitoring, run rain gauge collection daily. Radar collection can be weekly or as needed for specific events.

**Q: Can I run multiple collections in parallel?**
A: No, API rate limiting will cause failures. Run collections sequentially.

**Q: How long is data retained in Moata API?**
A: Rain gauge data: 30-day rolling window. Radar data: Historical archives from 2024-01-01.

**Q: What's the difference between rain gauges and rain radar?**
A: Gauges measure exact rainfall at specific points. Radar provides spatial coverage across catchments but with lower precision.

### Data Collection

**Q: Why does radar collection take so long?**
A: Each catchment requires multiple API calls (~5 per catchment × 200 catchments = 1000 calls ≈ 20 minutes at 100 calls/minute).

**Q: Can I collect data for multiple dates at once?**
A: Not directly. Use a bash script to loop through dates:
```bash
for date in 2024-12-{01..07}; do
  python retrieve_rain_radar.py --date $date
  sleep 300  # Wait 5 minutes between collections
done
```

**Q: What if collection fails mid-way?**
A: The system saves progress incrementally. Re-run the script and it will resume from where it left off (already collected data won't be re-fetched).

### Analysis

**Q: What if ARI calculations seem unrealistic?**
A: Check `tp108_stats.csv` for your location. Some pixels may have outdated or incorrect coefficients. Report to supervisor.

**Q: Why do some gauges show no data?**
A: Gauges may be offline, under maintenance, or filtered out due to poor data quality (<80% uptime).

**Q: How accurate is the TP108 method?**
A: TP108 is calibrated for Auckland region using historical data. Accuracy decreases for extreme events (ARI > 100 years).

### Visualization

**Q: Can I customize the dashboard?**
A: Yes, edit templates in `moata_pipeline/viz/`. Dashboards use HTML + embedded matplotlib charts.

**Q: Why are some charts blank?**
A: Likely insufficient data for that duration or location. Check raw data files.

**Q: Can I export data to Excel?**
A: Yes, most CSV outputs can be opened in Excel. Use `pandas.to_excel()` in scripts if needed.

### Technical

**Q: Which Python version should I use?**
A: Python 3.10 or higher. Tested with 3.10, 3.11, and 3.12.

**Q: Can I use this on macOS/Linux?**
A: Yes, fully cross-platform. Use appropriate virtual environment activation commands.

**Q: Is there a GUI?**
A: No, this is a command-line tool. Outputs are HTML dashboards you can view in a browser.

---

## Testing

### Manual Testing

Verify installation and basic functionality:

```bash
# 1. Test imports
python -c "from moata_pipeline.collect import RainGaugeCollector; print('✓ Imports work')"

# 2. Test authentication
python -c "from moata_pipeline.moata.auth import get_access_token; token = get_access_token(); print(f'✓ Got token: {token[:20]}...')"

# 3. Test small collection
python retrieve_rain_gauges.py  # Should complete in 5-10 min

# 4. Test analysis
python analyze_rain_gauges.py  # Should produce outputs/rain_gauges/analyze/

# 5. Verify outputs
ls -lh outputs/rain_gauges/analyze/
```

### Validation Checks

After running the pipeline, verify data quality:

```bash
# Check gauge count
python -c "
import pandas as pd
df = pd.read_csv('outputs/rain_gauges/analyze/rain_gauge_analysis_*.csv')
print(f'✓ Found {len(df)} gauges')
"

# Check ARI calculations
python -c "
import pandas as pd
df = pd.read_csv('outputs/rain_gauges/analyze/rain_gauge_ari_results_*.csv')
print(f'✓ Calculated ARI for {len(df)} gauge-duration combinations')
print(f'  ARI range: {df['ari'].min():.1f} - {df['ari'].max():.1f} years')
"

# Check for missing data
python -c "
import pandas as pd
df = pd.read_csv('outputs/rain_gauges/raw/rain_gauges_*.json')
null_pct = df.isnull().sum().sum() / df.size * 100
print(f'✓ Null values: {null_pct:.1f}%')
"
```

### Automated Tests

Future enhancement: Unit tests using `pytest`:

```bash
# Planned structure (not yet implemented)
tests/
├── test_auth.py           # Test Moata authentication
├── test_collectors.py     # Test data collection
├── test_ari_calc.py       # Test ARI calculations
├── test_filtering.py      # Test data filters
└── test_utils.py          # Test utility functions
```

---

## Dependencies

### Core Dependencies

All dependencies are specified in `requirements.txt`:

```txt
# HTTP & API
requests>=2.31.0           # HTTP client
oauthlib>=3.2.2           # OAuth2 authentication
requests-oauthlib>=1.3.1  # OAuth2 for requests

# Data Processing
pandas>=2.1.0             # DataFrame operations
numpy>=1.24.0             # Numerical computing

# Visualization
matplotlib>=3.8.0         # Plotting and charts

# Configuration
python-dotenv>=1.0.0      # Environment variable loading

# Geometry (optional for radar)
shapely>=2.0.0            # Geometry simplification for catchments

# Documentation (optional)
python-docx>=1.1.0        # Word document generation
```

### Optional Dependencies

**For development:**
```bash
pip install pytest pytest-cov black flake8 mypy
```

**For advanced visualization:**
```bash
pip install plotly seaborn folium
```

### Dependency Notes

- **shapely**: Required for radar processing. Windows users may need wheel files.
- **python-docx**: Only needed for `generate_documentation.py`
- **oauthlib**: Critical for Moata API authentication

### Version Pinning

Current `requirements.txt` uses minimum versions (`>=`). For production, consider pinning exact versions:

```bash
# Generate exact versions
pip freeze > requirements-pinned.txt

# Install exact versions
pip install -r requirements-pinned.txt
```

---

## Contributing

### For Auckland Council Interns

This is an internal project. If you're continuing this work:

1. **Read existing code** before making changes
2. **Follow existing patterns** (folder structure, naming conventions)
3. **Update documentation** when adding features
4. **Test thoroughly** before committing
5. **Ask questions** if unsure about approach

### Code Style

- **Python**: Follow PEP 8 style guide
- **Imports**: Group by standard library, third-party, local
- **Docstrings**: Use Google-style docstrings
- **Type hints**: Use where appropriate (especially function signatures)

### Git Workflow

```bash
# 1. Create feature branch
git checkout -b feature/new-analysis

# 2. Make changes
# ... edit files ...

# 3. Test locally
python retrieve_rain_gauges.py

# 4. Commit with descriptive message
git add moata_pipeline/analyze/new_analysis.py
git commit -m "Add seasonal ARI analysis module"

# 5. Push to remote
git push origin feature/new-analysis

# 6. Request review from supervisor
```

### Adding New Features

When adding new functionality:

1. **Create module** in appropriate package (`collect/`, `analyze/`, `viz/`)
2. **Add entry point** as `action_feature.py` in project root
3. **Update README** with usage examples
4. **Add to requirements.txt** if new dependencies needed
5. **Document in docstrings** with examples

Example:
```python
# moata_pipeline/analyze/seasonal.py
def calculate_seasonal_ari(df: pd.DataFrame, season: str) -> pd.DataFrame:
    """
    Calculate ARI grouped by season.
    
    Args:
        df: Rain gauge data with datetime index
        season: Season name ('summer', 'winter', etc.)
    
    Returns:
        DataFrame with seasonal ARI values
        
    Example:
        >>> df = pd.read_csv('rain_data.csv')
        >>> summer_ari = calculate_seasonal_ari(df, 'summer')
    """
    # Implementation...
```

---

## Support

### Getting Help

| Issue Type | Contact | Response Time |
|------------|---------|---------------|
| **Technical errors** | Supervisor (email in OneDrive) | 1-2 business days |
| **API credentials** | Auckland Council IT | Same day |
| **Data questions** | Sam (Historical Data) | 2-3 business days |
| **TP108 methodology** | Project lead | 1 week |

### Useful Resources

- **Moata API Docs**: [Internal Wiki Link]
- **TP108 Technical Publication**: `data/inputs/tp108_methodology.pdf`
- **Auckland Council GIS**: [Internal Portal]
- **Project OneDrive**: [Shared Folder Link]

### Reporting Bugs

When reporting issues, include:

1. **Error message** (full traceback)
2. **Steps to reproduce** (exact commands run)
3. **Environment** (Python version, OS)
4. **Log files** (`rain_pipeline.log`)
5. **Expected vs actual behavior**

Example bug report:
```
**Issue**: Radar collection fails with 429 error

**Steps**:
1. python retrieve_rain_radar.py --date 2024-12-01
2. After ~100 requests, see error

**Error**:
requests.exceptions.HTTPError: 429 Too Many Requests

**Environment**:
- Python 3.10.5
- Windows 11
- outputs/ on network drive

**Expected**: Complete collection in 20 minutes
**Actual**: Fails after 10 minutes

**Logs**: Attached rain_pipeline.log
```

---

## License

**Internal Auckland Council Use Only**

This software is developed for Auckland Council's internal operations and is not licensed for external use, modification, or distribution.

Copyright © 2024-2025 Auckland Council. All rights reserved.

### Usage Restrictions

- ✅ Use for Auckland Council flood resilience projects
- ✅ Academic use for COMPSCI 778 coursework
- ❌ No external distribution without written permission
- ❌ No commercial use
- ❌ No modification of Moata API client without approval

### Data Privacy

This system processes operational data from Auckland Council's monitoring network. All data remains confidential and subject to Auckland Council's data governance policies.

---

## Acknowledgments

**Developed by**: Juang (COMPSCI 778 Intern)  
**Supervisor**: [Supervisor Name], Auckland Council Healthy Waters  
**Historical Data**: Sam (Auckland Council)  
**API Access**: Auckland Council IT Team  
**Institution**: University of Auckland  

Special thanks to the Auckland Council Healthy Waters team for providing access to the Moata API and supporting this research project.

---

## Project Status & Roadmap

### Current Status (v1.0)

✅ Rain gauge data collection and analysis  
✅ Rain radar (QPE) data collection and analysis  
✅ ARI calculation using TP108 methodology  
✅ Alarm validation framework  
✅ Interactive HTML dashboards  
✅ Word documentation generation  

### Future Enhancements (v2.0)

- [ ] Real-time alerting system (email/SMS when ARI > 10 years)
- [ ] Machine learning for rainfall prediction
- [ ] Integration with Auckland Council GIS
- [ ] REST API for web dashboard
- [ ] Automated scheduled runs (cron/Task Scheduler)
- [ ] Unit test suite (pytest)
- [ ] Docker containerization
- [ ] Performance optimization (parallel processing where safe)

### Known Issues

1. Radar collection slow for large historical periods (API limitation)
2. Memory usage high for processing >7 days of radar data
3. No automated cleanup of old outputs (manual deletion required)
4. HTML dashboards not mobile-responsive

---

**Last Updated**: December 28, 2024  
**Version**: 1.0.0  
**Maintained by**: COMPSCI 778 Internship Team