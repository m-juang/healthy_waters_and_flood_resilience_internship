# Healthy Waters & Flood Resilience Internship

**Moata Rain Gauge Data Pipeline**

A modular Python data pipeline for **collecting, filtering, analysing, and visualising rain gauge alarms** from the Moata Analytical Engine API to support **Healthy Waters & Flood Resilience** initiatives at Auckland Council.

---

## Overview

This project interfaces with the **Moata Analytical Engine (AE) API** to provide an end-to-end workflow:

1. **Collect** rain gauge assets, traces, thresholds, and alarm metadata
2. **Filter** active Auckland gauges based on data freshness
3. **Analyse** alarm configurations (overflow & recency)
4. **Visualise** results in a **self-contained HTML report**
5. **Produce reproducible outputs** (CSV, JSON, PNG, HTML)

The codebase has been **refactored into a clean, modular pipeline architecture** following **DRY (Don't Repeat Yourself)** principles to support maintainability and future extensions.

---

## Key Features

* ✅ **Clean pipeline architecture** with separation of concerns
* ✅ **Sequential API processing** (rate-limit safe: 800 requests / 5 minutes)
* ✅ **OAuth2 client-credentials authentication** with automatic refresh
* ✅ **Robust retry logic** with exponential backoff
* ✅ **Active gauge filtering** (Auckland region + recent data within 3 months)
* ✅ **Comprehensive alarm analysis**
  * Threshold alarms (overflow monitoring)
  * Data freshness alarms (recency monitoring)
  * Critical flag identification
* ✅ **Rich visual reporting**
  * Interactive charts (PNG)
  * Self-contained HTML summary with risk scoring
  * Per-gauge detailed HTML pages
* ✅ **Reproducible environment** with locked dependencies
* ✅ **Reusable utility modules** in `common/` package

---

## Project Structure

```
.
├── moata_data_retriever.py        # Entry point: collect raw data from Moata API
├── filter_active_rain_gauges.py   # Filter + analyse active Auckland gauges
├── visualizer.py                  # Generate HTML + PNG reports
│
├── moata_pipeline/
│   ├── __init__.py
│   ├── logging_setup.py           # Centralized logging configuration
│   │
│   ├── common/                    # Shared utilities (DRY principle)
│   │   ├── constants.py           # API endpoints & configuration
│   │   ├── dataframe_utils.py     # DataFrame type coercion helpers
│   │   ├── file_utils.py          # File/directory operations
│   │   ├── html_utils.py          # HTML generation helpers
│   │   ├── json_io.py             # JSON read/write utilities
│   │   ├── paths.py               # Pipeline path management
│   │   ├── text_utils.py          # Text sanitization
│   │   ├── time_utils.py          # Date/time parsing & formatting
│   │   └── typing_utils.py        # Type definitions
│   │
│   ├── moata/                     # Moata API client layer
│   │   ├── auth.py                # OAuth2 token handling with refresh
│   │   ├── http.py                # HTTP client with retry & rate limiting
│   │   ├── client.py              # High-level API methods
│   │   └── endpoints.py           # API endpoint definitions
│   │
│   ├── collect/                   # Stage 1: Data collection
│   │   ├── collector.py           # Rain gauge data collector
│   │   └── runner.py              # Collection orchestrator
│   │
│   ├── analyze/                   # Stage 2: Filtering & analysis
│   │   ├── alarm_analysis.py      # Alarm data extraction
│   │   ├── filtering.py           # Active gauge identification
│   │   ├── reporting.py           # Text report generation
│   │   └── runner.py              # Analysis orchestrator
│   │
│   └── viz/                       # Stage 3: Visualization
│       ├── charts.py              # Chart generation (matplotlib)
│       ├── cleaning.py            # Data cleaning for viz
│       ├── pages.py               # Per-gauge HTML pages
│       ├── report.py              # Main HTML report builder
│       └── runner.py              # Visualization orchestrator
│
├── moata_output/                  # Raw collected data (JSON)
│   ├── rain_gauges.json
│   └── rain_gauges_traces_alarms.json
│
├── moata_filtered/                # Filtered & analysed data
│   ├── active_auckland_gauges.json
│   ├── alarm_summary.csv
│   ├── alarm_summary.json
│   ├── analysis_report.txt
│   └── viz/                       # Visual reports
│       ├── report.html            # Main interactive report
│       ├── cleaned_alarm_summary.csv
│       ├── 01_records_by_gauge.png
│       ├── 02_record_categories.png
│       ├── 03_severity_distribution.png
│       ├── 04_threshold_hist.png
│       ├── 05_critical_flag.png
│       ├── 06_ladder_gauge_*.png  # Threshold ladder charts
│       └── 07_gauge_pages/        # Individual gauge pages
│           └── *.html
│
├── .env.example                   # Environment template
├── .gitignore
├── requirements.txt               # Locked dependencies
└── README.md
```

---

## Installation

### Prerequisites

* **Python 3.10+** (tested on 3.10, 3.11, 3.12)
* Moata API client credentials
* Internet connection for API access

### Setup

```bash
# Clone repository
git clone https://github.com/m-juang/healthy_waters_and_flood_resilience_internship.git
cd healthy_waters_and_flood_resilience_internship

# Create virtual environment
python -m venv .venv

# Activate virtual environment
.venv\Scripts\Activate.ps1   # Windows PowerShell
# .venv\Scripts\activate.bat  # Windows CMD
# source .venv/bin/activate   # macOS / Linux

# Install dependencies
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

### Configure credentials

```bash
# Copy environment template
cp .env.example .env

# Edit .env with your credentials
```

**.env file:**
```env
MOATA_CLIENT_ID=your_client_id_here
MOATA_CLIENT_SECRET=your_client_secret_here
```

> ⚠️ **Security:** Never commit `.env` to version control (already excluded via `.gitignore`)

---

## Usage

### Complete Pipeline (3 stages)

#### 1️⃣ Collect data from Moata API

```bash
python moata_data_retriever.py
```

**What happens:**
* Authenticates via OAuth2 (automatic token refresh)
* Fetches all rain gauge assets from project 594
* Collects traces, thresholds, overflow alarms, and detailed alarm metadata
* Saves structured JSON to `moata_output/`
  * `rain_gauges.json` - basic gauge info
  * `rain_gauges_traces_alarms.json` - full nested structure

⏱️ **Runtime:** ~45-60 minutes (sequential processing, rate-limit safe at 2 req/sec)

📊 **Output:** `moata_output/rain_gauges_traces_alarms.json`

---

#### 2️⃣ Filter & analyse active gauges

```bash
python filter_active_rain_gauges.py
```

**What happens:**
* Filters to **Auckland gauges only** (excludes Northland)
* Identifies **active gauges** (data within last 3 months)
* Extracts primary "Rainfall" trace per gauge
* Normalizes alarm configurations from multiple sources:
  * Overflow alarms
  * Threshold configurations
  * Detailed alarms (recency monitoring)
* Generates alarm summary CSV with all configurations

⏱️ **Runtime:** < 1 minute

📊 **Outputs:**
* `moata_filtered/active_auckland_gauges.json` - filtered gauge list
* `moata_filtered/alarm_summary.csv` - normalized alarm table
* `moata_filtered/alarm_summary.json` - same data in JSON format
* `moata_filtered/analysis_report.txt` - human-readable summary

**Key filtering criteria:**
* Region: Auckland (excludes "northland" keyword)
* Data freshness: Last data within 3 months
* Trace type: Primary "Rainfall" trace

---

#### 3️⃣ Generate visual report

```bash
python visualizer.py
```

**Or with custom paths:**
```bash
python visualizer.py --csv moata_filtered/alarm_summary.csv --out moata_filtered/viz
```

**What happens:**
* Loads and cleans `alarm_summary.csv`
* Categorizes records (threshold alarms vs recency monitoring)
* Generates comprehensive visualizations:
  * **Bar charts:** Records per gauge, alarm type distribution
  * **Histograms:** Threshold value distributions
  * **Ladder charts:** Min/max thresholds per trace (top 8 gauges)
  * **Risk scoring:** Weighted by critical flags + alarm counts
* Builds interactive HTML report with:
  * Quick navigation to all gauges
  * Visual overview (charts)
  * Top risky gauges table
  * Threshold ladder visualizations
  * Detailed data tables
* Creates per-gauge HTML pages with full alarm breakdown

⏱️ **Runtime:** < 30 seconds

📊 **Outputs:**
* `moata_filtered/viz/report.html` - **Main interactive report** 👈 Open this!
* `moata_filtered/viz/*.png` - All charts
* `moata_filtered/viz/07_gauge_pages/*.html` - Individual gauge pages
* `moata_filtered/viz/cleaned_alarm_summary.csv` - Cleaned data

👉 **Open `moata_filtered/viz/report.html` in any browser** (no server required, fully self-contained)

---

## Data Outputs Explained

### Raw Data (`moata_output/`)
* **Purpose:** Raw API responses, reproducible source of truth
* **Format:** JSON with nested structures
* **Use:** Re-run analysis without hitting API again

### Filtered Data (`moata_filtered/`)
* **Purpose:** Processed, analysis-ready datasets
* **Key file:** `alarm_summary.csv` - normalized alarm table with columns:
  * `gauge_id`, `gauge_name`, `last_data`
  * `trace_id`, `trace_name`
  * `alarm_id`, `alarm_name`, `alarm_type`
  * `threshold`, `severity`, `is_critical`
  * `source` (overflow_alarm | threshold_config | detailed_alarm | has_alarms_flag)

### Visual Reports (`moata_filtered/viz/`)
* **Purpose:** Non-technical stakeholder communication
* **Main file:** `report.html` - interactive summary with navigation
* **Charts:** PNG files showing distributions, risk, and thresholds
* **Per-gauge pages:** Detailed breakdown for each gauge

---

## Understanding Alarm Types

### 🚨 Threshold Alarms (Overflow Monitoring)
* **Purpose:** Alert when rainfall exceeds configured trigger levels
* **Example:** "15 mm in 30 minutes" or "50 mm in 24 hours"
* **Source:** `overflow_alarms` and `thresholds` API endpoints
* **Interpretation:** These define when a gauge triggers flooding alerts

### 📡 Data Freshness Alarms (Recency Monitoring)
* **Purpose:** Alert when sensor stops reporting (monitoring health)
* **Example:** "No data received in last 6 hours"
* **Source:** `detailed_alarms` with type "DataRecency"
* **Interpretation:** System monitoring, not rainfall measurement

### ⚠️ Critical Flag
* **Purpose:** Marks alarms requiring immediate attention
* **Usage:** Prioritize gauges in reports by critical count
* **Source:** `is_critical` field in alarm configurations

---

## Visual Report Features

### Main Report (`report.html`)
* **Quick navigation table** - click to view any gauge's detail page
* **Visual overview** - charts showing:
  * Top gauges by record count
  * Alarm type distribution
  * Severity breakdown
  * Critical flag counts
* **Top Risky Gauges** - weighted scoring system:
  * `risk_score = critical×3 + thresholds×2 + recency×1`
* **Threshold ladders** - min/max ranges per trace type
* **Actionable tables**:
  * Critical records (immediate attention)
  * Overflow configurations (all thresholds)
  * Recency monitoring flags

### Per-Gauge Pages (`07_gauge_pages/*.html`)
* Full alarm breakdown for each gauge
* Separated by category (critical, overflow, recency)
* Sortable tables with all metadata
* Back-link to main report

---

## Engineering Notes

### Architecture Principles
* **Modular design:** Each stage is independent and testable
* **DRY principle:** Common utilities centralized in `common/` package
* **Type safety:** Type hints throughout with `from __future__ import annotations`
* **Error handling:** Robust retry logic and graceful degradation
* **Rate limiting:** Configurable request throttling (default: 2 req/sec)

### API Client Design
* **Layered abstraction:**
  * `auth.py` - OAuth2 token lifecycle
  * `http.py` - HTTP client with retry/rate-limit
  * `client.py` - Domain methods (gauges, traces, alarms)
* **Automatic token refresh** when approaching expiry (5 min buffer)
* **Retry with backoff** for transient failures (429, 500, 502, 503, 504)
* **No async** (per Moata guidance for stability)

### Data Processing
* **Defensive parsing:** Handles missing fields, malformed data
* **Multiple alarm sources:** Merges overflow, threshold, and detailed alarms
* **Timezone-aware:** Uses `dateutil` for robust datetime parsing
* **Pandas-based:** Efficient data manipulation and CSV output

### Visualization
* **Matplotlib charts:** High-quality PNG exports (160 DPI)
* **Self-contained HTML:** No external dependencies, works offline
* **Responsive design:** Works on desktop and mobile browsers
* **Safe rendering:** HTML escaping to prevent XSS

---

## Configuration

### Environment Variables (`.env`)
```env
MOATA_CLIENT_ID=your_client_id
MOATA_CLIENT_SECRET=your_client_secret
```

### Pipeline Constants (`common/constants.py`)
```python
DEFAULT_PROJECT_ID = 594  # Auckland Council project
DEFAULT_RAIN_GAUGE_ASSET_TYPE_ID = 100
DEFAULT_REQUESTS_PER_SECOND = 2.0
INACTIVE_THRESHOLD_MONTHS = 3  # For filtering
```

### Customization
* **Inactive threshold:** Edit `INACTIVE_THRESHOLD_MONTHS` in `constants.py`
* **Exclude regions:** Modify `exclude_keyword` in `filter_active_rain_gauges.py`
* **Rate limiting:** Adjust `DEFAULT_REQUESTS_PER_SECOND` in `constants.py`
* **Chart parameters:** Edit `max_gauges_for_bars` and `top_gauges_for_ladders` in `visualizer.py`

---

## Known Limitations

* **No real-time updates:** Pipeline runs on-demand, not live monitoring
* **Permission-dependent:** Some detailed alarm endpoints require elevated API access
* **Recency thresholds:** Not always fully exposed by API, uses heuristics
* **SSL verification disabled:** Due to upstream certificate constraints (controlled environment)
* **Sequential processing:** No parallelization (API stability requirement)
* **Auckland-specific:** Filtering logic tailored for Auckland Council regions

---

## Troubleshooting

### Import Errors
```bash
# Ensure virtual environment is activated
.venv\Scripts\Activate.ps1  # Windows
source .venv/bin/activate    # macOS/Linux

# Reinstall dependencies
pip install -r requirements.txt
```

### API Authentication Errors
* Verify `.env` credentials are correct
* Check credentials haven't expired
* Ensure no trailing spaces in `.env` values

### SSL Certificate Errors
* Expected behavior, verification disabled by design
* Contact IT if warnings become errors

### Missing Output Files
* Run scripts in order: retriever → filter → visualizer
* Check for error messages in console output
* Verify `moata_output/` has JSON files before filtering

---

## Development

### Code Quality Standards
* **Type hints:** Required for all functions
* **Docstrings:** Required for public APIs
* **DRY principle:** No code duplication
* **Modular design:** Single responsibility per module
* **Error handling:** Graceful degradation with logging

### Testing
```bash
# Dry run (test imports)
python -c "from moata_pipeline import *"

# Test individual stages
python moata_data_retriever.py
python filter_active_rain_gauges.py
python visualizer.py
```

### Adding New Features
1. **New alarm types:** Extend `analyze/alarm_analysis.py`
2. **New visualizations:** Add functions to `viz/charts.py`
3. **New data sources:** Create new module under `collect/`
4. **New utilities:** Add to appropriate `common/*.py` module

---

## Changelog

### v2.0.0 – Code Quality Refactor (Dec 2024)
* ✅ **Fixed critical bug:** Added missing functions to `dataframe_utils.py`
* ✅ **Eliminated code duplication:** 
  * Created `common/file_utils.py` for `ensure_dir()`
  * Created `common/html_utils.py` for `df_to_html_table()`
  * Centralized date formatting in `time_utils.py`
* ✅ **Consolidated constants:** All API configuration in `common/constants.py`
* ✅ **Unified logging setup:** Single `logging_setup.py` module
* ✅ **Optimized performance:** Eliminated redundant threshold parsing
* ✅ **Improved maintainability:** DRY principle applied throughout
* ✅ **Enhanced documentation:** Comprehensive README and inline comments

### v1.1.0 – Pipeline Refactor (Dec 2024)
* Modular pipeline architecture
* Separated auth / HTTP / domain logic
* Added HTML visual reporting
* Locked runtime dependencies

### v1.0.0 – Initial Prototype
* Raw data collection
* Basic filtering
* CSV / JSON outputs

---

## License & Context

Developed as part of the **Auckland Council – Healthy Waters & Flood Resilience Internship** program.

**Purpose:** Support flood risk assessment and rain gauge monitoring by providing actionable insights into alarm configurations across Auckland's rain gauge network.

**Stakeholders:** 
* Healthy Waters team (operational decisions)
* Flood resilience planners (risk assessment)
* Asset management (maintenance prioritization)

---

## Support

For questions or issues:
1. Check this README for troubleshooting steps
2. Review code comments in relevant modules
3. Contact internship supervisor or Auckland Council IT support

---

## Future Enhancements

**Potential improvements:**
* [ ] Database backend for historical tracking
* [ ] Automated scheduling (cron/Task Scheduler)
* [ ] Email notifications for critical changes
* [ ] Dashboard integration (Power BI / Grafana)
* [ ] Multi-asset type support (not just rain gauges)
* [ ] Machine learning for anomaly detection
* [ ] Real-time streaming (WebSocket API if available)

---

**Last updated:** December 2024  
**Version:** 2.0.0