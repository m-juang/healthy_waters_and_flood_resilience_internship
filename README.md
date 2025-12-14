Siap — ini versi **README yang sama persis strukturnya**, tapi sudah **disisipkan bagian “ARI Alarm Validation”** (tanpa membuat README terpisah). Kamu tinggal copy–paste dan replace `README.md`.

---

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
6. **Validate ARI alarms** against ARI time-series data (Max TP108 ARI) ✅ *(new)*

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
* ✅ **ARI alarm validation (Max TP108 ARI)** *(new)*

  * Validates alarm occurrences using ARI trace time-series values
  * Produces per-alarm validation results (SUPPORTED / NOT_SUPPORTED / UNVERIFIABLE)
  * Adds trace ID + max ARI value near alarm time for auditability
* ✅ **Reproducible environment** with locked dependencies
* ✅ **Reusable utility modules** in `common/` package

---

## Project Structure

```
.
├── moata_data_retriever.py          # Entry point: collect raw data from Moata API
├── filter_active_rain_gauges.py     # Filter + analyse active Auckland gauges
├── visualizer.py                    # Generate HTML + PNG reports
├── validate_ari_alarms.py           # Validate ARI alarms using ARI trace time-series (NEW)
│
├── moata_pipeline/
│   ├── __init__.py
│   ├── logging_setup.py             # Centralized logging configuration
│   │
│   ├── common/                      # Shared utilities (DRY principle)
│   │   ├── constants.py             # API endpoints & configuration
│   │   ├── dataframe_utils.py       # DataFrame type coercion helpers
│   │   ├── file_utils.py            # File/directory operations
│   │   ├── html_utils.py            # HTML generation helpers
│   │   ├── json_io.py               # JSON read/write utilities
│   │   ├── paths.py                 # Pipeline path management
│   │   ├── text_utils.py            # Text sanitization
│   │   ├── time_utils.py            # Date/time parsing & formatting
│   │   └── typing_utils.py          # Type definitions
│   │
│   ├── moata/                       # Moata API client layer
│   │   ├── auth.py                  # OAuth2 token handling with refresh
│   │   ├── http.py                  # HTTP client with retry & rate limiting
│   │   ├── client.py                # High-level API methods
│   │   └── endpoints.py             # API endpoint definitions
│   │
│   ├── collect/                     # Stage 1: Data collection
│   │   ├── collector.py             # Rain gauge data collector
│   │   └── runner.py                # Collection orchestrator
│   │
│   ├── analyze/                     # Stage 2: Filtering & analysis
│   │   ├── alarm_analysis.py        # Alarm data extraction
│   │   ├── filtering.py             # Active gauge identification
│   │   ├── reporting.py             # Text report generation
│   │   └── runner.py                # Analysis orchestrator
│   │
│   └── viz/                         # Stage 3: Visualization
│       ├── charts.py                # Chart generation (matplotlib)
│       ├── cleaning.py              # Data cleaning for viz
│       ├── pages.py                 # Per-gauge HTML pages
│       ├── report.py                # Main HTML report builder
│       └── runner.py                # Visualization orchestrator
│
├── data/
│   └── inputs/
│       └── raingauge_ari_alarms.csv # ARI alarm log from Moata (input for validation)
│
├── moata_output/                    # Raw collected data (JSON)
│   ├── rain_gauges.json
│   └── rain_gauges_traces_alarms.json
│
├── moata_filtered/                  # Filtered & analysed data
│   ├── active_auckland_gauges.json
│   ├── alarm_summary.csv
│   ├── alarm_summary.json
│   ├── analysis_report.txt
│   └── viz/                         # Visual reports
│       ├── report.html              # Main interactive report
│       ├── cleaned_alarm_summary.csv
│       ├── 01_records_by_gauge.png
│       ├── 02_record_categories.png
│       ├── 03_severity_distribution.png
│       ├── 04_threshold_hist.png
│       ├── 05_critical_flag.png
│       ├── 06_ladder_gauge_*.png
│       └── 07_gauge_pages/
│           └── *.html
│
├── outputs/                         # Validation outputs (ARI validation)
│   ├── ari_alarm_validation_by_ari_trace.csv
│   └── figures/
│       └── top_exceedances.png      # optional viz output if enabled
│
├── .env.example
├── .gitignore
├── requirements.txt
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
* Normalizes alarm configurations (overflow + threshold config + recency flags)

⏱️ **Runtime:** < 1 minute

📊 **Outputs:**

* `moata_filtered/active_auckland_gauges.json`
* `moata_filtered/alarm_summary.csv`
* `moata_filtered/alarm_summary.json`
* `moata_filtered/analysis_report.txt`

---

#### 3️⃣ Generate visual report

```bash
python visualizer.py
```

📊 **Outputs:**

* `moata_filtered/viz/report.html` (main report)
* `moata_filtered/viz/*.png`
* `moata_filtered/viz/07_gauge_pages/*.html`

👉 **Open `moata_filtered/viz/report.html` in any browser** (no server required)

---

## ARI Alarm Validation (NEW)

This step validates ARI alarm events using the **ARI trace** time-series (virtual trace), following Moata guidance.

### Why this exists

ARI alarm events (e.g. “exceeded 5-year ARI”) are not exposed directly via Moata API.
Instead, Moata provides an **alarm log CSV** (e.g., from Teams) and you validate by:

* pulling the **ARI trace data** around alarm time
* verifying the ARI values exceed the configured threshold

### Inputs

**Alarm log CSV:**

```
data/inputs/raingauge_ari_alarms.csv
```

Expected columns include:

* `assetid`
* `name` (gauge name)
* `description` (filter to `Max TP108 ARI`)
* `createdtimeutc` (alarm timestamp)

### Run validation

```bash
python validate_ari_alarms.py
```

### What happens

For each row (alarm event):

1. Reads `assetid` and `createdtimeutc`
2. Queries Moata to find the gauge’s **ARI trace** (e.g. “Max TP108 ARI”)
3. Downloads ARI time-series data around the alarm time
4. Computes `max_ari_value`
5. Compares with `threshold` (typically 5)
6. Outputs per-event validation record

### Output

Validation result CSV:

```
outputs/ari_alarm_validation_by_ari_trace.csv
```

Typical columns:

* `assetid`
* `gauge_name`
* `alarm_time`
* `status` (SUPPORTED / NOT_SUPPORTED / UNVERIFIABLE)
* `reason` (only populated for failure/edge cases)
* `ari_trace_id`
* `max_ari_value`
* `threshold`

**Meaning of status:**

* **SUPPORTED** → max ARI value ≥ threshold (alarm is consistent with data)
* **NOT_SUPPORTED** → max ARI value < threshold (alarm not supported by data window)
* **UNVERIFIABLE** → data unavailable / trace missing / API limits / empty values

---

## Data Outputs Explained

### Raw Data (`moata_output/`)

* Purpose: raw API responses, reproducible source of truth
* Use: rerun analysis without re-hitting the API

### Filtered Data (`moata_filtered/`)

* Purpose: analysis-ready dataset
* Key file: `alarm_summary.csv`

### Visual Reports (`moata_filtered/viz/`)

* Purpose: stakeholder-friendly outputs
* Main file: `report.html`

### ARI Validation Outputs (`outputs/`)

* Purpose: verify ARI alarms against ARI trace time-series
* Key file: `ari_alarm_validation_by_ari_trace.csv`

---

## Known Limitations

* ARI traces are **virtual** → API has **32-day limit** for data windows
* Alarm events are **not directly available via API** → requires external alarm log CSV
* Some gauges may return empty windows depending on the time range or trace behaviour
* SSL verification disabled (controlled environment)
* Sequential processing only (per Moata guidance)

---

## Changelog

### v2.1.0 – ARI Alarm Validation (Dec 2025)

* ✅ Added `validate_ari_alarms.py`
* ✅ Added input support for `data/inputs/raingauge_ari_alarms.csv`
* ✅ Added output `outputs/ari_alarm_validation_by_ari_trace.csv`
* ✅ Validates ARI alarm events by comparing ARI trace time-series vs threshold

### v2.0.0 – Code Quality Refactor (Dec 2025)

* DRY refactor + utilities + constants consolidation + improved documentation

---

**Last updated:** December 2025
**Version:** 2.1.0
