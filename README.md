# MOATA AlertLab

**Comprehensive Rainfall Monitoring and Flood Alarm Validation System for Auckland Council**

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: Proprietary](https://img.shields.io/badge/license-Auckland%20Council-red.svg)](LICENSE)

MOATA AlertLab is a sophisticated rainfall monitoring application developed for Auckland Council's Healthy Waters and Flood Resilience department. The system processes both rain gauge and radar (QPE) data from the Moata API to analyze and validate flood alarms using ARI (Average Recurrence Interval) calculations and TP108 methodology.

---

## Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [Architecture](#architecture)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Pipeline Workflows](#pipeline-workflows)
- [Module Reference](#module-reference)
- [Configuration](#configuration)
- [Output Structure](#output-structure)
- [Technical Details](#technical-details)
- [Troubleshooting](#troubleshooting)
- [Development](#development)
- [License](#license)

---

## Overview

MOATA AlertLab addresses the critical need for accurate flood monitoring in Auckland's stormwater infrastructure. The system:

- **Collects** rainfall data from 264+ rain gauges and radar QPE sources covering 233 stormwater catchments
- **Analyzes** rainfall intensity using industry-standard TP108 methodology
- **Validates** alarm configurations against historical events
- **Visualizes** results through interactive HTML dashboards

### Key Metrics

| Component | Coverage |
|-----------|----------|
| Rain Gauges | 264 active stations |
| Stormwater Catchments | 233 monitored areas |
| Radar Pixels | ~15,000+ QPE data points |
| Analysis Durations | 10min, 20min, 30min, 1hr, 2hr, 6hr, 12hr, 24hr |

---

## Key Features

### Dual Data Pipeline
- **Rain Gauge Pipeline**: Individual station monitoring with trace-level alarm validation
- **Radar Pipeline**: Spatial rainfall analysis using Quantitative Precipitation Estimation (QPE)

### Intelligent Filtering
- 3-step filtering process: exclude non-Auckland gauges, require physical sensors, require recent data
- Pre-filter optimization using Sam's method for efficient API calls
- Configurable inactive threshold (default: 3 months)

### ARI-Based Alarm Analysis
- TP108 coefficient-based calculations for rainfall intensity
- Weighted ARI values across multiple durations
- Configurable thresholds (default: ARI ≥ 5 years)

### Real-Time Alarm Checking
- Checks **LATEST window only** for each duration (not entire historical period)
- 25% area threshold for radar-based catchment alarms
- Supports both current and historical data analysis

### Professional Visualizations
- Interactive HTML dashboards with search and filtering
- Per-gauge detail pages with alarm configurations
- Catchment-level radar analysis reports

---

## Architecture

```
moata_alert_lab_gui/         # Desktop GUI application
├── pipelines/               # Pipeline implementations
│   ├── base.py              # Base pipeline class
│   ├── gauge.py             # Rain gauge pipeline
│   └── radar.py             # Radar pipeline
├── __main__.py              # Entry point: python -m moata_alert_lab_gui
├── main.py                  # Main application window
├── config.py                # Theme and timeout configuration
├── executor.py              # Script execution handler
└── components.py            # Reusable UI components

moata_pipeline/              # Core processing library
├── alarms/                  # Alarm checking logic
│   ├── gauge_alarm_checker.py
│   └── radar_alarm_checker.py
├── analyze/                 # Data analysis modules
│   ├── filtering.py         # 3-step gauge filtering
│   ├── ari_calculator.py    # ARI calculations
│   ├── reporting.py         # Report generation
│   └── runner.py            # Analysis entry point
├── collect/                 # Data collection (SOLID architecture)
│   ├── collectors/          # Specialized collectors
│   │   ├── base.py          # BaseCollector
│   │   ├── asset_fetcher.py
│   │   ├── trace_fetcher.py
│   │   ├── alarm_fetcher.py
│   │   ├── catchment_fetcher.py
│   │   ├── pixel_mapper.py
│   │   ├── radar_data_fetcher.py
│   │   └── weight_calculator.py
│   ├── gauge_collector.py   # Gauge collection facade
│   ├── radar_collector.py   # Radar collection facade
│   └── runner.py            # Collection entry points
├── common/                  # Shared utilities
│   ├── config.py            # Centralized configuration
│   ├── constants.py         # API constants
│   ├── paths.py             # PipelinePaths class
│   ├── exceptions.py        # Custom exceptions
│   └── [utils].py           # Various utilities
├── moata/                   # API client layer
│   ├── auth.py              # OAuth2 authentication
│   ├── http.py              # HTTP client with rate limiting
│   ├── client.py            # Unified API client (facade)
│   ├── endpoints.py         # API endpoint definitions
│   └── clients/             # Domain-specific clients
│       ├── assets.py
│       ├── traces.py
│       ├── alarms.py
│       ├── radar.py
│       └── ari.py
└── viz/                     # Visualization generation
    ├── runner.py            # Gauge visualization entry
    ├── radar_runner.py      # Radar visualization entry
    ├── cleaning.py          # Data preparation
    ├── pages.py             # Per-gauge HTML pages
    ├── report.py            # Main gauge report
    └── radar_report.py      # Radar dashboard

scripts/                     # CLI entry points
├── gauge/
│   ├── retrieve.py
│   ├── analyze.py
│   ├── visualize.py
│   ├── validate.py
│   └── check_alarms.py
├── radar/
│   ├── retrieve.py
│   ├── analyze.py
│   └── visualize.py
└── alarms/
    ├── check_alarms.py
    ├── check_radar_alarms.py
    └── validate_ari_alarms.py

data/inputs/                 # Static reference data
├── tp108_stats.csv          # TP108 rainfall coefficients
└── raingauge_ari_alarms.csv # Historical alarm records

outputs/                     # Pipeline outputs (date-organized)
└── [see Output Structure section]
```

### Design Principles

The codebase follows **SOLID principles**:

- **Single Responsibility**: Each collector/client handles one domain
- **Open/Closed**: Easy to add new collectors without modifying existing code
- **Liskov Substitution**: All collectors implement common protocols
- **Interface Segregation**: Clients only expose relevant methods
- **Dependency Inversion**: Components depend on abstractions (protocols)

---

## Installation

### Prerequisites

- Python 3.10 or higher
- pip package manager
- Git (for cloning)

### Setup

```bash
# Clone the repository
git clone https://github.com/auckland-council/moata-alertlab.git
cd moata-alertlab

# Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Install package in development mode
pip install -e .
```

### Environment Configuration

Create a `.env` file in the project root:

```env
# Moata API Credentials (required)
MOATA_CLIENT_ID=your_client_id_here
MOATA_CLIENT_SECRET=your_client_secret_here

# Optional: Override defaults
# MOATA_PROJECT_ID=594
# MOATA_REQUESTS_PER_SECOND=2.0
```

---

## Quick Start

### Option 1: GUI Application (Desktop Interface)

The GUI provides a modern desktop interface built with CustomTkinter:

```bash
# Launch the desktop interface
python -m moata_alert_lab_gui
```

**GUI Features:**
- **Pipeline Selection**: Choose between Rain Gauge or Radar pipeline
- **Step-by-Step Execution**: Run each pipeline step with a single click
- **Date Selection**: Built-in date picker for historical data
- **Real-time Console**: View script output in embedded terminal
- **Progress Tracking**: Visual progress bars with timeout information
- **Dashboard Integration**: Auto-open generated HTML dashboards

**GUI Pipeline Steps:**
| Step | Rain Gauge | Radar |
|------|------------|-------|
| 1 | Retrieve Data | Retrieve Data |
| 2 | Analyze Data | Analyze Data |
| 3 | Visualize Results | Visualize Results |
| 4 | Validate Alarms | Check Alarms |
| 5 | Visualize Validation | - |
| 6 | Check Alarms | - |

### Option 2: Command Line Scripts

All scripts support `--help` for detailed usage information.

**Rain Gauge Pipeline:**
```bash
# Collect last 24 hours (default)
python scripts/gauge/retrieve.py

# Collect specific date (24-hour period)
python scripts/gauge/retrieve.py --date 2025-05-09

# Collect date range
python scripts/gauge/retrieve.py --start 2025-05-09 --end 2025-05-12

# Analyze collected data
python scripts/gauge/analyze.py --date 2025-05-09

# With custom inactive threshold
python scripts/gauge/analyze.py --date 2025-05-09 --inactive-months 6

# Generate visualization
python scripts/gauge/visualize.py --date 2025-05-09

# Validate alarms against API data
python scripts/gauge/validate.py --date 2025-05-09

# Check alarms at specific datetime
python scripts/gauge/check_alarms.py --datetime "2025-05-09 14:00"
```

**Radar Pipeline:**
```bash
# Collect last 24 hours (default)
python scripts/radar/retrieve.py

# Collect specific date
python scripts/radar/retrieve.py --date 2025-05-09

# Force refresh pixel mappings from API
python scripts/radar/retrieve.py --date 2025-05-09 --force-refresh-pixels

# Analyze radar data
python scripts/radar/analyze.py --date 2025-05-09

# Generate radar dashboard
python scripts/radar/visualize.py --date 2025-05-09

# Check radar alarms at specific time
python scripts/alarms/check_radar_alarms.py --time "2025-05-09 14:00:00"

# With custom thresholds
python scripts/alarms/check_radar_alarms.py --ari-threshold 10.0 --area-threshold 0.25
```

**Verbose Logging:**
```bash
# Add --log-level DEBUG to any script for detailed output
python scripts/gauge/retrieve.py --date 2025-05-09 --log-level DEBUG
```

---

## Pipeline Workflows

### Rain Gauge Pipeline

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Retrieve  │ >> │   Analyze   │ >> │  Visualize  │ >> │   Validate  │
│  (collect)  │    │  (filter)   │    │ (dashboard) │    │   (alarms)  │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
      │                  │                  │                  │
      ▼                  ▼                  ▼                  ▼
 rain_gauges_      active_gauges.    report.html      validation.csv
 traces_alarms.    csv, alarm_       gauge_pages/
 json              summary.csv
```

**Step 1: Retrieve** - Collect gauge metadata, traces, and alarm configurations
```bash
python scripts/gauge/retrieve.py --date 2025-05-09
```

**Step 2: Analyze** - Filter active gauges and generate alarm summary
```bash
python scripts/gauge/analyze.py --date 2025-05-09
```

**Step 3: Visualize** - Generate HTML dashboard with gauge pages
```bash
python scripts/gauge/visualize.py --date 2025-05-09
```

**Step 4: Validate** - Fetch timeseries and validate historical alarms
```bash
python scripts/gauge/validate.py --date 2025-05-09
```

**Step 5: Check Alarms** - Check ARI alarms with verification
```bash
python scripts/gauge/check_alarms.py --datetime "2025-05-09 14:00"
```

### Radar Pipeline

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Retrieve  │ >> │   Analyze   │ >> │  Visualize  │ >> │ Check Alarms│
│  (collect)  │    │   (ARI)     │    │ (dashboard) │    │  (realtime) │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
      │                  │                  │                  │
      ▼                  ▼                  ▼                  ▼
 catchment_        ari_analysis_    radar_           alarm_status.csv
 *.csv files       summary.csv      dashboard.html   triggered_alarms.csv
```

**Step 1: Retrieve** - Collect radar QPE data for all catchments
```bash
python scripts/radar/retrieve.py --date 2025-05-09
```

**Step 2: Analyze** - Run ARI analysis on radar data
```bash
python scripts/radar/analyze.py --date 2025-05-09
```

**Step 3: Visualize** - Generate radar dashboard
```bash
python scripts/radar/visualize.py --date 2025-05-09
```

**Step 4: Check Alarms** - Check alarm status at specific timestamp
```bash
python scripts/alarms/check_radar_alarms.py --date 2025-05-09 --time "2025-05-09 14:00:00"
```

---

## Module Reference

### `moata_pipeline.collect`

Data collection from Moata API with SOLID-compliant architecture.

```python
from moata_pipeline.collect import run_collect_rain_gauges, run_collect_radar

# Collect rain gauge data
run_collect_rain_gauges(
    start_time=datetime(2025, 5, 9, tzinfo=timezone.utc),
    end_time=datetime(2025, 5, 10, tzinfo=timezone.utc)
)

# Collect radar data
run_collect_radar(
    start_time=start_time,
    end_time=end_time,
    force_refresh_pixels=False  # Use cached pixel mappings
)
```

**Specialized Collectors:**
- `AssetFetcher`: Fetch and prepare asset data
- `TraceFetcher`: Fetch trace and timeseries data
- `AlarmFetcher`: Fetch alarm and threshold data
- `CatchmentFetcher`: Fetch stormwater catchments
- `PixelMapper`: Map radar grid to catchment geometries
- `RadarDataFetcher`: Fetch QPE timeseries
- `WeightCalculator`: Calculate pixel area weights for de-duplication

### `moata_pipeline.analyze`

Data analysis with ARI calculations and quality filtering.

```python
from moata_pipeline.analyze import run_filter_active_gauges

result = run_filter_active_gauges(
    inactive_months=3,
    exclude_keyword="test",
    input_date="2025-05-09"  # Optional: for historical data
)

print(f"Active gauges: {result['active_count']}")
print(f"Output: {result['output_dir']}")
```

**Key Components:**
- `filtering.py`: 3-step quality filtering (exclude non-Auckland, require physical, require recent)
- `ari.py`: ARI calculations using TP108 coefficients
- `reporting.py`: Generate analysis summaries and CSV reports

### `moata_pipeline.alarms`

Alarm checking for both gauge and radar data.

```python
from moata_pipeline.alarms import GaugeAlarmChecker, RadarAlarmChecker

# Check gauge alarms
gauge_checker = GaugeAlarmChecker(
    tp108_path=Path("data/inputs/tp108_stats.csv"),
    alarm_config_path=Path("data/inputs/raingauge_ari_alarms.csv")
)
result = gauge_checker.check_gauge(gauge_data, check_time)

# Check radar alarms (LATEST window only)
radar_checker = RadarAlarmChecker(
    tp108_path=Path("data/inputs/tp108_stats.csv"),
    ari_threshold=5.0,
    area_threshold=0.25  # 25% of catchment must exceed
)
result = radar_checker.check_catchment_at_time(catchment_df, check_time)
```

**Important**: Radar alarm checking examines only the **LATEST window** for each duration, not the entire historical period. This matches operational requirements.

### `moata_pipeline.viz`

HTML dashboard generation.

```python
from moata_pipeline.viz import run_visual_report

report_path = run_visual_report(
    csv_path=Path("outputs/rain_gauges/.../analysis/alarm_summary.csv"),
    out_dir=Path("outputs/rain_gauges/.../visualizations"),
    input_date="2025-05-09"
)
print(f"Dashboard: {report_path}")
```

### `moata_pipeline.moata`

Low-level API client with OAuth2 authentication.

```python
from moata_pipeline.moata import MoataAuth, MoataHttp, MoataClient

# Create authenticated client
auth = MoataAuth(
    token_url="https://login.moata.io/connect/token",
    scope="mapi offline_access",
    client_id="your_id",
    client_secret="your_secret"
)

http = MoataHttp(
    get_token_fn=auth.get_token,
    base_url="https://api.moata.io/ae/v1",
    requests_per_second=2.0
)

client = MoataClient(http=http)

# Use domain-specific clients
gauges = client.assets.get_rain_gauges(project_id=594, asset_type_id=100)
traces = client.traces.get_traces_for_asset(asset_id=12345)
alarms = client.alarms.get_alarms_for_trace(trace_id=67890)
ari_data = client.ari.get_ari_data(trace_id=67890, from_time=..., to_time=...)
```

### `moata_alert_lab_gui`

Desktop application with CustomTkinter for interactive pipeline execution.

```bash
# Launch GUI
python -m moata_alert_lab_gui
```

**Module Structure:**
- `main.py` - Main application window and navigation
- `config.py` - Theme colors, timeouts, and settings
- `executor.py` - Script execution with real-time output
- `components.py` - Reusable UI components (cards, buttons, dialogs)
- `pipelines/base.py` - Base pipeline class
- `pipelines/gauge.py` - Rain gauge pipeline implementation
- `pipelines/radar.py` - Radar pipeline implementation

**Features:**
- Modern dark/light theme with toggle
- Pipeline selection cards with feature highlights
- Step-by-step execution with numbered workflow
- Date picker dialogs for historical data
- Real-time console output with auto-scroll
- Timeout handling per script type
- Automatic dashboard opening on completion

---

## Configuration

### Constants (`moata_pipeline/common/constants.py`)

| Constant | Default | Description |
|----------|---------|-------------|
| `PROJECT_ID` | 594 | Auckland Council Moata project |
| `RAIN_GAUGE_ASSET_TYPE_ID` | 100 | Asset type for rain gauges |
| `STORMWATER_CATCHMENT_ASSET_TYPE_ID` | 3541 | Asset type for catchments |
| `DEFAULT_REQUESTS_PER_SECOND` | 2.0 | API rate limit |
| `INACTIVE_THRESHOLD_MONTHS` | 3 | Inactive gauge threshold |
| `DEFAULT_ARI_THRESHOLD` | 5.0 | ARI alarm threshold (years) |
| `DEFAULT_RADAR_PROPORTION_THRESHOLD` | 0.25 | 25% area threshold |
| `RADAR_MAX_PIXELS_PER_REQUEST` | 150 | API batch limit |
| `RADAR_RECOMMENDED_BATCH_SIZE` | 50 | Recommended batch size |

### TP108 Coefficients

Located in `data/inputs/tp108_stats.csv`, containing:
- Auckland-specific rainfall intensity coefficients
- Duration categories: 10min to 24hr
- Used for ARI calculations

---

## Output Structure

```
outputs/
├── rain_gauges/
│   └── YYYYMMDD-YYYYMMDD/           # Date range folder
│       ├── raw/
│       │   └── rain_gauges_traces_alarms.json
│       ├── analysis/
│       │   ├── active_gauges.csv
│       │   ├── alarm_summary.csv
│       │   └── alarm_summary_full.csv
│       ├── visualizations/
│       │   ├── report.html
│       │   ├── cleaned_alarm_summary.csv
│       │   └── gauge_pages/
│       │       └── *.html
│       └── validation/
│           └── ari_alarm_validation.csv
│
└── rain_radar/
    └── YYYYMMDD-YYYYMMDD/
        ├── raw/
        │   ├── radar_data/
        │   │   └── {catchment_id}_{name}.csv
        │   ├── pixel_mappings.json
        │   └── pixel_weights.json
        ├── analysis/
        │   └── catchment_analysis.csv
        ├── alarms/
        │   ├── alarm_status.csv
        │   ├── triggered_alarms.csv
        │   └── alarm_report.txt
        └── visualizations/
            ├── radar_dashboard.html
            └── catchment_stats.csv
```

---

## Technical Details

### ARI (Average Recurrence Interval) Calculation

ARI represents the statistical return period of rainfall events in years. The system uses TP108 methodology:

1. **Extract rainfall totals** for each standard duration (10min, 20min, 30min, 1hr, 2hr, 6hr, 12hr, 24hr)
2. **Apply TP108 coefficients** specific to Auckland region
3. **Calculate weighted ARI** across all durations
4. **Compare against threshold** (default: 5-year return period)

```python
# Simplified ARI calculation
ari = calculate_ari(
    rainfall_mm=15.5,
    duration_minutes=60,
    tp108_coefficients=coefficients
)
# Returns: ARI in years (e.g., 2.5 means 2.5-year return period)
```

### Pixel Weighting for Radar Data

When radar pixels overlap multiple catchments, weighted values prevent double-counting:

```python
# Geometric weighting (preferred)
weight = intersection_area / pixel_area

# Simple weighting (fallback)
weight = 1.0 / number_of_overlapping_catchments
```

### Atomic Write Strategy

All file operations use atomic writes to prevent data corruption:

1. Write to temporary directory (`_temp/`)
2. Validate output integrity
3. Move to final location only on success
4. Windows file locking handled with retry logic

### Pre-Filter Optimization

Sam's optimization reduces API calls by identifying active gauges before full data fetch:

```python
# Uses /projects/{id}/traces/info endpoint
# Filters by: data_variable_type_id=10 (rainfall), inactive_months=3
# Excludes: "northland|waikato" patterns
```

---

## Troubleshooting

### Common Issues

**1. Authentication Error**
```
AuthenticationError: Failed to acquire token
```
**Solution**: Check `.env` file has valid `MOATA_CLIENT_ID` and `MOATA_CLIENT_SECRET`

**2. No Data Found**
```
FileNotFoundError: Raw gauge data not found
```
**Solution**: Run `retrieve.py` before `analyze.py`

**3. Rate Limit Exceeded**
```
RateLimitError: Rate limit exceeded
```
**Solution**: Reduce `MOATA_REQUESTS_PER_SECOND` in configuration

**4. SSL Certificate Error**
```
SSLError: certificate verify failed
```
**Solution**: The code disables SSL verification by default for corporate networks. If needed, set `verify_ssl=True` in auth/http configuration.

### Debug Mode

Enable verbose logging:
```bash
python scripts/gauge/retrieve.py --log-level DEBUG
```

---

## Development

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=moata_pipeline

# Run specific module tests
pytest tests/test_analyze.py -v
```

### Code Style

```bash
# Format code
black moata_pipeline/

# Check types
mypy moata_pipeline/

# Lint
flake8 moata_pipeline/
```

### Adding New Collectors

Follow the SOLID pattern:

```python
from moata_pipeline.collect.collectors.base import BaseCollector

class MyNewCollector(BaseCollector):
    """Single responsibility: collect specific data type."""
    
    def collect(self, **kwargs) -> dict:
        # Implementation
        pass
```

---

## License

This software is proprietary to Auckland Council. Developed as part of COMPSCI 778 internship program at the University of Auckland.

**Author**: Auckland Council Internship Team (COMPSCI 778)  
**Contact**: mott909@aucklanduni.ac.nz  
**Version**: 2.1.0  
**Last Modified**: January 2026

---

## Acknowledgments

- **Auckland Council Healthy Waters** - Project sponsorship and domain expertise
- **Kris Fordham** - Supervisor and technical guidance
- **Sam (Moata Team)** - API documentation and optimization suggestions
- **University of Auckland** - COMPSCI 778 program coordination