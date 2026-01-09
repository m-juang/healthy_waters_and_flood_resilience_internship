# Auckland Council Rain Monitoring System

Pipeline for collecting, analyzing, and visualizing rain monitoring data from Moata API.

> **Internship Project**: Auckland Council (COMPSCI 778)  
> **Version**: 2.0.0 (Updated Jan 2026)

---

## 📋 Table of Contents

- [Installation](#installation)
- [Configuration](#configuration)
- [Quick Start Guide](#quick-start-guide)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Troubleshooting](#troubleshooting)
- [Key Concepts](#key-concepts)

---

## Installation

### 1. Setup Virtual Environment

```bash
# Create virtual environment
python -m venv .venv

# Activate
# Windows PowerShell:
.\.venv\Scripts\Activate.ps1

# Windows CMD:
.venv\Scripts\activate.bat

# macOS/Linux:
source .venv/bin/activate
```

### 2. Install Dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

### 3. Configure Credentials

Create `.env` file in project root:

```bash
MOATA_CLIENT_ID=your_client_id_here
MOATA_CLIENT_SECRET=your_secret_here
```

**How to get credentials**: Contact your supervisor for Moata API OAuth2 credentials.

---

## Configuration

### `.env` File (REQUIRED)

```bash
# Moata API OAuth2
MOATA_CLIENT_ID=xxxxxxxxx
MOATA_CLIENT_SECRET=xxxxxxxxx
```

⚠️ **IMPORTANT**: Never commit `.env` to Git!

---

## Quick Start Guide

### 🎯 Workflow Overview

```
┌─────────────────────────────────────────────────────┐
│  RAIN GAUGE PIPELINE (Point Measurements)           │
├─────────────────────────────────────────────────────┤
│  1. RETRIEVE  → Collect gauge data from API         │
│                 (~120 minutes)          │
│                                                     │
│  2. ANALYZE   → Filter & analyze configurations     │
│                 (~2-3 minutes)                      │
│                                                     │
│  3. VISUALIZE → Generate HTML dashboard             │
│                 (~3-5 minutes)                      │
│                                                     │
│  4. VALIDATE  → Validate ARI alarm events           │
│                 (optional, ~5-10 minutes)           │
│                                                     │
│  5. VIZ VAL   → Validation dashboard                │
│                 (optional, <1 minute)               │
└─────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────┐
│  RAIN RADAR PIPELINE (Spatial Coverage)             │
├─────────────────────────────────────────────────────┤
│  1. RETRIEVE  → Collect radar data for catchments   │
│                 (~60 minutes)    │
│                                                     │
│  2. ANALYZE   → Calculate ARI using TP108           │
│                 (~10-15 minutes)                    │
│                                                     │
│  3. VISUALIZE → Generate HTML dashboard             │
│                 (~5-7 minutes)                      │
│                                                     │
│  4. VALIDATE  → Validate spatial alarm thresholds   │
│                 (optional, <1 minute)               │
│                                                     │
│  5. VIZ VAL   → Validation dashboard                │
│                 (optional, <1 minute)               │
└─────────────────────────────────────────────────────┘
```

### 🚀 First Time Setup

```bash
# 1. Install and activate environment
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

# 2. Create .env file with credentials
# (Get credentials from supervisor)

# 3. Run Rain Gauge pipeline (easier, faster)
python scripts/gauge/retrieve.py
python scripts/gauge/analyze.py
python scripts/gauge/visualize.py

# 4. Open dashboard
# outputs/rain_gauges/visualizations/dashboard.html
```

### 📊 Daily Usage Pattern

**Recommended workflow:**

```bash
# Morning: Collect yesterday's data
python scripts/gauge/retrieve.py --date 2025-01-09
python scripts/gauge/analyze.py
python scripts/gauge/visualize.py

# Check dashboard for any issues
# outputs/rain_gauges/visualizations/dashboard.html

# Weekly: Run radar analysis
python scripts/radar/retrieve.py --date 2025-01-09
python scripts/radar/analyze.py --date 2025-01-09
python scripts/radar/visualize.py --date 2025-01-09
```

---

## Usage

### GUI Mode (Recommended) 🎨

**Easy-to-use graphical interface:**

```bash
# Run the GUI
python moata_alert_lab.py
```

**Features:**
- ✅ Modern dark/light theme
- ✅ Step-by-step pipeline execution
- ✅ Real-time console output
- ✅ Auto-detection of latest data
- ✅ No command-line knowledge needed

**Workflow:**
1. Launch GUI
2. Select pipeline (Rain Gauge or Rain Radar)
3. Click "Run" for each step in order
4. View dashboards when complete

---

### Command-Line Mode (Advanced Users)

### Rain Gauge Pipeline

```bash
# 1. Retrieve data from API (~5-10 minutes)
python scripts/gauge/retrieve.py

# 2. Analyze data (~2-3 minutes)
python scripts/gauge/analyze.py

# 3. Generate HTML dashboard (~3-5 minutes)
python scripts/gauge/visualize.py

# 4. [Optional] Validate alarms
python scripts/gauge/validate.py
python scripts/gauge/visualize_validation.py
```

**View all options:**
```bash
python scripts/gauge/retrieve.py --help
```

### Rain Radar Pipeline

**Current Data (Last 24 hours):**
```bash
python scripts/radar/retrieve.py
python scripts/radar/analyze.py --current
python scripts/radar/visualize.py
```

**Historical Data (Specific date):**
```bash
python scripts/radar/retrieve.py --date 2025-05-09
python scripts/radar/analyze.py --date 2025-05-09
python scripts/radar/visualize.py --date 2025-05-09
```

---

## Project Structure

```
internship-project/
│
├── 📁 moata_pipeline/              # Main Python package
│   ├── __init__.py                 # Package root
│   ├── logging_setup.py            # Centralized logging
│   │
│   ├── 📁 moata/                   # API Client (5 files)
│   │   ├── __init__.py
│   │   ├── auth.py                 # OAuth2 authentication
│   │   ├── http.py                 # HTTP client with rate limiting
│   │   ├── client.py               # High-level API methods
│   │   └── endpoints.py            # API endpoint definitions
│   │
│   ├── 📁 collect/                 # Data Collection (3 files)
│   │   ├── __init__.py
│   │   ├── collector.py            # RainGaugeCollector, RadarDataCollector
│   │   └── runner.py               # Collection orchestration
│   │
│   ├── 📁 analyze/                 # Data Analysis (7 files)
│   │   ├── __init__.py
│   │   ├── runner.py               # Analysis orchestration
│   │   ├── filtering.py            # Gauge filtering logic
│   │   ├── alarm_analysis.py       # Alarm configuration analysis
│   │   ├── ari_calculator.py       # ARI calculations (TP108)
│   │   ├── reporting.py            # Text report generation
│   │   └── radar_analysis.py       # Radar-specific analysis
│   │
│   ├── 📁 viz/                     # Visualization (8 files)
│   │   ├── __init__.py
│   │   ├── runner.py               # Gauge visualization orchestration
│   │   ├── radar_runner.py         # Radar visualization orchestration
│   │   ├── cleaning.py             # Data cleaning for gauge viz
│   │   ├── pages.py                # Per-gauge HTML page generation
│   │   ├── report.py               # Main gauge dashboard HTML
│   │   ├── radar_cleaning.py       # Radar data cleaning
│   │   └── radar_report.py         # Radar dashboard HTML
│   │
│   └── 📁 common/                  # Shared Utilities (12 files)
│       ├── __init__.py
│       ├── paths.py                # Singleton path management
│       ├── constants.py            # Configuration constants
│       ├── time_utils.py           # Datetime utilities
│       ├── file_utils.py           # File operations
│       ├── json_io.py              # JSON read/write
│       ├── html_utils.py           # HTML generation helpers
│       ├── text_utils.py           # Text processing utilities
│       ├── dataframe_utils.py      # Pandas helper functions
│       ├── output_writer.py        # Output file writers
│       ├── typing_utils.py         # Type conversion utilities
│       └── iter_utils.py           # Iterator utilities
│
├── 📁 moata_alert_lab_gui/         # GUI Application (10 files)
│   ├── __init__.py                 # Package init
│   ├── __main__.py                 # Module entry point
│   ├── main.py                     # Main application window (ModernApp)
│   ├── config.py                   # Colors, constants, themes
│   ├── components.py               # Reusable UI components
│   ├── executor.py                 # Script execution handler
│   └── 📁 pipelines/               # Pipeline implementations (4 files)
│       ├── __init__.py
│       ├── base.py                 # Abstract base class (BasePipeline)
│       ├── gauge.py                # Rain Gauge pipeline implementation
│       └── radar.py                # Rain Radar pipeline implementation
│
├── 📁 scripts/                     # CLI Scripts (10 files)
│   ├── 📁 gauge/                   # Rain Gauge scripts (5 files)
│   │   ├── retrieve.py             # Data collection from API
│   │   ├── analyze.py              # Data analysis & filtering
│   │   ├── visualize.py            # HTML dashboard generation
│   │   ├── validate.py             # ARI alarm validation
│   │   └── visualize_validation.py # Validation dashboard
│   │
│   └── 📁 radar/                   # Rain Radar scripts (5 files)
│       ├── retrieve.py             # Radar data collection
│       ├── analyze.py              # ARI analysis using TP108
│       ├── visualize.py            # Radar dashboard generation
│       ├── validate.py             # Spatial alarm validation
│       └── visualize_validation.py # Validation dashboard
│
├── 📁 data/                        # Input data (static)
│   └── 📁 inputs/
│       ├── tp108_stats.csv         # TP108 ARI coefficients (REQUIRED)
│       └── raingauge_ari_alarms.csv # Historical alarm events
│
├── 📁 outputs/                     # Generated outputs (Git-ignored)
│   ├── 📁 rain_gauges/
│   │   ├── 📁 raw/                 # Raw API data (JSON)
│   │   │   ├── rain_gauges.json
│   │   │   └── rain_gauges_traces_alarms.json
│   │   ├── 📁 analyzed/            # Analysis results (CSV, TXT)
│   │   │   ├── rain_gauge_analysis_YYYYMMDD.csv
│   │   │   ├── alarm_summary.csv
│   │   │   ├── alarm_summary_full.csv
│   │   │   └── analysis_report.txt
│   │   ├── 📁 visualizations/      # HTML dashboards
│   │   │   ├── dashboard.html
│   │   │   └── 📁 gauges/
│   │   │       └── GAUGE_XXX.html
│   │   ├── ari_alarm_validation.csv
│   │   ├── 📁 validation_viz/      # Validation dashboards
│   │   │   ├── validation_dashboard.html
│   │   │   ├── validation_summary.png
│   │   │   └── top_exceedances.png
│   │   └── 📁 historical/          # Historical date-specific data
│   │       └── YYYY-MM-DD/
│   │           └── (same structure as above)
│   │
│   └── 📁 rain_radar/
│       ├── 📁 raw/                 # Current radar data
│       │   ├── catchments.json
│       │   ├── 📁 pixel_mappings/  # Catchment → pixel mappings (cached)
│       │   │   └── catchment_XXX_pixels.json (~157 files)
│       │   └── 📁 radar_data/      # Radar CSV data per catchment
│       │       └── XXXX_CatchmentName.csv (~157 files)
│       ├── 📁 analyze/              # ARI analysis results
│       │   ├── ari_analysis_summary.csv
│       │   ├── ari_exceedances.csv
│       │   └── analysis_report.txt
│       ├── 📁 dashboard/            # HTML dashboards
│       │   ├── radar_dashboard.html
│       │   ├── catchment_stats.csv
│       │   └── 📁 charts/
│       │       ├── rainfall_timeseries.png
│       │       ├── top_catchments.png
│       │       ├── ari_distribution.png
│       │       └── spatial_heatmap.png
│       ├── ari_alarm_validation.csv
│       ├── 📁 validation_viz/      # Validation dashboards
│       │   ├── validation_dashboard.html
│       │   ├── ari_distribution.png
│       │   ├── top_catchments.png
│       │   ├── proportion_distribution.png
│       │   └── validation_stats.csv
│       └── 📁 historical/          # Historical data by date
│           └── YYYY-MM-DD/
│               └── (same structure as above)
│
├── 🎨 Entry Points
├── moata_alert_lab.py          # GUI launcher
├── logging_setup.py                # Root logging config
│
├── 📄 Configuration
├── .env                            # Credentials (Git-ignored, REQUIRED)
├── .env.example                    # Template for credentials
├── requirements.txt                # Python dependencies (24 packages)
├── .gitignore                      # Git ignore rules
│
└── 📖 Documentation
    └── README.md                   # This file
```

**Total Files:**
- **Python Source**: 44 files
- **CLI Scripts**: 10 files (5 gauge + 5 radar)
- **GUI Files**: 10 files
- **Lines of Code**: ~11,100 LOC

---

## Troubleshooting

### Error: Authentication Failed

```bash
# Check if .env exists and is correct
cat .env

# Verify credentials are loaded
python -c "from dotenv import load_dotenv; import os; load_dotenv(); print(os.getenv('MOATA_CLIENT_ID'))"
```

**Solution**: Ensure `.env` file exists in project root with correct credentials.

### Error: InsecureRequestWarning

**Message:**
```
InsecureRequestWarning: Unverified HTTPS request is being made to host 'api.moata.io'
```

**Solution**: This warning is safe to suppress in Auckland Council development environment. It's already handled in the scripts.

### Error: Rate Limit Exceeded

```bash
# Wait for the specified time (usually 60 seconds)
sleep 60
python scripts/gauge/retrieve.py
```

**Tip**: Don't run multiple collections simultaneously. Moata API has rate limit of 2 requests/second.

### Error: Memory Error

**Solutions**: 
- Close other applications
- Process specific dates only (avoid large date ranges)
- Use computer with more RAM (radar analysis needs ~2GB)

### Slow Performance

**Check:**
- Network connection to `api.moata.io`
- If outputs folder is on network drive (move to local disk)
- API rate limiting (normal if slow - radar takes 15-30 min)

### View Detailed Logs

```bash
# Use debug mode
python scripts/gauge/retrieve.py --log-level DEBUG
```

---

## Key Concepts

### ARI (Average Recurrence Interval)

ARI indicates how rare a rainfall event is.
- **5-year ARI** = event occurs on average once per 5 years
- **100-year ARI** = very rare (extreme) event

**Calculation**: Uses TP108 methodology (Auckland Regional Council)

```
ARI = exp(m × D + b)
```
- D = rainfall depth (mm)
- m, b = location-specific coefficients (from `tp108_stats.csv`)

### Analyzed Durations

| Duration | Use Case |
|----------|----------|
| 10 minutes | Flash flooding |
| 20 minutes | Urban drainage |
| 30 minutes | Infrastructure capacity |
| 1 hour | Storm system design |
| 2 hours | Extended events |
| 6 hours | Multi-hour storms |
| 12 hours | Long-duration events |
| 24 hours | Multi-day storms |

### Alarm Thresholds

- **Rain Gauge**: ARI ≥ 5 years at single gauge point
- **Rain Radar**: ≥30% catchment area with ARI ≥ 5 years (spatial threshold)

### Data Sources

- **Rain Gauges**: 76 gauges across Auckland
- **Stormwater Catchments**: 233 catchments
- **Radar Pixels**: ~25k pixels total (~6-800 per catchment)

---

## Main Dependencies

```txt
# Core Data Processing
pandas==2.3.3
numpy==2.3.5

# HTTP Client & API
requests==2.32.5
urllib3==2.6.1

# Configuration
python-dotenv==1.2.1

# Date/Time Utilities
python-dateutil==2.9.0.post0

# GUI Framework
customtkinter==5.2.2

# Visualization
matplotlib==3.10.8

# Document Generation
python-docx==1.2.0
```

Install all: `pip install -r requirements.txt`

Total: **24 packages** (including dependencies)

---

## Support

| Issue | Contact |
|-------|---------|
| API credentials | Sam Greenwood (Mott MacDonald) |
| Technical questions | Kris Fordham (Systems and Insights' Manager) |
| Moata API issues | Sam (Mott MacDonald) |

---

## Tips

### CLI Arguments (All scripts support)

```bash
# View all options
python scripts/gauge/retrieve.py --help

# Historical data
python scripts/gauge/retrieve.py --date 2025-01-09

# Date range
python scripts/gauge/retrieve.py --start 2025-01-01 --end 2025-01-07

# Custom threshold
python scripts/radar/validate.py --threshold 0.50

# Debug mode
python scripts/gauge/analyze.py --log-level DEBUG
```

### Automation with Exit Codes

```bash
python scripts/gauge/retrieve.py
if [ $? -eq 0 ]; then
  echo "Success!"
  python scripts/gauge/analyze.py
else
  echo "Failed, check logs"
fi
```

Exit codes:
- `0` = success
- `1` = error
- `130` = Ctrl+C interrupted

### Best Practices

1. ✅ Always run in virtual environment
2. ✅ Never commit `.env` to Git
3. ✅ Backup `outputs/` regularly
4. ✅ Run gauge collection daily, radar weekly
5. ✅ Use `--help` to view options
6. ✅ Check logs if errors occur
7. ✅ Use historical mode for specific dates
8. ✅ Keep `data/inputs/tp108_stats.csv` updated

---

## Changelog

### Version 2.0.0 (January 2026)
- **Reorganized project structure**: Moved CLI scripts to `scripts/` folder for better organization
- **Updated GUI**: Fixed script paths to work with new structure
- **Improved maintainability**: Cleaner root folder with organized scripts
- **No functional changes**: All features work exactly as before

### Version 1.0.0 (December 2024)
- Initial release
- Rain gauge and radar data collection pipelines
- ARI analysis and alarm validation using TP108 methodology
- HTML dashboard generation with interactive visualizations
- Modern GUI interface with CustomTkinter

---

## License

**Internal Auckland Council Use Only**

Copyright © 2025-2026 Auckland Council. All rights reserved.

---

**Last Updated**: January 2026  
**Version**: 2.0.0  
**Created by**: Muhammad Juang (Healthy Waters and Flood Resilience Intern) 
**Supervisor**: Yu-Cheng Tu 
**Institution**: University of Auckland (COMPSCI 778: Internship)