# Auckland Council Rain Monitoring System

A Python pipeline for collecting, analyzing, and visualizing Auckland Council's rain monitoring data from the Moata API, including rain gauges and rain radar (QPE) data.

## Overview

This project provides tools to:
- **Collect** rain gauge and radar data from Moata API
- **Analyze** alarm configurations and calculate ARI (Annual Recurrence Interval)
- **Validate** ARI alarms against actual data
- **Visualize** results with interactive HTML dashboards
- **Generate** documentation

## Project Structure
```
internship-project/
│
├── data/
│   └── inputs/
│       ├── raingauge_ari_alarms.csv      # Historical alarm events from Sam
│       └── tp108_stats.csv               # TP108 ARI coefficients per pixel
│
├── moata_pipeline/                        # Main package
│   ├── __init__.py
│   ├── logging_setup.py
│   │
│   ├── analyze/                           # Analysis modules
│   │   ├── __init__.py
│   │   ├── alarm_analysis.py              # Rain gauge alarm analysis
│   │   ├── ari_calculator.py              # ARI calculation from radar data
│   │   ├── filtering.py                   # Gauge filtering logic
│   │   ├── radar_analysis.py              # Radar ARI batch processing
│   │   ├── reporting.py                   # Report generation
│   │   └── runner.py                      # Analysis entry points
│   │
│   ├── collect/                           # Data collection
│   │   ├── __init__.py
│   │   ├── collector.py                   # RainGaugeCollector, RadarDataCollector
│   │   └── runner.py                      # Collection entry points
│   │
│   ├── common/                            # Shared utilities
│   │   ├── __init__.py
│   │   ├── constants.py                   # API URLs, project IDs
│   │   ├── dataframe_utils.py
│   │   ├── file_utils.py
│   │   ├── html_utils.py
│   │   ├── iter_utils.py                  # chunk() function
│   │   ├── json_io.py
│   │   ├── output_writer.py
│   │   ├── paths.py                       # Output path management
│   │   ├── text_utils.py                  # safe_filename()
│   │   ├── time_utils.py                  # iso_z(), parse_datetime()
│   │   └── typing_utils.py                # safe_int(), safe_float()
│   │
│   ├── moata/                             # Moata API client
│   │   ├── __init__.py
│   │   ├── auth.py                        # OAuth2 authentication
│   │   ├── client.py                      # High-level API methods
│   │   ├── endpoints.py                   # API endpoint definitions
│   │   └── http.py                        # HTTP client with rate limiting
│   │
│   └── viz/                               # Visualization
│       ├── __init__.py
│       ├── cleaning.py                    # Rain gauge data cleaning
│       ├── pages.py                       # Per-gauge HTML pages
│       ├── radar_cleaning.py              # Radar data cleaning
│       ├── radar_report.py                # Radar HTML dashboard
│       ├── radar_runner.py                # Radar visualization runner
│       ├── report.py                      # Rain gauge HTML report
│       └── runner.py                      # Rain gauge visualization runner
│
├── outputs/                               # big file, not in repo
│   ├── documentation/
│   │   └── Rain_Monitoring_System_Documentation.docx
│   │
│   ├── rain_gauges/
│   │   ├── raw/                           # Raw API responses
│   │   ├── analyze/                       # Analysis results
│   │   ├── validation_viz/                # Validation visualizations
│   │   ├── visualizations/                # Dashboard visualizations
│   │   └── ari_alarm_validation.csv       # Validation results
│   │
│   └── rain_radar/
│       ├── raw/                           # Current (last 24h) data
│       │   ├── catchments/
│       │   ├── pixel_mappings/
│       │   ├── radar_data/
│       │   └── collection_summary.json
│       ├── analyze/                       # Current data analysis
│       ├── historical/                    # Historical data by date
│       │   └── 2025-05-09/
│       │       ├── raw/
│       │       ├── analyze/
│       │       ├── dashboard/
│       │       ├── validation_viz/
│       │       └── ari_alarm_validation.csv
│       └── visualizations/
│
├── .env                                   # Credentials (not in repo)
├── .env.example
├── .gitignore
├── README.md
├── requirements.txt
│
├── # Entry Points - Rain Gauges
├── retrieve_rain_gauges.py                # Collect rain gauge data
├── analyze_rain_gauges.py                 # Analyze and filter gauges
├── visualize_rain_gauges.py               # Generate gauge dashboard
├── validate_ari_alarms_rain_gauges.py     # Validate gauge alarms
├── visualize_ari_alarms_rain_gauges.py    # Visualize gauge validation
│
├── # Entry Points - Rain Radar
├── retrieve_rain_radar.py                 # Collect radar data
├── analyze_rain_radar.py                  # Calculate ARI from radar
├── visualize_rain_radar.py                # Generate radar dashboard
├── validate_ari_alarms_rain_radar.py      # Validate radar alarms
├── visualize_ari_alarms_rain_radar.py     # Visualize radar validation
│
└── generate_documentation.py              # Generate Word documentation
```

## Installation

### Prerequisites
- Python 3.10+
- Access to Moata API (credentials required)

### Setup
```bash
# Clone repository
git clone <repository-url>
cd internship-project

# Create virtual environment
python -m venv .venv
.venv\Scripts\Activate.ps1  # Windows PowerShell

# Install dependencies
pip install -r requirements.txt

# Create .env file
cp .env.example .env
# Edit .env with your credentials
```

## Usage

### Rain Gauge Pipeline
```bash
# 1. Collect data
python retrieve_rain_gauges.py

# 2. Analyze and filter
python analyze_rain_gauges.py

# 3. Visualize
python visualize_rain_gauges.py

# 4. Validate alarms (requires raingauge_ari_alarms.csv)
python validate_ari_alarms_rain_gauges.py

# 5. Visualize validation
python visualize_ari_alarms_rain_gauges.py
```

### Rain Radar Pipeline
```bash
# 1. Collect data
python retrieve_rain_radar.py                    # Last 24 hours
python retrieve_rain_radar.py --date 2025-05-09  # Historical date

# 2. Analyze (calculate ARI)
python analyze_rain_radar.py                     # Auto-detect data
python analyze_rain_radar.py --date 2025-05-09   # Specific date
python analyze_rain_radar.py --current           # Current data only

# 3. Visualize rainfall
python visualize_rain_radar.py --date 2025-05-09

# 4. Validate alarms
python validate_ari_alarms_rain_radar.py --date 2025-05-09

# 5. Visualize validation
python visualize_ari_alarms_rain_radar.py --date 2025-05-09
```

### Generate Documentation
```bash
python generate_documentation.py
```

## Key Concepts

### ARI (Annual Recurrence Interval)

ARI indicates how rare a rainfall event is. Calculated using TP108 formula:
```
ARI = exp(m × D + b)
```

Where:
- `D` = Rainfall depth (mm) for a duration
- `m`, `b` = Coefficients from tp108_stats.csv

### Alarm Thresholds

| Type | Threshold | Description |
|------|-----------|-------------|
| Rain Gauge | ARI ≥ 5 years | Single point exceedance |
| Rain Radar | ≥30% area with ARI ≥ 5 | Spatial proportion |

### Durations

ARI is calculated for 8 durations:
- Short: 10m, 20m, 30m
- Medium: 60m, 2h
- Long: 6h, 12h, 24h

## API Configuration

| Parameter | Value |
|-----------|-------|
| Project ID | 594 |
| Rain Gauge Asset Type | 25 |
| Stormwater Catchment Asset Type | 3541 |
| Radar Collection ID | 1 |
| Radar TraceSet ID | 3 |

## Dependencies

Core:
- `requests` - HTTP client
- `pandas` - Data manipulation
- `python-dotenv` - Environment variables
- `matplotlib` - Charts

Optional:
- `shapely` - Geometry simplification
- `python-docx` - Word document generation

## License

Internal Auckland Council use only.
