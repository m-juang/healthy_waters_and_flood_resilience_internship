# Auckland Council Rain Monitoring System

Pipeline for collecting, analyzing, and visualizing rain monitoring data from Moata API.

> **Internship Project**: Auckland Council (COMPSCI 778)  
> **Version**: 2.0.0 (Updated Jan 2026)

---

## 📋 Table of Contents

- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Troubleshooting](#troubleshooting)

---

## Installation

### 1. Setup Virtual Environment

```bash
# Create virtual environment
python -m venv .venv

# Activate
# Windows PowerShell:
.venv\Scripts\Activate.ps1

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

## Usage

### GUI Mode (Recommended) 🎨

**Easy-to-use graphical interface:**

```bash
# Run the GUI
python rain_monitoring_gui.py
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
# 1. Retrieve data from API (~60 minutes)
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
├── 📁 data/inputs/               # Reference data
│   ├── raingauge_ari_alarms.csv
│   └── tp108_stats.csv
│
├── 📁 moata_pipeline/            # Source code (backend)
│   ├── analyze/                  # ARI analysis
│   ├── collect/                  # Data collection
│   ├── common/                   # Utilities
│   ├── moata/                    # Moata API client
│   └── viz/                      # Visualization
│
├── 📁 rain_monitoring_gui/       # GUI application
│   ├── __init__.py               # Package init
│   ├── __main__.py               # Module entry point
│   ├── config.py                 # Colors, constants, themes
│   ├── components.py             # Reusable UI components
│   ├── executor.py               # Script execution handler
│   ├── main.py                   # Main application window
│   └── pipelines/                # Pipeline implementations
│       ├── __init__.py
│       ├── base.py               # Abstract base class
│       ├── gauge.py              # Rain Gauge pipeline
│       └── radar.py              # Rain Radar pipeline
│
├── 📁 scripts/                   # Command-line scripts
│   ├── gauge/                    # Rain gauge pipeline scripts
│   │   ├── retrieve.py           # Data collection
│   │   ├── analyze.py            # Data analysis
│   │   ├── visualize.py          # Dashboard generation
│   │   ├── validate.py           # Alarm validation
│   │   └── visualize_validation.py  # Validation dashboard
│   └── radar/                    # Rain radar pipeline scripts
│       ├── retrieve.py           # Data collection
│       ├── analyze.py            # Data analysis
│       ├── visualize.py          # Dashboard generation
│       ├── validate.py           # Alarm validation
│       └── visualize_validation.py  # Validation dashboard
│
├── 📁 outputs/                   # Generated outputs (Git-ignored)
│   ├── rain_gauges/
│   │   ├── raw/                  # Raw API data
│   │   ├── analyze/              # Analysis results
│   │   └── visualizations/       # HTML dashboard
│   │
│   └── rain_radar/
│       ├── raw/                  # Current radar data
│       └── historical/           # Historical data by date
│           └── YYYY-MM-DD/
│               ├── raw/          # Raw radar data
│               ├── analyze/      # Analysis results
│               └── dashboard/    # HTML dashboard
│
├── 🎨 Main Entry Point
├── rain_monitoring_gui.py        # GUI launcher
│
├── .env                          # Credentials (Git-ignored, REQUIRED)
├── .env.example                  # Template
├── requirements.txt              # Dependencies
└── README.md                     # This file
```

---

## Troubleshooting

### Error: ModuleNotFoundError: No module named 'moata_pipeline'

**Cause**: Scripts moved to `scripts/` folder can't find `moata_pipeline` module.

**Solution**: Scripts have been updated with path fixing code. If you still see this error, ensure you're running the latest version of the scripts.

**Manual fix** (if needed):
Add this at the top of the script (after docstring, before other imports):
```python
import sys
from pathlib import Path
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))
```

### Error: Authentication Failed

```bash
# Check if .env exists and is correct
cat .env

# Verify credentials are loaded
python -c "from dotenv import load_dotenv; import os; load_dotenv(); print(os.getenv('MOATA_CLIENT_ID'))"
```

**Solution**: Ensure `.env` file exists with correct credentials.

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

**Tip**: Don't run multiple collections simultaneously.

### Error: Memory Error

**Solutions**: 
- Close other applications
- Process specific dates only (avoid large date ranges)
- Use computer with more RAM

### Slow Performance

**Check:**
- Network connection to `api.moata.io`
- If outputs folder is on network drive (move to local disk)
- API rate limiting (normal if slow)

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

### Example Analyzed Durations

| Duration | Use Case |
|----------|----------|
| 10 minutes | Flash flooding |
| 1 hour | Infrastructure design |
| 24 hours | Multi-day storms |

### Alarm Thresholds

- **Rain Gauge**: ARI ≥ 5 years at single gauge
- **Rain Radar**: ≥30% catchment area with ARI ≥ 5 years

---

## Main Dependencies

```txt
# Core
requests>=2.31.0           # HTTP client
pandas>=2.1.0              # Data processing
matplotlib>=3.8.0          # Visualization
python-dotenv>=1.0.0       # Environment variables
shapely>=2.0.0             # Geometry (for radar)

# GUI
customtkinter>=5.2.0       # Modern GUI interface
```

Install all: `pip install -r requirements.txt`

---

## Support

| Issue | Contact |
|-------|---------|
| API credentials | Sam (Mott MacDonald) |
| Data questions | Sam (Mott MacDonald) |

---

## Tips

### CLI Arguments (All scripts support)

```bash
# View all options
python scripts/gauge/retrieve.py --help

# Custom threshold
python scripts/gauge/validate.py --threshold 10.0

# Custom time window
python scripts/gauge/validate.py --window-before 2 --window-after 2

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
3. ✅ Backup `outputs/` to OneDrive
4. ✅ Run gauge collection daily, radar weekly
5. ✅ Use `--help` to view options
6. ✅ Check logs if errors occur

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
- ARI analysis and alarm validation
- HTML dashboard generation
- Modern GUI interface

---

## License

**Internal Auckland Council Use Only**

Copyright © 2024-2026 Auckland Council. All rights reserved.

---

**Last Updated**: January 2026  
**Version**: 2.0.0  
**Created by**: Muhammad Juang (Healthy Waters and Flood Resilience Intern)