# Auckland Council Rain Monitoring System

Pipeline for collecting, analyzing, and visualizing rain monitoring data from Moata API.

> **Internship Project**: Auckland Council (COMPSCI 778)  
> **Version**: 1.0.0

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

### GUI Mode (Recommended for Beginners) 🎨

**Easy-to-use graphical interface:**

```bash
# Install GUI dependency (if not yet installed)
pip install customtkinter

# Run the GUI
python rain_monitoring_gui_modern.py
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
python retrieve_rain_gauges.py

# 2. Analyze data (~2-3 minutes)
python analyze_rain_gauges.py

# 3. Generate HTML dashboard (~3-5 minutes)
python visualize_rain_gauges.py

# 4. [Optional] Validate alarms
python validate_ari_alarms_rain_gauges.py
python visualize_ari_alarms_rain_gauges.py
```

**View all options:**
```bash
python retrieve_rain_gauges.py --help
```

### Rain Radar Pipeline

**Current Data (Last 24 hours):**
```bash
python retrieve_rain_radar.py
python analyze_rain_radar.py --current
python visualize_rain_radar.py
```

**Historical Data (Specific date):**
```bash
python retrieve_rain_radar.py --date 2025-05-09
python analyze_rain_radar.py --date 2025-05-09
python visualize_rain_radar.py --date 2025-05-09
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
├── 📁 moata_pipeline/            # Source code
│   ├── analyze/                  # ARI analysis
│   ├── collect/                  # Data collection
│   ├── common/                   # Utilities
│   ├── moata/                    # Moata API client
│   └── viz/                      # Visualization
│
├── 📁 rain_monitoring_gui/       # GUI package (modular)
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
├── 🚀 Scripts - Rain Gauges
├── retrieve_rain_gauges.py
├── analyze_rain_gauges.py
├── visualize_rain_gauges.py
├── validate_ari_alarms_rain_gauges.py
├── visualize_ari_alarms_rain_gauges.py
│
├── 🚀 Scripts - Rain Radar
├── retrieve_rain_radar.py
├── analyze_rain_radar.py
├── visualize_rain_radar.py
├── validate_ari_alarms_rain_radar.py
├── visualize_ari_alarms_rain_radar.py
│
├── 🎨 GUI Entry Point
├── rain_monitoring_gui_modern.py # GUI launcher
│
├── .env                          # Credentials (Git-ignored, REQUIRED)
├── .env.example                  # Template
├── requirements.txt              # Dependencies
└── README.md                     # This file
```

---

## Troubleshooting

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

**Solution**: Add to your script (e.g., `validate_ari_alarms_rain_gauges.py`):

```python
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
```

**Note**: This warning is safe to suppress in Auckland Council development environment.

### Error: Rate Limit Exceeded

```bash
# Wait for the specified time (usually 60 seconds)
sleep 60
python retrieve_rain_radar.py
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
python retrieve_rain_gauges.py --log-level DEBUG
```

---

## Key Concepts

### ARI (Annual Recurrence Interval)

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

# GUI (optional)
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
python retrieve_rain_gauges.py --help

# Custom threshold
python validate_ari_alarms_rain_gauges.py --threshold 10.0

# Custom time window
python validate_ari_alarms_rain_gauges.py --window-before 2 --window-after 2

# Debug mode
python analyze_rain_gauges.py --log-level DEBUG
```

### Automation with Exit Codes

```bash
python retrieve_rain_gauges.py
if [ $? -eq 0 ]; then
  echo "Success!"
  python analyze_rain_gauges.py
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

## License

**Internal Auckland Council Use Only**

Copyright © 2025-2026 Auckland Council. All rights reserved.

---

**Last Updated**: January 2025  
**Version**: 1.0.0  
**Created by**: Muhammad Juang (Healthy Waters and Flood Resilience Intern)