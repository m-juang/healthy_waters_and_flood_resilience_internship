
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

The codebase has been **refactored into a modular pipeline architecture** to support future extensions (new asset types, additional alarm endpoints, alternative outputs).

---

## Key Features

* **Pipeline architecture** with clear separation of concerns
* **Sequential API processing** (rate-limit safe: 800 requests / 5 minutes)
* **OAuth2 client-credentials authentication**
* **Automatic retry & token refresh**
* **Active gauge filtering** (Auckland + recent data)
* **Alarm analysis**

  * Threshold alarms (overflow)
  * Data freshness alarms (recency)
* **Visual reporting**

  * Charts (PNG)
  * Interactive HTML summary
  * Per-gauge HTML pages
* **Reproducible environment**

  * Virtual environment
  * Locked `requirements.txt`

---

## Project Structure

```
.
├── moata_data_retriever.py        # Entry point: collect raw data from Moata API
├── filter_active_rain_gauges.py   # Filter + analyse active Auckland gauges
├── visualizer.py                  # Generate HTML + PNG reports
│
├── moata_pipeline/
│   ├── common/                    # Shared utilities
│   │   ├── text_utils.py
│   │   ├── io_utils.py
│   │   └── date_utils.py
│   │
│   ├── moata/                     # Moata API abstraction
│   │   ├── auth.py                # OAuth2 token handling
│   │   ├── http.py                # HTTP + retry + rate limiting
│   │   └── client.py              # Domain API client
│   │
│   ├── collect/                   # Data collection stage
│   │   ├── collector.py
│   │   └── runner.py
│   │
│   ├── analyze/                   # Filtering & aggregation
│   │   └── runner.py
│   │
│   └── viz/                       # Reporting & visualisation
│       ├── charts.py
│       ├── report.py
│       ├── gauge_pages.py
│       └── runner.py
│
├── moata_output/                  # Raw collected data (JSON)
├── moata_filtered/                # Filtered & analysed data (CSV/JSON)
├── reports/                       # HTML + PNG outputs
│   ├── report.html
│   └── 07_gauge_pages/
│
├── .env.example
├── requirements.txt               # Locked dependencies
└── README.md
```

---

## Installation

### Prerequisites

* **Python 3.10+** (tested)
* Moata API client credentials

### Setup

```bash
git clone https://github.com/m-juang/healthy_waters_and_flood_resilience_internship.git
cd healthy_waters_and_flood_resilience_internship

python -m venv .venv
.venv\Scripts\Activate.ps1   # Windows
# source .venv/bin/activate  # macOS / Linux

python -m pip install -r requirements.txt
```

### Configure credentials

```bash
cp .env.example .env
```

```env
MOATA_CLIENT_ID=your_client_id
MOATA_CLIENT_SECRET=your_client_secret
```

> **Never commit `.env`** (already excluded via `.gitignore`)

---

## Usage

### 1️⃣ Collect data from Moata API

```bash
python moata_data_retriever.py
```

**What happens:**

* Authenticates via OAuth2
* Fetches all rain gauge assets (project 594)
* Collects traces, thresholds, and alarm metadata
* Saves structured JSON to `moata_output/`

⏱️ *Runtime:* ~1 hour (sequential, rate-limit safe)

---

### 2️⃣ Filter & analyse active gauges

```bash
python filter_active_rain_gauges.py
```

**What happens:**

* Keeps Auckland gauges only
* Identifies active gauges (recent data)
* Normalises alarm records
* Produces analysis tables

⏱️ *Runtime:* < 1 minute

---

### 3️⃣ Generate visual report

```bash
python visualizer.py
```

**Outputs:**

* `reports/report.html` – main summary
* `reports/07_gauge_pages/*.html` – one page per gauge
* PNG charts for thresholds, severity, and risk

👉 Open `reports/report.html` in a browser (no server required)

---

## Data Outputs

| Directory         | Contents                           |
| ----------------- | ---------------------------------- |
| `moata_output/`   | Raw API responses                  |
| `moata_filtered/` | Filtered alarm tables (CSV / JSON) |
| `reports/`        | HTML reports + PNG charts          |

---

## Alarm Types

### Threshold (Overflow)

* Triggered when rainfall exceeds configured values
* Example: *15 mm in 30 minutes*

### Data Freshness (Recency)

* Triggered when sensor stops updating
* Indicates monitoring coverage rather than rainfall magnitude

---

## Engineering Notes

* **No async requests** (per Moata guidance)
* **Retry with backoff** for transient failures
* **Token refresh** handled automatically
* **Pipeline design** enables:

  * new asset types
  * additional alarm endpoints
  * alternative outputs (database, dashboard)

---

## Known Limitations

* Some detailed alarm endpoints require elevated permissions
* Recency alarm thresholds may not always be fully exposed by API
* SSL verification disabled due to upstream certificate constraints

---

## Changelog

### v1.1.0 – Pipeline Refactor (Dec 2025)

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

Developed as part of the **Auckland Council – Healthy Waters & Flood Resilience Internship**.

---

