# 📘 **MOATA AlertLab - TECHNICAL SUMMARY**

**Version**: 2.0.0  
**Last Updated**: January 2026  
**Author**: Muhammad Juang (COMPSCI 778 Internship)

---

## 📋 **TABLE OF CONTENTS**

1. [System Overview](#1-system-overview)
2. [Software Architecture](#2-software-architecture)
3. [Rain Gauge Pipeline - Detailed Flow](#3-rain-gauge-pipeline---detailed-flow)
4. [Rain Radar Pipeline - Detailed Flow](#4-rain-radar-pipeline---detailed-flow)
5. [File Dependencies Map](#5-file-dependencies-map)
6. [Data Flow Diagrams](#6-data-flow-diagrams)
7. [Key Design Patterns](#7-key-design-patterns)

---

## 1️⃣ **SYSTEM OVERVIEW**

### **Purpose**
Automated pipeline for collecting, analyzing, and visualizing rainfall data from Moata API for Auckland Council's flood monitoring operations.

### **Key Features**
- 🌧️ **Dual Pipeline**: Rain Gauge (point measurements) + Rain Radar (spatial coverage)
- 📊 **ARI Analysis**: Average Recurrence Interval calculation using TP108 methodology
- 🎨 **Interactive Dashboards**: HTML-based visualization with charts
- ✅ **Alarm Validation**: Verify ARI alarm events against actual data
- 🖥️ **GUI + CLI**: User-friendly interface + powerful command-line tools

### **Technology Stack**
- **Language**: Python 3.10+
- **Data Processing**: Pandas, NumPy
- **Visualization**: Matplotlib, HTML/CSS/JavaScript
- **GUI**: CustomTkinter
- **API Client**: Requests (OAuth2)
- **Architecture**: Layered (5 layers)

### **Statistics**
- **Total Files**: 44 Python source files
- **Lines of Code**: ~11,100 LOC
- **CLI Scripts**: 10 (5 gauge + 5 radar)
- **GUI Components**: 10 files
- **Dependencies**: 24 packages

---

## 2️⃣ **SOFTWARE ARCHITECTURE**

### **High-Level Architecture**

```
┌────────────────────────────────────────────────────────────────────┐
│                       PRESENTATION LAYER                           │
│  ┌──────────────────────┐         ┌──────────────────────┐         │
│  │   CLI Scripts        │         │   GUI Application    │         │
│  │  (10 scripts)        │         │  (CustomTkinter)     │         │
│  └──────────┬───────────┘         └──────────┬───────────┘         │
└─────────────┼────────────────────────────────┼─────────────────────┘
              │                                │
              ▼                                ▼
┌──────────────────────────────────────────────────────────────────┐
│                    ORCHESTRATION LAYER                           │
│  ┌────────────────┐  ┌────────────────┐  ┌───────────────┐       │
│  │ Collection     │  │ Analysis       │  │ Visualization │       │
│  │ Runners        │  │ Runners        │  │ Runners       │       │
│  └────────┬───────┘  └────────┬───────┘  └───────┬───────┘       │
└───────────┼───────────────────┼──────────────────┼───────────────┘
            │                   │                  │
            ▼                   ▼                  ▼
┌───────────────────────────────────────────────────────────────────┐
│                     BUSINESS LOGIC LAYER                          │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                │
│  │ Collectors  │  │ Analyzers   │  │ Visualizers │                │
│  │ • Gauge     │  │ • Filtering │  │ • Cleaning  │                │
│  │ • Radar     │  │ • ARI Calc  │  │ • Report    │                │
│  └────────┬────┘  └─────────┬───┘  └─────────┬───┘                │
└───────────┼─────────────────┼────────────────┼────────────────────┘
            │                 │                │
            ▼                 ▼                ▼
┌────────────────────────────────────────────────────────────────────┐
│                      DATA ACCESS LAYER                             │
│  ┌────────────────┐           ┌────────────────┐                   │
│  │ MOATA API      │           │ File I/O       │                   │
│  │ Client         │           │ (JSON/CSV)     │                   │
│  └────────┬───────┘           └────────┬───────┘                   │
└───────────┼────────────────────────────┼───────────────────────────┘
            │                            │
            ▼                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      UTILITIES LAYER                                │
│  • Paths (Singleton)  • Constants  • Time Utils  • DataFrame Utils  │
│  • Text Utils  • HTML Utils  • JSON I/O  • Output Writers           │
└─────────────────────────────────────────────────────────────────────┘
            │                              │
            ▼                              ▼
  ┌──────────────┐              ┌──────────────┐
  │  Moata API   │              │ File System  │
  │  (External)  │              │  outputs/    │
  └──────────────┘              └──────────────┘
```

### **Layered Architecture Details**

| Layer | Components | Responsibility |
|-------|-----------|----------------|
| **Layer 1: Presentation** | CLI Scripts (10), GUI (10 files) | User interaction, input validation |
| **Layer 2: Orchestration** | Runners (collection, analysis, viz) | Coordinate multi-step workflows |
| **Layer 3: Business Logic** | Collectors, Analyzers, Visualizers | Core algorithms and processing |
| **Layer 4: Data Access** | API Client (5 files), File I/O | External data communication |
| **Layer 5: Utilities** | Common utilities (12 files) | Shared helper functions |

### **Module Structure**

```
moata_pipeline/
├── moata/          (5 files)  - API Client
├── collect/        (3 files)  - Data Collection
├── analyze/        (7 files)  - Data Analysis
├── viz/            (8 files)  - Visualization
└── common/         (12 files) - Utilities
```

---

## 3️⃣ **RAIN GAUGE PIPELINE - DETAILED FLOW**

### **STAGE 1: DATA COLLECTION (RETRIEVE)**

#### **Entry Point:** `scripts/gauge/retrieve.py`

```
┌──────────────────────────────────────────────────────────────┐
│ USER INPUT                                                   │
│ Command: python scripts/gauge/retrieve.py [--date YYYY-MM-DD]│
└──────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ scripts/gauge/retrieve.py                                   │
│ • parse_args()                                              │
│ • Validate date inputs                                      │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ moata_pipeline/logging_setup.py                             │
│ • setup_logging(level)                                      │
│ • Configure console output                                  │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ moata_pipeline/collect/runner.py                            │
│ • run_collect_rain_gauges(start_time, end_time)             │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ moata_pipeline/common/paths.py                              │
│ • PipelinePaths.get_instance() [SINGLETON]                  │
│ • Determine output directory                                │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ .env file                                                   │
│ • Load MOATA_CLIENT_ID                                      │
│ • Load MOATA_CLIENT_SECRET                                  │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ moata_pipeline/common/constants.py                          │
│ • TOKEN_URL, BASE_API_URL                                   │
│ • PROJECT_ID = 594                                          │
│ • RAIN_GAUGE_ASSET_TYPE_ID = 100                            │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ CREATE API CLIENT                                           │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ moata_pipeline/moata/auth.py                                │
│ • MoataAuth(token_url, client_id, client_secret)            │
│ • get_token() → OAuth2 token with caching                   │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ moata_pipeline/moata/http.py                                │
│ • MoataHttp(get_token_fn, base_url)                         │
│ • Rate limiting: 2 requests/second                          │
│ • Retry logic with exponential backoff                      │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ moata_pipeline/moata/endpoints.py                           │
│ • get_assets_endpoint()                                     │
│ • get_traces_endpoint()                                     │
│ • get_alarms_endpoint()                                     │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ moata_pipeline/moata/client.py                              │
│ • MoataClient(http)                                         │
│ • get_rain_gauges()                                         │
│ • get_traces()                                              │
│ • get_alarms()                                              │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ moata_pipeline/collect/collector.py                         │
│ • RainGaugeCollector(client)                                │
│ • collect(project_id, asset_type_id)                        │
│                                                             │
│ PROCESS:                                                    │
│ 1. Fetch all rain gauges (60 gauges)                        │
│ 2. For each gauge:                                          │
│    a) Fetch traces                                          │
│    b) Fetch alarms/thresholds                               │
│    c) Build combined data structure                         │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ UTILITIES USED                                              │
│ • moata_pipeline/common/time_utils.py                       │
│   - parse_datetime(), iso_z()                               │
│ • moata_pipeline/common/text_utils.py                       │
│   - safe_filename()                                         │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ SAVE OUTPUT                                                 │
│ • moata_pipeline/common/output_writer.py                    │
│   - JsonOutputWriter.write_rain_gauges()                    │
│   - JsonOutputWriter.write_combined()                       │
│ • moata_pipeline/common/json_io.py                          │
│   - write_json()                                            │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ OUTPUT FILES                                                │
│ outputs/rain_gauges/raw/                                    │
│ ├── rain_gauges.json                                        │
│ └── rain_gauges_traces_alarms.json                          │
└─────────────────────────────────────────────────────────────┘
```

**Files Involved (Stage 1):**
- Entry: `scripts/gauge/retrieve.py`
- Orchestration: `moata_pipeline/collect/runner.py`
- Logic: `moata_pipeline/collect/collector.py`
- API: `moata_pipeline/moata/` (5 files)
- Utilities: `moata_pipeline/common/` (6 files)
- Config: `.env`, `moata_pipeline/common/constants.py`

---

### **STAGE 2: DATA ANALYSIS (ANALYZE)**

#### **Entry Point:** `scripts/gauge/analyze.py`

```
┌─────────────────────────────────────────────────────────────┐
│ USER INPUT                                                  │
│ Command: python scripts/gauge/analyze.py                    │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ scripts/gauge/analyze.py                                    │
│ • parse_args()                                              │
│ • --inactive-months (default: 3)                            │
│ • --exclude-keyword (default: "northland|waikato")          │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ moata_pipeline/analyze/runner.py                            │
│ • run_filter_active_gauges()                                │
│ ORCHESTRATION:                                              │
│ 1. Load raw data                                            │
│ 2. Filter gauges                                            │
│ 3. Analyze alarms                                           │
│ 4. Generate reports                                         │
│ 5. Save outputs                                             │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ LOAD INPUT                                                  │
│ • moata_pipeline/common/json_io.py                          │
│   - read_json_maybe_wrapped()                               │
│ INPUT: outputs/rain_gauges/raw/                             │
│        rain_gauges_traces_alarms.json                       │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ moata_pipeline/analyze/filtering.py                         │
│ • FilterConfig(inactive_months, exclude_keyword)            │
│ • filter_gauges(all_data, config)                           │
│                                                             │
│ FILTERING LOGIC:                                            │
│ 1. Exclude by location (non-Auckland)                       │
│    - is_auckland_gauge()                                    │
│ 2. Find primary rainfall trace                              │
│    - get_rainfall_trace()                                   │
│ 3. Check recency (last 3 months)                            │
│    - is_gauge_active()                                      │
│                                                             │
│ OUTPUT CATEGORIES:                                          │
│ • active_gauges                                             │
│ • inactive_gauges                                           │
│ • excluded_gauges                                           │
│ • no_rainfall_trace                                         │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ moata_pipeline/common/time_utils.py                         │
│ • parse_datetime()                                          │
│ • months_ago()                                              │
│ • now_like()                                                │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ moata_pipeline/analyze/alarm_analysis.py                    │
│ • analyze_alarms(active_gauges)                             │
│                                                             │
│ CREATES HETEROGENEOUS DATAFRAME:                            │
│ Row Types:                                                  │
│ 1. derived_recency (per gauge)                              │
│ 2. trace_inventory (per trace)                              │
│ 3. threshold_config (per threshold)                         │
│ 4. alarm_inventory (per alarm type)                         │
│                                                             │
│ OUTPUT:                                                     │
│ • all_traces_df (complete)                                  │
│ • alarms_only_df (filtered)                                 │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ moata_pipeline/analyze/reporting.py                         │
│ • create_summary_report()                                   │
│                                                             │
│ REPORT SECTIONS:                                            │
│ 1. Filtering results summary                                │
│ 2. Active gauge details                                     │
│ 3. Alarm & threshold configuration summary                  │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ SAVE OUTPUTS                                                │
│ • moata_pipeline/common/output_writer.py                    │
│   - CsvOutputWriter.write_csv()                             │
│   - TextOutputWriter.write_report()                         │
│ • moata_pipeline/common/dataframe_utils.py                  │
│   - ensure_columns(), coerce_bool_series()                  │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ OUTPUT FILES                                                │
│ outputs/rain_gauges/analyzed/                               │
│ ├── rain_gauge_analysis_YYYYMMDD.csv                        │
│ ├── alarm_summary.csv                                       │
│ ├── alarm_summary_full.csv                                  │
│ └── analysis_report.txt                                     │
└─────────────────────────────────────────────────────────────┘
```

**Files Involved (Stage 2):**
- Entry: `scripts/gauge/analyze.py`
- Orchestration: `moata_pipeline/analyze/runner.py`
- Logic: `moata_pipeline/analyze/` (filtering.py, alarm_analysis.py, reporting.py)
- Utilities: `moata_pipeline/common/` (7 files)

---

### **STAGE 3: VISUALIZATION (VISUALIZE)**

#### **Entry Point:** `scripts/gauge/visualize.py`

```
┌─────────────────────────────────────────────────────────────┐
│ USER INPUT                                                  │
│ Command: python scripts/gauge/visualize.py                  │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ scripts/gauge/visualize.py                                  │
│ • parse_args()                                              │
│ • validate_csv_path()                                       │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ moata_pipeline/viz/runner.py                                │
│ • run_visual_report(csv_path, out_dir)                      │
│ ORCHESTRATION:                                              │
│ 1. Auto-detect input CSV                                    │
│ 2. Load and clean data                                      │
│ 3. Generate per-gauge pages                                 │
│ 4. Generate main dashboard                                  │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ LOAD INPUT                                                  │
│ INPUT: outputs/rain_gauges/analyzed/                        │
│        rain_gauge_analysis_YYYYMMDD.csv                     │
│ Using: pandas.read_csv()                                    │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ moata_pipeline/viz/cleaning.py                              │
│ • clean_dataframe(df)                                       │
│                                                             │
│ CLEANING STEPS:                                             │
│ 1. Handle heterogeneous source column                       │
│ 2. Type conversions (bool, numeric, datetime)               │
│ 3. Text normalization                                       │
│ 4. Column validation                                        │
│                                                             │
│ Uses: moata_pipeline/common/dataframe_utils.py              │
│   - to_bool_series(), to_numeric_series()                   │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ moata_pipeline/viz/pages.py                                 │
│ • create_gauge_pages(df, out_dir)                           │
│                                                             │
│ FOR EACH GAUGE:                                             │
│ 1. Filter data for gauge                                    │
│ 2. Extract metadata                                         │
│ 3. Generate HTML page                                       │
│ 4. Save to gauges/GAUGE_XXX.html                            │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ moata_pipeline/viz/report.py                                │
│ • create_main_report(df, out_dir, gauge_pages)              │
│                                                             │
│ DASHBOARD SECTIONS:                                         │
│ 1. Header & timestamp                                       │
│ 2. Summary statistics                                       │
│ 3. Interactive map (Leaflet.js)                             │
│ 4. Gauge list table (sortable)                              │
│ 5. Alarm summary charts                                     │
│ 6. Footer                                                   │
│                                                             │
│ LIBRARIES:                                                  │
│ • Bootstrap CSS                                             │
│ • Leaflet.js (maps)                                         │
│ • Chart.js (charts)                                         │
│ • DataTables (tables)                                       │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ moata_pipeline/common/html_utils.py                         │
│ • df_to_html_table()                                        │
│ • create_html_page()                                        │
│ • create_dashboard_section()                                │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ OUTPUT FILES                                                │
│ outputs/rain_gauges/visualizations/                         │
│ ├── dashboard.html                                          │
│ └── gauges/                                                 │
│     ├── GAUGE_1234.html                                     │
│     ├── GAUGE_1235.html                                     │
│     └── ... (~60 files)                                     │
└─────────────────────────────────────────────────────────────┘
```

**Files Involved (Stage 3):**
- Entry: `scripts/gauge/visualize.py`
- Orchestration: `moata_pipeline/viz/runner.py`
- Logic: `moata_pipeline/viz/` (cleaning.py, pages.py, report.py)
- Utilities: `moata_pipeline/common/` (html_utils.py, dataframe_utils.py)

---

### **STAGE 4: VALIDATION (VALIDATE)**

#### **Entry Point:** `scripts/gauge/validate.py`

```
┌─────────────────────────────────────────────────────────────┐
│ USER INPUT                                                  │
│ Command: python scripts/gauge/validate.py                   │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ scripts/gauge/validate.py                                   │
│ HARDCODED SETTINGS:                                         │
│ • INPUT_CSV: data/inputs/raingauge_ari_alarms.csv           │
│ • TRACE_MAPPING_CSV: alarm_summary_full.csv                 │
│ • ARI_THRESHOLD: 5.0 years                                  │
│ • WINDOW: 1 hour before/after                               │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ CREATE API CLIENT (same as Stage 1)                         │
│ • MoataAuth, MoataHttp, MoataClient                         │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ LOAD INPUTS                                                 │
│ 1. Historical alarms:                                       │
│    data/inputs/raingauge_ari_alarms.csv                     │
│ 2. Trace mappings:                                          │
│    outputs/rain_gauges/analyzed/alarm_summary_full.csv      │
│                                                             │
│ • build_trace_mapping()                                     │
│   Creates: asset_id → trace_id mapping                      │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ VALIDATION LOOP                                             │
│ FOR EACH ALARM EVENT:                                       │
│                                                             │
│ 1. Lookup trace_id from mapping                             │
│ 2. Calculate time window (alarm_time ± 1 hour)              │
│ 3. Fetch trace data from API                                │
│    client.get_trace_data(trace_id, from, to)                │
│ 4. Find max ARI value in window                             │
│ 5. Compare against threshold (5.0 years)                    │
│ 6. Set status:                                              │
│    • VALIDATED (exceeded)                                   │
│    • NOT_VALIDATED (not exceeded)                           │
│    • UNVALIDATABLE (no data/mapping/error)                  │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ SAVE RESULTS                                                │
│ Using: pandas.to_csv()                                      │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ OUTPUT FILE                                                 │
│ outputs/rain_gauges/ari_alarm_validation.csv                │
│                                                             │
│ Columns:                                                    │
│ • assetid, gauge_name, alarm_time_utc                       │
│ • trace_id, status, max_ari_value                           │
│ • threshold, reason                                         │
└─────────────────────────────────────────────────────────────┘
```

**Files Involved (Stage 4):**
- Entry: `scripts/gauge/validate.py`
- API: `moata_pipeline/moata/` (5 files)
- Utilities: `moata_pipeline/common/` (time_utils.py)
- Inputs: `data/inputs/raingauge_ari_alarms.csv`

---

### **STAGE 5: VALIDATION VISUALIZATION**

#### **Entry Point:** `scripts/gauge/visualize_validation.py`

```
┌─────────────────────────────────────────────────────────────┐
│ USER INPUT                                                  │
│ Command: python scripts/gauge/visualize_validation.py       │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ scripts/gauge/visualize_validation.py                       │
│ • parse_args()                                              │
│ • Default: outputs/.../ari_alarm_validation.csv             │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ LOAD VALIDATION DATA                                        │
│ INPUT: outputs/rain_gauges/ari_alarm_validation.csv         │
│ Using: pandas.read_csv()                                    │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ CREATE CHARTS (Using Matplotlib)                            │
│                                                             │
│ 1. create_status_chart()                                    │
│    • Pie chart of validation status                         │
│    • Colors: green (VALIDATED), red (NOT), gray (UNVAL)     │
│    • Save: validation_summary.png                           │
│                                                             │
│ 2. create_exceedance_chart()                                │
│    • Bar chart of top 10 exceedances                        │
│    • Sorted by exceed_by = max_ari - threshold              │
│    • Save: top_exceedances.png                              │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ CREATE HTML DASHBOARD                                       │
│ • create_html_dashboard()                                   │
│                                                             │
│ SECTIONS:                                                   │
│ 1. Header with timestamp                                    │
│ 2. Summary statistics cards                                 │
│ 3. Embedded charts (PNG images)                             │
│ 4. Validation results table (searchable)                    │
│ 5. Footer                                                   │
│                                                             │
│ FEATURES:                                                   │
│ • Live search with JavaScript                               │
│ • Color-coded status                                        │
│ • Responsive layout                                         │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ OUTPUT FILES                                                │
│ outputs/rain_gauges/validation_viz/                         │
│ ├── validation_dashboard.html                               │
│ ├── validation_summary.png                                  │
│ ├── top_exceedances.png                                     │
│ └── validation_stats.csv                                    │
└─────────────────────────────────────────────────────────────┘
```

**Files Involved (Stage 5):**
- Entry: `scripts/gauge/visualize_validation.py`
- Libraries: Matplotlib, Pandas
- No pipeline modules (standalone script)

---

## 4️⃣ **RAIN RADAR PIPELINE - DETAILED FLOW**

### **STAGE 1: DATA COLLECTION (RETRIEVE)**

#### **Entry Point:** `scripts/radar/retrieve.py`

```
┌─────────────────────────────────────────────────────────────┐
│ USER INPUT                                                  │
│ Command: python scripts/radar/retrieve.py [--date]          │
│         [--force-refresh-pixels]                            │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ scripts/radar/retrieve.py                                   │
│ • parse_args()                                              │
│ • Validate date                                             │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ moata_pipeline/collect/runner.py                            │
│ • run_collect_radar(start, end, force_refresh_pixels)       │
│ ORCHESTRATION:                                              │
│ 1. Fetch stormwater catchments                              │
│ 2. Build/load pixel mappings                                │
│ 3. Collect radar data                                       │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ CREATE API CLIENT (same as gauge)                           │
│ Constants:                                                  │
│ • STORMWATER_CATCHMENT_ASSET_TYPE_ID = 3541                 │
│ • RADAR_COLLECTION_ID = 1                                   │
│ • RADAR_QPE_TRACESET_ID = 3                                 │
│ • RADAR_MAX_PIXELS_PER_REQUEST = 150                        │
│ • RADAR_RECOMMENDED_BATCH_SIZE = 50                         │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ moata_pipeline/collect/collector.py                         │
│ • RadarDataCollector(client)                                │
│                                                             │
│ STEP 1: Fetch Catchments                                    │
│   client.get_catchments(594, 3541)                          │
│   → 157 stormwater catchments                               │
│   Save: catchments.json                                     │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ STEP 2: Build Pixel Mappings                                │
│ • build_pixel_mappings()                                    │
│                                                             │
│ FOR EACH CATCHMENT:                                         │
│ 1. Check cache (skip if exists)                             │
│ 2. If not cached:                                           │
│    client.get_collection_pixels(1, catchment_id)            │
│    → Returns pixel indices list                             │
│ 3. Save to cache                                            │
│    pixel_mappings/catchment_{id}_pixels.json                │
│                                                             │
│ CACHING:                                                    │
│ • Prevents redundant API calls                              │
│ • 157 cache files (~50-200 pixels each)                     │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ moata_pipeline/common/iter_utils.py                         │
│ • chunk(pixel_indices, batch_size)                          │
│   Split pixels into batches of 50                           │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ STEP 3: Collect Radar Data                                  │
│ • collect_radar_data()                                      │
│                                                             │
│ FOR EACH CATCHMENT:                                         │
│ 1. Get pixel indices from mapping                           │
│ 2. Batch pixels (50 per request)                            │
│ 3. For each batch:                                          │
│    client.get_collection_data(1, 3, pixels, from, to)       │
│    → Returns time series for pixels                         │
│ 4. Aggregate all batches                                    │
│ 5. Save CSV: {catchment_id}_{name}.csv                      │
│                                                             │
│ DATA VOLUME:                                                │
│ • ~15,700 pixels total                                      │
│ • ~2.26M data points (24h @ 10min intervals)                │
│ • ~500 MB - 2 GB total                                      │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ OUTPUT FILES                                                │
│ outputs/rain_radar/raw/                                     │
│ ├── catchments.json                                         │
│ ├── pixel_mappings/                                         │
│ │   └── catchment_XXX_pixels.json (~157 files)              │
│ └── radar_data/                                             │
│     └── XXXX_CatchmentName.csv (~157 files)                 │
└─────────────────────────────────────────────────────────────┘
```

**Files Involved (Stage 1):**
- Entry: `scripts/radar/retrieve.py`
- Orchestration: `moata_pipeline/collect/runner.py`
- Logic: `moata_pipeline/collect/collector.py` (RadarDataCollector)
- API: `moata_pipeline/moata/` (5 files)
- Utilities: `moata_pipeline/common/` (iter_utils.py, time_utils.py, etc)

---

### **STAGE 2: ARI ANALYSIS (ANALYZE)**

#### **Entry Point:** `scripts/radar/analyze.py`

```
┌─────────────────────────────────────────────────────────────┐
│ USER INPUT                                                  │
│ Command: python scripts/radar/analyze.py                    │
│         [--date] [--threshold 5.0]                          │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ scripts/radar/analyze.py                                    │
│ • parse_args()                                              │
│ • detect_radar_data_dir() [auto-detect]                     │
│ • determine_output_dir()                                    │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ moata_pipeline/analyze/radar_analysis.py                    │
│ • run_radar_analysis(data_dir, output_dir, tp108, threshold)│
│ ORCHESTRATION:                                              │
│ 1. Load TP108 coefficients                                  │
│ 2. Process each catchment CSV                               │
│ 3. Calculate ARI values                                     │
│ 4. Generate summary                                         │
│ 5. Save outputs                                             │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ LOAD TP108 COEFFICIENTS                                     │
│ INPUT: data/inputs/tp108_stats.csv                          │
│                                                             │
│ moata_pipeline/analyze/ari_calculator.py                    │
│ • ARICalculator(tp108_path, threshold)                      │
│ • load_coefficients()                                       │
│                                                             │
│ CSV Format:                                                 │
│   pixelindex, 10m_b, 10m_m, ..., 24h_b, 24h_m               │
│                                                             │
│ Formula: ARI = exp(m × Depth + b)                           │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ PROCESS EACH CATCHMENT                                      │
│ FOR EACH radar CSV file:                                    │
│                                                             │
│ 1. Load radar CSV                                           │
│    Columns: timestamp, pixel_index, value                   │
│                                                             │
│ 2. Get unique pixels in catchment                           │
│                                                             │
│ 3. FOR EACH PIXEL:                                          │
│    • process_pixel_data()                                   │
│                                                             │
│    FOR EACH DURATION (10m, 20m, 30m, 1h, 2h, 6h, 12h, 24h): │
│      a) Calculate rolling sum over duration                 │
│      b) Get coefficients (b, m) for this pixel+duration     │
│      c) Calculate ARI = exp(m × depth + b)                  │
│      d) If ARI ≥ threshold (5.0), record exceedance         │
│                                                             │
│ 4. Aggregate results across pixels                          │
│    • Max ARI                                                │
│    • Peak pixel, timestamp, duration                        │
│    • Pixels exceeding count                                 │
│    • Proportion exceeding                                   │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ BUILD SUMMARY                                               │
│ Create DataFrames:                                          │
│ 1. summary_df (one row per catchment)                       │
│    • catchment_id, name, max_ari                            │
│    • peak_pixel, timestamp, duration, depth                 │
│    • pixels_total, pixels_exceeding, proportion             │
│                                                             │
│ 2. exceedance_df (one row per exceedance)                   │
│    • All ARI values ≥ threshold                             │
│    • catchment, pixel, timestamp, duration, ari, depth      │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ GENERATE REPORT                                             │
│ • _generate_report()                                        │
│                                                             │
│ SECTIONS:                                                   │
│ 1. Overall statistics                                       │
│ 2. Top 20 catchments by max ARI                             │
│ 3. Proportion exceeding distribution                        │
│ 4. Duration analysis                                        │
│ 5. Recommendations                                          │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ OUTPUT FILES                                                │
│ outputs/rain_radar/analyze/                                 │
│ ├── ari_analysis_summary.csv                                │
│ ├── ari_exceedances.csv                                     │
│ └── analysis_report.txt                                     │
└─────────────────────────────────────────────────────────────┘
```

**Files Involved (Stage 2):**
- Entry: `scripts/radar/analyze.py`
- Orchestration: `moata_pipeline/analyze/radar_analysis.py`
- Logic: `moata_pipeline/analyze/ari_calculator.py`
- Input: `data/inputs/tp108_stats.csv` (CRITICAL)
- Utilities: Pandas

---

### **STAGE 3: VISUALIZATION (VISUALIZE)**

#### **Entry Point:** `scripts/radar/visualize.py`

```
┌─────────────────────────────────────────────────────────────┐
│ USER INPUT                                                  │
│ Command: python scripts/radar/visualize.py [--date]         │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ scripts/radar/visualize.py                                  │
│ • parse_args()                                              │
│ • detect_data_dir() [auto-detect]                           │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ moata_pipeline/viz/radar_runner.py                          │
│ • run_radar_visual_report(data_dir, out_dir, date)          │
│ ORCHESTRATION:                                              │
│ 1. Load radar data & analysis                               │
│ 2. Clean data                                               │
│ 3. Generate charts                                          │
│ 4. Create HTML dashboard                                    │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ LOAD DATA                                                   │
│ 1. data_dir/catchments.json                                 │
│ 2. data_dir/radar_data/*.csv (sample)                       │
│ 3. analyze_dir/ari_analysis_summary.csv                     │
│ 4. analyze_dir/ari_exceedances.csv                          │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ moata_pipeline/viz/radar_cleaning.py                        │
│ • clean_radar_data(radar_df)                                │
│                                                             │
│ CLEANING:                                                   │
│ 1. Type conversions                                         │
│ 2. Remove invalid values                                    │
│ 3. Sort and validate                                        │
│ 4. Aggregate statistics                                     │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ CREATE CHARTS (Matplotlib)                                  │
│ 1. rainfall_timeseries.png                                  │
│ 2. top_catchments.png                                       │
│ 3. ari_distribution.png                                     │
│ 4. spatial_heatmap.png                                      │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ moata_pipeline/viz/radar_report.py                          │
│ • create_radar_dashboard()                                  │
│                                                             │
│ SECTIONS:                                                   │
│ 1. Header & summary statistics                              │
│ 2. Interactive map (Leaflet.js)                             │
│    • Catchment polygons                                     │
│    • Color-coded by ARI/rainfall                            │
│ 3. Time series charts                                       │
│ 4. Catchment comparison tables                              │
│ 5. ARI exceedance summary                                   │
│ 6. Footer                                                   │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ OUTPUT FILES                                                │
│ outputs/rain_radar/dashboard/                               │
│ ├── radar_dashboard.html                                    │
│ ├── catchment_stats.csv                                     │
│ └── charts/                                                 │
│     ├── rainfall_timeseries.png                             │
│     ├── top_catchments.png                                  │
│     ├── ari_distribution.png                                │
│     └── spatial_heatmap.png                                 │
└─────────────────────────────────────────────────────────────┘
```

**Files Involved (Stage 3):**
- Entry: `scripts/radar/visualize.py`
- Orchestration: `moata_pipeline/viz/radar_runner.py`
- Logic: `moata_pipeline/viz/` (radar_cleaning.py, radar_report.py)
- Libraries: Matplotlib, Pandas

---

### **STAGE 4 & 5: VALIDATION (Similar to Gauge)**

**Stage 4:** `scripts/radar/validate.py`
- Uses **spatial threshold** (≥30% area) instead of point threshold
- Simpler (no API calls, just CSV processing)

**Stage 5:** `scripts/radar/visualize_validation.py`
- Similar charts to gauge validation
- Focus on spatial proportion metrics

---

## 5️⃣ **FILE DEPENDENCIES MAP**

### **Core Dependencies**

```
┌────────────────────────────────────────────────────────────┐
│                   DEPENDENCY TREE                          │
└────────────────────────────────────────────────────────────┘

CLI Scripts (10)
  ├─→ Runners (3 files)
  │    ├─→ Collectors (collector.py)
  │    │    └─→ API Client (moata/, 5 files)
  │    │         └─→ Constants, Auth, HTTP
  │    ├─→ Analyzers (analyze/, 4 files)
  │    │    └─→ Common Utilities
  │    └─→ Visualizers (viz/, 6 files)
  │         └─→ HTML Utils, DataFrame Utils
  │
  └─→ Common Utilities (common/, 12 files)
       ├─→ PipelinePaths (Singleton)
       ├─→ Time Utils
       ├─→ File Utils
       ├─→ JSON I/O
       └─→ DataFrame Utils

GUI (10 files)
  ├─→ ModernApp (main.py)
  │    ├─→ Config (colors, themes)
  │    ├─→ Components (UI widgets)
  │    └─→ Executor (subprocess)
  │         └─→ CLI Scripts
  │
  └─→ Pipelines (4 files)
       ├─→ BasePipeline (abstract)
       ├─→ GaugePipeline (concrete)
       └─→ RadarPipeline (concrete)
```

### **Import Relationships**

```python
# Example: scripts/gauge/retrieve.py

import sys
from pathlib import Path
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

# Core imports
from moata_pipeline.collect.runner import run_collect_rain_gauges
from moata_pipeline.logging_setup import setup_logging

# Which then imports:
# moata_pipeline/collect/runner.py
#   ├─→ moata_pipeline/collect/collector.py
#   │    └─→ moata_pipeline/moata/client.py
#   │         ├─→ moata_pipeline/moata/auth.py
#   │         ├─→ moata_pipeline/moata/http.py
#   │         └─→ moata_pipeline/moata/endpoints.py
#   ├─→ moata_pipeline/common/paths.py
#   ├─→ moata_pipeline/common/constants.py
#   └─→ moata_pipeline/common/output_writer.py
```

---

## 6️⃣ **DATA FLOW DIAGRAMS**

### **Overall System Data Flow**

```
┌──────────────┐
│  Moata API   │  (External System)
└──────┬───────┘
       │ OAuth2 + REST
       ▼
┌────────────────────────────────────────────────┐
│          DATA COLLECTION                       │
│  • Rain Gauges (60)                            │
│  • Stormwater Catchments (157)                 │
│  • Radar Pixels (~15,700)                      │
└────────┬───────────────────────────────────────┘
         │ JSON
         ▼
┌────────────────────────────────────────────────┐
│          RAW DATA STORAGE                      │
│  outputs/rain_gauges/raw/                      │
│  outputs/rain_radar/raw/                       │
└────────┬───────────────────────────────────────┘
         │
         ▼
┌────────────────────────────────────────────────┐
│          DATA ANALYSIS                         │
│  • Filtering (gauge)                           │
│  • ARI Calculation (radar, TP108)              │
│  • Alarm Configuration Analysis                │
└────────┬───────────────────────────────────────┘
         │ CSV + TXT
         ▼
┌────────────────────────────────────────────────┐
│          ANALYZED DATA                         │
│  outputs/.../analyzed/                         │
│  • Summary CSVs                                │
│  • Reports                                     │
└────────┬───────────────────────────────────────┘
         │
         ▼
┌────────────────────────────────────────────────┐
│          VISUALIZATION                         │
│  • HTML Dashboard Generation                   │
│  • Charts (Matplotlib)                         │
│  • Interactive Maps (Leaflet.js)               │
└────────┬───────────────────────────────────────┘
         │ HTML + PNG
         ▼
┌────────────────────────────────────────────────┐
│          DASHBOARDS                            │
│  outputs/.../visualizations/                   │
│  outputs/.../dashboard/                        │
└────────┬───────────────────────────────────────┘
         │
         ▼
┌────────────────────────────────────────────────┐
│          VALIDATION (Optional)                 │
│  • Compare against historical alarms           │
│  • Fetch actual data from API                  │
│  • Generate validation reports                 │
└────────┬───────────────────────────────────────┘
         │ CSV + HTML
         ▼
┌────────────────────────────────────────────────┐
│          VALIDATION DASHBOARDS                 │
│  outputs/.../validation_viz/                   │
└────────────────────────────────────────────────┘
         │
         ▼
    [ End User ]
```

### **File Flow Per Pipeline Stage**

```
RAIN GAUGE PIPELINE:

API → JSON → CSV → HTML → (Validation) → HTML
 ↓     ↓     ↓     ↓          ↓          ↓
raw/  raw/  analyzed/ viz/   validation/ val_viz/
```

```
RAIN RADAR PIPELINE:

API → JSON + CSV → CSV → HTML → (Validation) → HTML
 ↓     ↓     ↓      ↓     ↓          ↓          ↓
raw/  raw/  raw/  analyzed/ dashboard/ validation/ val_viz/
      catchments  radar                         
      + pixels    data
```

---

## 7️⃣ **KEY DESIGN PATTERNS**

### **1. Singleton Pattern**
```python
# moata_pipeline/common/paths.py
class PipelinePaths:
    _instance = None
    
    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
```
**Used for**: Centralized path management

### **2. Factory Pattern**
```python
# moata_alert_lab_gui/pipelines/
if pipeline_type == "gauge":
    pipeline = GaugePipeline(...)
elif pipeline_type == "radar":
    pipeline = RadarPipeline(...)
```
**Used for**: Creating different pipeline instances

### **3. Strategy Pattern**
```python
# moata_pipeline/collect/collector.py
class RainGaugeCollector:
    def collect(...): pass

class RadarDataCollector:
    def collect(...): pass
```
**Used for**: Different collection strategies

### **4. Template Method Pattern**
```python
# moata_alert_lab_gui/pipelines/base.py
class BasePipeline(ABC):
    @abstractmethod
    def get_steps(self): pass
    
class GaugePipeline(BasePipeline):
    def get_steps(self):
        return [step1, step2, ...]
```
**Used for**: Define pipeline structure

### **5. Facade Pattern**
```python
# moata_pipeline/collect/runner.py
def run_collect_rain_gauges(...):
    # Hides complexity of:
    # Auth → HTTP → Client → Collector → Output
    pass
```
**Used for**: Simplify complex subsystems

### **6. Observer Pattern**
```python
# moata_alert_lab_gui/executor.py
class ScriptExecutor:
    def execute_script(...):
        # Observes subprocess output
        # Updates GUI in real-time
        pass
```
**Used for**: Real-time output streaming

---

## 📌 **QUICK REFERENCE**

### **File Count Summary**

| Component | Files | Purpose |
|-----------|-------|---------|
| **moata/** | 5 | API Client (auth, http, client, endpoints) |
| **collect/** | 3 | Data collection orchestration |
| **analyze/** | 7 | Data analysis & ARI calculation |
| **viz/** | 8 | Visualization & dashboard generation |
| **common/** | 12 | Shared utilities |
| **GUI** | 10 | CustomTkinter application |
| **CLI Scripts** | 10 | Command-line entry points |
| **TOTAL** | **55** | Production code files |

### **Key Files**

| File | Purpose |
|------|---------|
| `moata_alert_lab.py` | GUI launcher |
| `scripts/gauge/retrieve.py` | Gauge collection entry |
| `scripts/radar/retrieve.py` | Radar collection entry |
| `moata_pipeline/common/paths.py` | Path management (singleton) |
| `moata_pipeline/moata/client.py` | Main API client |
| `moata_pipeline/analyze/ari_calculator.py` | ARI calculation (TP108) |
| `data/inputs/tp108_stats.csv` | **CRITICAL** - ARI coefficients |

---

**END OF TECHNICAL SUMMARY**

---

**Document Version**: 1.0  
**Last Updated**: January 2026  
**Created by**: Muhammad Juang  
**Institution**: University of Auckland (COMPSCI 778)