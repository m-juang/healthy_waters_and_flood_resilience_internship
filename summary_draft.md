# 📊 RAIN GAUGE vs RAIN RADAR: What is Retrieved & Analyzed

**Complete explanation of data collection and analysis**

---

## 🌧️ RAIN GAUGE PIPELINE

### **RETRIEVE Stage: What is Collected**

#### **1. Gauge Assets (Sensors)**
**Data retrieved:**
```json
{
  "id": 12345,
  "name": "Auckland CBD Rain Gauge",
  "description": "Central city rainfall monitoring",
  "projectId": 594,
  "assetTypeId": 100,
  "location": {
    "latitude": -36.8485,
    "longitude": 174.7633
  }
}
```

**What it is:**
- Physical rain gauge sensors installed across Auckland
- ~264 total gauges (Auckland + Northland + Waikato)
- ~60 active Auckland gauges (after filtering)

**What they measure:**
- Point rainfall at specific location
- Typically tipping bucket gauges (0.2mm per tip)

---

#### **2. Traces (Measurement Series)**
**Data retrieved:**
```json
{
  "id": 67890,
  "assetId": 12345,
  "traceId": 67890,
  "name": "Rainfall",
  "description": "Total rainfall measurement",
  "hasAlarms": true,
  "units": "mm"
}
```

**What it is:**
- Time series of measurements from each gauge
- Multiple traces per gauge (e.g., "Rainfall", "Intensity")
- Each trace can have alarms configured

**Typical traces per gauge:**
- `Rainfall` - Cumulative rainfall (mm)
- `Intensity` - Rainfall rate (mm/hr)
- `Battery Voltage` - Sensor health

---

#### **3. Alarms (Alert Configurations)**
**Data retrieved:**
```json
{
  "id": 11111,
  "traceId": 67890,
  "name": "High Rainfall",
  "alarmType": "overflow",
  "threshold": 50.0,
  "enabled": true
}
```

**What it is:**
- Alert thresholds configured for each trace
- Triggers when rainfall exceeds threshold

**Alarm types:**
- **Overflow**: Rainfall exceeds threshold (e.g., >50mm)
- **Recency**: No data received (gauge offline)
- **Other**: Custom alerts

---

#### **4. Thresholds (ARI Values)**
**Data retrieved:**
```json
{
  "id": 22222,
  "traceId": 67890,
  "alarmId": 11111,
  "duration": "1h",
  "ari": "10-year",
  "value": 35.5
}
```

**What it is:**
- ARI (Average Recurrence Interval) thresholds
- Design rainfall for different durations
- Used for flood alarming

**Example thresholds:**
- 1-hour, 10-year ARI: 35.5mm
- 1-hour, 50-year ARI: 48.0mm
- 24-hour, 100-year ARI: 150.0mm

---

#### **5. Timeseries Data (Actual Measurements)**
**Data retrieved:**
```json
{
  "items": [
    {
      "timestamp": "2025-01-03T08:00:00Z",
      "value": 2.4
    },
    {
      "timestamp": "2025-01-03T08:01:00Z",
      "value": 2.6
    }
  ]
}
```

**What it is:**
- Minute-by-minute rainfall measurements
- Actual rainfall data over time range
- Used for calculating totals and intensities

**Time resolution:**
- 1-minute intervals (60 data points per hour)
- Collected for specified date range

---

### **RETRIEVE Output:**

**File:** `rain_gauges_traces_alarms.json`

**Structure:**
```json
[
  {
    "gauge": {
      "id": 12345,
      "name": "Auckland CBD Rain Gauge",
      ...
    },
    "traces": [
      {
        "trace": {
          "id": 67890,
          "name": "Rainfall",
          ...
        },
        "alarms": [...],
        "alarms_by_type": {
          "overflow": [...],
          "recency": [...],
          "other": [...]
        },
        "thresholds": [...],
        "timeseries": [...]
      }
    ]
  }
]
```

**Data size:**
- ~60 gauges × 3 traces × alarm data
- 24 hours × 60 minutes = 1,440 data points per trace
- Total: ~5-10 MB JSON file

---

### **ANALYZE Stage: What is Analyzed**

#### **1. Active Gauge Filtering**

**Process:**
```python
# Filter criteria:
1. Last data within 6 months (not inactive)
2. Name doesn't contain "northland|waikato" (Auckland only)
3. Has valid traces and alarms
```

**Output:** `active_auckland_gauges.json`
- ~60 active gauges (from 264 total)

---

#### **2. Trace Inventory**

**Process:**
```python
# Extract all traces:
for gauge in gauges:
    for trace in gauge.traces:
        record = {
            "gauge_name": gauge.name,
            "trace_id": trace.id,
            "trace_description": trace.description,
            "has_alarms": trace.hasAlarms,
            "alarm_count": len(trace.alarms)
        }
```

**Output:** `all_traces.csv`
- All traces from all active gauges
- ~180 traces (60 gauges × 3 traces each)

---

#### **3. Alarm Summary**

**Process:**
```python
# Extract alarms:
for trace in all_traces:
    for alarm in trace.alarms:
        if alarm.type == "overflow":  # ARI alarms
            record = {
                "gauge_name": gauge.name,
                "trace_description": trace.description,
                "alarm_name": alarm.name,
                "alarm_type": alarm.type,
                "threshold": alarm.threshold
            }
```

**Output:** 
- `alarm_summary_full.csv` - All columns
- `alarm_summary.csv` - Essential columns only

**Data analyzed:**
- Which gauges have ARI alarms configured
- Threshold values for each alarm
- Alarm types and configurations

---

#### **4. Analysis Report**

**Process:**
```python
# Generate statistics:
report = f"""
Total gauges: {len(all_gauges)}
Active gauges: {len(active_gauges)}
Excluded (Northland/Waikato): {num_excluded}
Inactive (>6 months): {num_inactive}
Total traces: {len(all_traces)}
Total alarms: {len(alarms)}
ARI alarms: {len(overflow_alarms)}
"""
```

**Output:** `analysis_report.txt`
- Summary statistics
- Data quality metrics
- Coverage assessment

---

## 📡 RAIN RADAR PIPELINE

### **RETRIEVE Stage: What is Collected**

#### **1. Stormwater Catchments**
**Data retrieved:**
```json
{
  "id": 456,
  "name": "Auckland_CBD",
  "description": "Central city catchment",
  "projectId": 594,
  "assetTypeId": 3541,
  "geometrySrId": 4326,
  "geometryWkt": "POLYGON((174.76 -36.85, ...))"
}
```

**What it is:**
- Stormwater drainage catchments
- Geographic polygons covering Auckland
- 157 catchments total

**What they represent:**
- Areas draining to specific stormwater pipes
- Used for flood modeling and management

---

#### **2. Pixel Mappings**
**Data retrieved:**
```json
{
  "catchmentId": 456,
  "pixelIndices": [1234, 1235, 1236, 1237, ...]
}
```

**What it is:**
- Radar grid pixels that intersect each catchment
- Mapping from catchment geometry to radar pixels
- Cached (doesn't change)

**Radar grid:**
- Auckland covered by ~50,000 pixels
- Each pixel: ~250m × 250m
- Each catchment: 5-50 pixels typically

---

#### **3. QPE Data (Quantitative Precipitation Estimation)**
**Data retrieved:**
```json
{
  "pixelIndex": 1234,
  "tracesetId": 3,
  "startTime": "2025-01-03T00:00:00Z",
  "dataOffsetSeconds": 60,
  "values": [0.0, 0.2, 0.4, 0.6, 0.8, ...]
}
```

**What it is:**
- Rainfall estimates from weather radar
- Spatial rainfall data across entire catchment
- High resolution in space and time

**Time resolution:**
- 1-minute intervals
- 24 hours = 1,440 data points per pixel

**Spatial resolution:**
- 250m × 250m pixels
- Full catchment coverage

---

### **RETRIEVE Output:**

**Files created:**

1. **Catchment metadata:** `catchments.csv`
```csv
id,name,description,geometryWkt
456,Auckland_CBD,Central city,"POLYGON(...)"
```

2. **Pixel mappings:** `pixels.json` + `pixels.pkl`
```json
{
  "456": [1234, 1235, 1236, ...],
  "457": [2345, 2346, 2347, ...]
}
```

3. **Radar data:** `radar_data/456_Auckland_CBD.csv`
```csv
pixel_index,value_index,timestamp,value
1234,0,2025-01-03T00:00:00Z,0.0
1234,1,2025-01-03T00:01:00Z,0.2
1234,2,2025-01-03T00:02:00Z,0.4
```

4. **Summary:** `collection_summary.json`

**Data size:**
- 157 catchments × 20 pixels avg × 1,440 minutes
- ~4.5 million data points
- ~500 MB total (CSV files)

---

### **ANALYZE Stage: What is Analyzed**

#### **1. ARI Exceedance Calculation**

**Process:**
```python
# For each catchment:
for pixel in catchment_pixels:
    for duration in [15min, 30min, 1h, 2h, 6h, 12h, 24h]:
        # Calculate moving sum
        rainfall = sum(pixel_data[window])
        
        # Compare to ARI thresholds
        for ari in [2yr, 5yr, 10yr, 20yr, 50yr, 100yr]:
            if rainfall > ari_threshold[duration][ari]:
                exceedance_recorded()
```

**What is calculated:**
- Rolling rainfall totals for different durations
- Comparison to design rainfall (ARI thresholds)
- Peak intensities and timing

**ARI Durations analyzed:**
- 15 minutes, 30 minutes
- 1 hour, 2 hours
- 6 hours, 12 hours, 24 hours

**ARI Return periods:**
- 2-year, 5-year, 10-year
- 20-year, 50-year, 100-year

---

#### **2. Spatial Aggregation**

**Process:**
```python
# For each catchment:
catchment_pixels = [pixel1, pixel2, pixel3, ...]

# Calculate statistics:
mean_rainfall = average(all_pixels)
max_rainfall = max(all_pixels)
min_rainfall = min(all_pixels)
spatial_variance = variance(all_pixels)
```

**What is calculated:**
- Average rainfall across catchment
- Maximum pixel (hotspot)
- Spatial variability

---

#### **3. Temporal Analysis**

**Process:**
```python
# Find peaks:
for duration in durations:
    rolling_sum = moving_window(rainfall, duration)
    peak_value = max(rolling_sum)
    peak_time = timestamp_of_max(rolling_sum)
    peak_ari = classify_ari(peak_value, duration)
```

**What is calculated:**
- Peak rainfall for each duration
- When peaks occurred
- ARI classification of peaks

---

### **ANALYZE Output:**

**Files created:**

1. **ARI Summary:** `ari_summary.csv`
```csv
catchment_id,catchment_name,duration,peak_rainfall,peak_time,ari_exceeded
456,Auckland_CBD,1h,45.2,2025-01-03T14:30:00Z,50-year
456,Auckland_CBD,24h,98.5,2025-01-03T23:59:00Z,20-year
```

2. **ARI Exceedances:** `ari_exceedances.csv`
```csv
catchment_id,timestamp,duration,rainfall,ari_threshold,ari_period,exceeded_by
456,2025-01-03T14:30:00Z,1h,45.2,48.0,50-year,-2.8
```

3. **Per-catchment ARI:** `ari_456_Auckland_CBD.csv`
- Detailed timeseries with ARI classifications
- All durations and thresholds

---

## 📊 KEY DIFFERENCES

| Aspect | Rain Gauge | Rain Radar |
|--------|------------|------------|
| **Measurement type** | Point (single location) | Spatial (grid) |
| **Coverage** | 60 discrete locations | 157 catchments (full spatial) |
| **Spatial resolution** | N/A (point) | 250m × 250m pixels |
| **Time resolution** | 1 minute | 1 minute |
| **Data volume** | 5-10 MB (60 gauges) | 500 MB (157 catchments) |
| **Primary use** | Ground truth, validation | Spatial coverage, modeling |
| **Accuracy** | High (direct measurement) | Moderate (estimation) |

---

## 🎯 WHAT IS ANALYZED

### **Rain Gauge Analysis:**
1. ✅ **Gauge health** - Which gauges are active/inactive
2. ✅ **Alarm configuration** - Which gauges have ARI alarms
3. ✅ **Data availability** - Coverage and quality
4. ✅ **Threshold inventory** - What ARI values are configured

**NOT analyzed (yet):**
- ❌ Actual ARI exceedances from timeseries
- ❌ Peak rainfall calculations
- ❌ Temporal patterns

---

### **Rain Radar Analysis:**
1. ✅ **ARI exceedances** - When/where design rainfall exceeded
2. ✅ **Peak intensities** - Maximum rainfall for each duration
3. ✅ **Spatial patterns** - Where rainfall was highest
4. ✅ **Temporal patterns** - When peaks occurred
5. ✅ **Catchment-level** - Aggregated statistics per catchment

---

## 💡 PRACTICAL EXAMPLES

### **Example 1: Heavy Rainfall Event**

**Gauge data:**
```
Auckland CBD Gauge:
- 1-hour peak: 42mm at 14:30
- Has alarm: "High Rainfall" (threshold 50mm)
- Status: Below threshold, no alarm triggered
```

**Radar data:**
```
Auckland CBD Catchment:
- 1-hour peak: 45.2mm at 14:30 (averaged across catchment)
- ARI classification: 50-year event
- Spatial: Max pixel 52mm, min pixel 38mm
- Coverage: 15 of 20 pixels exceeded 10-year ARI
```

**Insight:**
- Gauge shows point measurement (42mm)
- Radar shows spatial average (45.2mm) and variability
- Radar detects 50-year event, gauge didn't trigger alarm

---

### **Example 2: Data Quality Check**

**Gauge analysis:**
```
Analysis Report:
- Total gauges: 264
- Active (Auckland): 60
- Inactive: 45 (no data >6 months)
- Excluded: 159 (Northland/Waikato)
- With ARI alarms: 48
```

**Radar analysis:**
```
Collection Summary:
- Total catchments: 157
- Successful: 155
- Failed: 2 (geometry errors)
- Total pixels: 3,140
- Data records: 4,521,600
```

---

## 🎯 USE CASES

### **Rain Gauge:**
1. ✅ Ground truth validation
2. ✅ Point-specific monitoring
3. ✅ Alarm system configuration
4. ✅ Long-term trend analysis
5. ✅ Data quality assessment

### **Rain Radar:**
1. ✅ Spatial rainfall distribution
2. ✅ Catchment-level flood risk
3. ✅ ARI exceedance detection
4. ✅ Real-time flood alarming
5. ✅ Stormwater modeling input

---

## 📋 SUMMARY

### **Rain Gauge Pipeline:**
**Retrieves:**
- 60 gauge sensors
- ~180 measurement traces
- Alarm configurations
- ARI thresholds
- Minute-resolution timeseries

**Analyzes:**
- Gauge health/availability
- Alarm inventory
- Data quality
- Coverage assessment

---

### **Rain Radar Pipeline:**
**Retrieves:**
- 157 catchment geometries
- Pixel-to-catchment mappings
- Spatial rainfall grids
- 4.5M data points (24h)

**Analyzes:**
- ARI exceedances (7 durations × 6 ARIs)
- Peak rainfall calculations
- Spatial patterns
- Temporal patterns
- Flood risk assessment

---

## ✅ Conclusion

**Rain Gauge:** Point measurements → Configuration analysis  
**Rain Radar:** Spatial coverage → Risk analysis

**Both are complementary:**
- Gauges provide accuracy
- Radar provides coverage
- Together: Complete monitoring system! 🌧️📡