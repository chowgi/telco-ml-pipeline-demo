# Atlas Charts Dashboard Setup

Create this dashboard in ~5 minutes via the Atlas UI.

## Steps

### 1. Open Atlas Charts
Atlas UI → Charts (left sidebar) → Dashboards → **New Dashboard**
- Title: `Telco ODS - Autonomous Network Health`
- Description: `Real-time network health monitoring`

### 2. Add Data Source
If not already connected:
- Data Sources → Add Data Source → DemoCluster → `ods_demo_db`

### 3. Create Charts

Enable **Auto Refresh** (top-right, set to 10s) before presenting.

---

#### Chart 1: Network Health Predictions (Donut)
- Type: **Donut**
- Data source: `ods_demo_db.network_health_predictions`
- Label: `prediction.label`
- Arc: Count
- Colors: excellent=green, good=orange, poor=red

#### Chart 2: Anomalies Over Time (Line)
- Type: **Line**
- Data source: `ods_demo_db.network_health_predictions`
- Filter: `prediction.label = "poor"`
- X-axis: `window_end` (continuous, 5 min bin)
- Y-axis: Count
- Series: `region`

#### Chart 3: Events Processed (Number)
- Type: **Number**
- Data source: `ods_demo_db.windowed_network_metrics`
- Value: `SUM(event_count)`
- Filter: last 10 minutes
- Label: "Events in Latest Window"

#### Chart 4: Signal vs Latency (Scatter)
- Type: **Scatter**
- Data source: `ods_demo_db.windowed_network_metrics`
- X-axis: `signal_strength_dbm.avg`
- Y-axis: `latency_ms.avg`
- Color: `region`

#### Chart 5: Regional Health (Stacked Bar)
- Type: **Stacked Bar**
- Data source: `ods_demo_db.network_health_predictions`
- X-axis: `region`
- Y-axis: Count
- Series: `prediction.label`
- Filter: last 30 minutes

#### Chart 6: Prediction Volume (Stacked Area)
- Type: **Stacked Area**
- Data source: `ods_demo_db.network_health_predictions`
- X-axis: `timestamp` (continuous, 5 min bin)
- Y-axis: Count
- Series: `prediction.label`

### 4. Add Filters (top of dashboard)
- Region dropdown (field: `region`)
- Date range (field: `window_end`, default: last 1 hour)

### 5. Enable Auto-Refresh
Top-right corner → Toggle auto-refresh → 10 seconds

---

## Layout Suggestion

```
┌─────────────────────────┬─────────────────────────┐
│  Health Predictions     │  Anomalies Over Time    │
│  (Donut)                │  (Line by region)       │
├────────────┬────────────┼─────────────────────────┤
│  Events    │  Signal vs │  Regional Health        │
│  (Number)  │  Latency   │  (Stacked Bar)          │
│            │  (Scatter) │                         │
├────────────┴────────────┼─────────────────────────┤
│  Prediction Volume (Stacked Area)                 │
└───────────────────────────────────────────────────┘
```
