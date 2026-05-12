# Demo Walkthrough - Telco ODS Autonomous Networks

## Pre-Demo Setup (30 minutes before)

### 1. Deploy Infrastructure
```bash
export KEY_PAIR_NAME=bennyk_aws_key
export SSH_KEY_PATH=./bennyk_aws_key.pem
./infrastructure/scripts/deploy.sh
```

### 2. Whitelist IPs in Atlas
The deploy script prints the EC2 public IPs. Add them to:
- Atlas > Network Access > Add IP Address
- Or add `0.0.0.0/0` temporarily for the demo

### 3. Configure Atlas Trigger
1. Atlas > App Services > Create Application (name: `telco-ods-demo`)
2. Create Database Trigger:
   - Name: `network_health_inference`
   - Cluster: `DemoCluster`
   - Database: `ods_demo_db`
   - Collection: `windowed_network_metrics`
   - Operation Type: Insert
   - Full Document: ON
3. Function: paste contents of `atlas-trigger/trigger_function.js`
4. Create Value: `MLFLOW_ENDPOINT` = `http://<mlflow-ip>:5003/invocations`

### 4. Verify Pipeline
```bash
./infrastructure/scripts/validate.sh
```

Wait 5 minutes, then check Atlas for data:
```
db.windowed_network_metrics.countDocuments()
db.network_health_predictions.find().sort({timestamp:-1}).limit(3)
```

### 5. Set Up Atlas Charts Dashboard
1. Atlas > Charts > New Dashboard
2. Data source: `ods_demo_db`
3. Create charts (see `atlas-charts/dashboard_config.json` for specs):
   - Donut chart: prediction distribution
   - Line chart: anomaly rate over time by region
   - Number chart: total events processed

---

## Demo Script (15-20 minutes)

### Slide 1: Problem Statement (2 min)
"Telstra operates thousands of cell towers across Australia. They need an Operational Data Store that can ingest real-time network telemetry, enable ML-driven anomaly detection, and serve operational insights with sub-second latency."

### Slide 2: Architecture Overview (3 min)
Show the pipeline running live:
- "Data generator simulating 50 cell towers across 5 Australian regions at ~80k events/sec"
- "Kafka buffering the raw telemetry stream"
- "Flink performing 5-minute windowed aggregation — avg, min, max, p95 across signal strength, throughput, latency, packet loss, jitter, call drops"
- "MongoDB Atlas as the ODS — receiving windowed aggregates in a time series collection"
- "Atlas Trigger firing on each new window — calling our ML model for real-time inference"
- "Predictions stored back in MongoDB — powering this real-time dashboard"

### Slide 3: Live MongoDB Data (3 min)
Show in Atlas UI or mongosh:
```javascript
// Windowed aggregates arriving every 5 minutes
db.windowed_network_metrics.find().sort({window_end: -1}).limit(3).pretty()

// Real-time predictions
db.network_health_predictions.find().sort({timestamp: -1}).limit(5).pretty()

// Anomaly detection working
db.network_health_predictions.aggregate([
  {$group: {_id: "$prediction.label", count: {$sum: 1}}}
])
```

### Slide 4: ML Model & MLflow (2 min)
- Open MLflow UI: `http://<mlflow-ip>:5002`
- Show experiment tracking, model registry
- Live inference test:
```bash
# Poor network - should predict "poor"
curl -X POST http://<mlflow-ip>:5003/invocations \
  -H "Content-Type: application/json" \
  -d '{"dataframe_records": [{"signal_strength_dbm": -85, "throughput_mbps": 20, "latency_ms": 120, "call_drop_rate_percent": 3.0, "packet_loss_percent": 2.5, "jitter_ms": 8.0}]}'
```

### Slide 5: Feast Feature Store (2 min)
"MongoDB serves as the online store for Feast — features are materialized from windowed aggregates and served at inference time with sub-millisecond latency."

Show the MongoDB-Feast integration:
```python
from feast import FeatureStore
store = FeatureStore(repo_path="feast-feature-store")
features = store.get_online_features(
    features=["windowed_cell_metrics:avg_signal_strength_dbm", ...],
    entity_rows=[{"cell_id": "CELL_0001"}]
)
```

### Slide 6: ODS Requirements Mapping (3 min)
Walk through how this demo addresses Telstra's requirements:
- **FR-3** (Real-time ingestion): Kafka + Flink streaming at 80k eps
- **FR-5** (Schema-flexible): MongoDB document model, no schema migration needed
- **FR-9** (API-based access): MLflow REST API, Atlas Data API
- **FR-11** (Operational features): Feast feature store with MongoDB backend
- **FR-12** (Operational workflows): Anomaly detection loop, trigger-based inference
- **NFRs**: Sub-100ms queries, <500ms inference, horizontal scalability

### Slide 7: Atlas Charts Dashboard (2 min)
Show the live dashboard updating:
- Network health prediction distribution
- Anomaly rate by region over time
- Event processing throughput

### Close: Q&A
"Everything you've seen runs on 4 EC2 instances and MongoDB Atlas. The entire stack deploys in under 5 minutes and tears down with one command. This is a fraction of the 200k eps target — MongoDB and this architecture scale horizontally to millions."

---

## Post-Demo Teardown
```bash
./infrastructure/scripts/teardown.sh
```

Then clean up Atlas:
- Remove the trigger in App Services
- Remove IP whitelist entries
- Optionally drop collections in `ods_demo_db`

---

## Troubleshooting During Demo

| Issue | Fix |
|-------|-----|
| No data in MongoDB | Check generator: `ssh ubuntu@<gen-ip> "tail /var/log/generator.log"` |
| No predictions appearing | Check trigger logs in App Services > Logs |
| MLflow not responding | `ssh ubuntu@<mlflow-ip> "sudo systemctl restart mlflow-tracking"` |
| Flink not processing | `ssh ubuntu@<flink-ip> "tail /var/log/flink-job.log"` |
| Dashboard not updating | Ensure Charts auto-refresh is set to 10s |

## Key Talking Points
- MongoDB as ODS handles both the ingestion (writes) and serving (reads) in one platform
- No ETL pipeline needed between ODS and ML — Atlas Triggers close the loop
- Schema flexibility means new telemetry fields don't require migration
- Time series collections provide optimized storage and query for windowed metrics
- Feast + MongoDB = production-grade feature store with sub-ms retrieval
- The entire pipeline is event-driven and real-time (no batch processing)
