# Live Demo Script - Telco ODS Autonomous Networks

Target audience: Telstra engineering/architecture team
Duration: 15-20 minutes
Goal: Prove MongoDB Atlas as ODS for real-time ML-driven network health monitoring

---

## Pre-Demo Checklist (5 min before)

```bash
# 1. Ensure infrastructure is up (skip if already running)
./infrastructure/scripts/validate.sh

# 2. If SSH fails, update security group:
./infrastructure/scripts/start_demo.sh   # handles SG update + starts everything
# OR just open the dashboard and use Start button

# 3. Open these tabs in browser:
#    - Dashboard: http://<mlflow-ip>:8050
#    - Flink UI: http://<flink-ip>:8081
#    - Atlas UI > Browse Collections > ods_demo_db
#    - MLflow tracking UI: http://<mlflow-ip>:5002
```

The demo starts with the dashboard showing an empty state. You will click Start during the presentation.

---

## Demo Flow

### Opening: "The Problem" (2 min)
*Talking points -- no live demo yet*

- Telstra operates 50,000+ cell towers, generating millions of telemetry events per second
- Current approach: batch analytics with 30-60 min delay
- Need: real-time anomaly detection, sub-second query latency, operational ML
- "Let me show you how MongoDB Atlas as an ODS enables this in real-time."

---

### "Architecture Overview" (2 min)
*Show architecture diagram*

```
Generator (~1k eps) -> Kafka -> Apache Flink (30s windows) -> MongoDB Atlas (ODS)
                                                                    |
                         Dashboard <- predictions <- Atlas Trigger -> MLflow
```

Key talking points:
- MongoDB Atlas IS the ODS -- not just a cache or sink
- Schema-flexible: windowed metrics with nested stats (avg/min/max/p95)
- Atlas Triggers enable event-driven ML without external orchestration
- Single platform: storage, compute (triggers), serving, all in Atlas

---

### Live Demo: "One-Click Pipeline Start" (3 min)

**Show the dashboard (empty state):**
- "This is our operations dashboard. Right now the pipeline is stopped -- no data flowing."
- "Watch me start an entire ML pipeline with one click."

**Click the "Start Demo" button:**
- "Behind the scenes, this is:"
  - "Restarting Apache Flink (hard-kill required for clean state)"
  - "Starting the data generator at ~1,000 events per second"
  - "Clearing previous results"

**Wait ~30 seconds for data to appear:**
- "Each cell tower emits a statistical snapshot every 30 seconds."
- "50 towers means ~100 documents per minute hitting MongoDB, each triggering ML inference."

**Key talking point:**
> "Watch me start an entire streaming ML pipeline with one click. Generator, Flink, inference -- all running. This is what operational simplicity looks like."

---

### Live Demo: "Data Flowing" (3 min)

**Dashboard -- show real-time data appearing:**
- Point out predictions appearing in real-time
- Show the health distribution updating
- "30% of cells are configured as excellent, 12% degraded, 6% poor -- and the model is catching them all"

**Flink Web UI (separate tab):**
- Show the running job
- Point out throughput metrics, records processed
- "Flink computes rolling statistics -- avg, min, max, p95 -- for every metric on every cell tower"

**Atlas UI -- Show documents arriving:**
- Browse Collections -> `ods_demo_db.windowed_network_metrics`
- Expand a document, show nested metric objects
- "Every 30 seconds per cell, a rich statistical snapshot lands in Atlas"

**Key talking point:**
> "We're processing ~1,000 raw events per second through Flink, emitting per-cell snapshots every 30 seconds directly to Atlas. That's FR-3: real-time ingestion. And the schema is fully flexible -- nested objects, no migration required."

---

### Live Demo: "ML Inference" (3 min)

**Terminal -- Call the inference endpoint live:**
```bash
MLFLOW_IP=<from dashboard>

# Healthy cell
curl -s -X POST http://$MLFLOW_IP:5003/invocations \
  -H "Content-Type: application/json" \
  -d '{"dataframe_records": [{"signal_strength_dbm": -55, "throughput_mbps": 95, "latency_ms": 20, "call_drop_rate_percent": 0.3, "packet_loss_percent": 0.2, "jitter_ms": 1.5}]}' | python3 -m json.tool

# Degraded cell
curl -s -X POST http://$MLFLOW_IP:5003/invocations \
  -H "Content-Type: application/json" \
  -d '{"dataframe_records": [{"signal_strength_dbm": -85, "throughput_mbps": 12, "latency_ms": 180, "call_drop_rate_percent": 4.5, "packet_loss_percent": 8.2, "jitter_ms": 25}]}' | python3 -m json.tool
```

*Expected: `{"predictions": [0]}` (excellent) and `{"predictions": [2]}` (poor)*

**Key talking point:**
> "The model classifies network health in real-time. This runs automatically via an Atlas Database Trigger -- every 30 seconds per cell, the trigger fires, calls the model, stores the prediction. Zero orchestration code. That's FR-12."

---

### Live Demo: "Atlas Trigger in Action" (2 min)

**Atlas UI -- Show predictions collection:**
- Browse Collections -> `ods_demo_db.network_health_predictions`
- Sort by timestamp descending
- Expand a prediction document -- show input features, label, cell_id, region

**Key talking point:**
> "These predictions are written automatically by the Atlas Trigger -- ~100 per minute. No Airflow, no cron, no Lambda -- Atlas itself is the event-driven compute layer. The trigger fires within milliseconds of each new cell snapshot arriving."

---

### Live Demo: "MLflow Model Management" (2 min)

**Open MLflow UI (http://<mlflow-ip>:5002):**
- Show experiment tracking
- Show model registry
- "Full model lifecycle management -- versioning, metrics, artifacts"

**Key talking point:**
> "MLflow gives us the model lifecycle -- training, versioning, serving. Combined with Atlas Triggers, we have a fully automated inference pipeline with full auditability."

---

### Live Demo: "Query the ODS" (2 min)

**Atlas UI -- Run aggregation in Data Explorer or show mongosh:**
```javascript
// Latest health per region
db.network_health_predictions.aggregate([
  { $match: { timestamp: { $gte: new Date(Date.now() - 600000) } } },
  { $group: {
      _id: { region: "$region", label: "$prediction.label" },
      count: { $sum: 1 }
  }},
  { $sort: { "_id.region": 1, "count": -1 } }
])
```

**Key talking point:**
> "Sub-second query response on operational data. The ODS serves both the ML pipeline AND ad-hoc operational queries simultaneously. That's FR-9: API-based data access."

---

### Wrap-up: "ODS Requirements Covered" (2 min)

| Requirement | How We Demonstrated It |
|-------------|----------------------|
| FR-3: Real-time ingestion | ~1k events/sec -> Kafka -> 30s rolling snapshots -> Atlas |
| FR-5: Schema flexibility | Nested metric objects (avg/min/max/p95 per field) |
| FR-9: API-based access | MLflow REST API, Atlas Data API, Dashboard |
| FR-11: Feature enablement | Windowed aggregates as ML features |
| FR-12: Operational workflows | Atlas Trigger -> MLflow -> automated predictions |
| FR-14: Operational scope | 30s snapshots in ODS; raw data stays in Kafka/archive |

---

### Close: "Stop Demo" (30 sec)

**Click "Stop Demo" on the dashboard:**
- "Clean shutdown -- generator stops, Flink stops, data cleared."
- "Ready to run again any time with one click."

**Key closing point:**
> "Everything you've seen runs on 4 EC2 instances and MongoDB Atlas. Deploys in minutes, tears down with one command. This architecture scales horizontally -- MongoDB sharding, Kafka partitions, Flink parallelism. The ODS pattern works at any scale."

---

## Troubleshooting During Demo

| Problem | Quick Fix |
|---------|-----------|
| No new documents appearing | Check generator via dashboard or `ssh ubuntu@<gen-ip> "tail /var/log/generator.log"` |
| Predictions not flowing | Check trigger logs in Atlas UI > App Services > Logs (ap-southeast-2) |
| MLflow returning errors | `ssh ubuntu@<mlflow-ip> "curl localhost:5003/health"` |
| Dashboard not updating | `ssh ubuntu@<mlflow-ip> "tail /var/log/dashboard.log"` |
| Flink not processing | Check Flink UI :8081 -- may need restart.sh |
| SSH connection refused | Security group rule expired -- run start_demo.sh to update |

---

## Post-Demo Teardown

```bash
./infrastructure/scripts/teardown.sh
```

This destroys all EC2 resources. MongoDB Atlas data persists until manually dropped.
