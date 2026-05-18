# Live Demo Script - Telco ODS Autonomous Networks

Target audience: Telstra engineering/architecture team
Duration: 15-20 minutes
Goal: Prove MongoDB Atlas as ODS for real-time ML-driven network health monitoring

---

## Pre-Demo Checklist (do 30 min before)

```bash
# 1. Deploy infrastructure (skip if already running)
export KEY_PAIR_NAME=bennyk_aws_key
export SSH_KEY_PATH=~/.ssh/bennyk_aws_key.pem  # or wherever your key is
./infrastructure/scripts/deploy.sh

# 2. Set up Atlas trigger (if not already done)
export ATLAS_PUBLIC_KEY=<your-key>
export ATLAS_PRIVATE_KEY=<your-key>
./atlas-trigger/setup_trigger.sh

# 3. Verify pipeline is running
./infrastructure/scripts/validate.sh

# 4. Open these tabs in browser:
#    - Atlas Charts dashboard (auto-refreshing)
#    - Atlas UI > Browse Collections > ods_demo_db
#    - MLflow tracking UI: http://<mlflow-ip>:5002
```

---

## Demo Flow

### Slide 1: "The Problem" (2 min)
*Talking points — no live demo yet*

- Telstra operates 50,000+ cell towers, generating millions of telemetry events per second
- Current approach: batch analytics with 30-60 min delay
- Need: real-time anomaly detection, sub-second query latency, operational ML

### Slide 2: "Architecture Overview" (2 min)
*Show architecture diagram*

```
Generator (80k eps) → Kafka → Stream Processor (5-min windows) → MongoDB Atlas (ODS)
                                                                        |
                              Charts ← predictions ← Atlas Trigger → MLflow
```

Key talking points:
- MongoDB Atlas IS the ODS — not just a cache or sink
- Schema-flexible: windowed metrics with nested stats (avg/min/max/p95)
- Atlas Triggers enable event-driven ML without external orchestration
- Single platform: storage, compute (triggers), serving (Charts), all in Atlas

---

### Live Demo: "Data Flowing" (3 min)

**Terminal 1 — Show generator throughput:**
```bash
SSH_KEY=~/.ssh/bennyk_aws_key.pem
GENERATOR_IP=<from deploy output>
ssh -i $SSH_KEY ubuntu@$GENERATOR_IP "tail -5 /var/log/generator.log"
```
*Expected output: ~17k events/sec*

**Atlas UI — Show documents arriving:**
- Browse Collections → `ods_demo_db.windowed_network_metrics`
- Click a document, expand nested metric objects
- Point out: "Every 30 seconds, each cell tower emits a rolling statistical snapshot"
- Refresh the page — new documents arriving continuously

**Key talking point:**
> "We're ingesting 17,000+ events per second from 50 simulated cell towers.
> The stream processor computes rolling 5-minute statistics — avg, min, max, p95
> for every metric — and emits per-cell snapshots every 30 seconds directly to Atlas.
> That's ~100 documents per minute, each triggering an ML prediction. This is FR-3: real-time ingestion."

---

### Live Demo: "ML Inference" (3 min)

**Terminal — Call the inference endpoint live:**
```bash
MLFLOW_IP=<from deploy output>

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
> "The model takes the rolling averages and classifies network health in real-time.
> This runs automatically via an Atlas Database Trigger — every 30 seconds per cell,
> the trigger fires, calls the model, and stores the prediction.
> Zero orchestration code. That's FR-12: integration with operational workflows."

---

### Live Demo: "Atlas Trigger in Action" (2 min)

**Atlas UI — Show predictions collection:**
- Browse Collections → `ods_demo_db.network_health_predictions`
- Sort by timestamp descending
- Expand a prediction document — show input features, label, cell_id, region

**Key talking point:**
> "These predictions are being written automatically by the Atlas Trigger — ~100 per minute.
> No Airflow, no cron, no Lambda — Atlas itself is the event-driven compute layer.
> The trigger fires within milliseconds of each new cell snapshot arriving."

---

### Live Demo: "Real-Time Dashboard" (3 min)

**Atlas Charts — Show the dashboard (auto-refreshing):**
- Health distribution donut: "67% excellent, 28% good, 5% poor — matches our 5% anomaly injection"
- Anomalies by region: "All regions have similar rates — this is simulated, but in production you'd see hotspots"
- Signal vs Latency scatter: "Notice the cluster of points in the poor zone — those are our injected anomalies"

**Key talking point:**
> "This dashboard is built on Atlas Charts with Change Streams. It updates every 10 seconds.
> In production, this becomes the network operations center view.
> All from the same ODS — no ETL to a separate analytics platform."

---

### Live Demo: "Query the ODS" (2 min)

**Atlas UI — Run aggregation in Data Explorer or show mongosh:**
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
> "Sub-second query response on operational data. This is FR-9: API-based data access.
> The ODS serves both the ML pipeline AND ad-hoc operational queries simultaneously."

---

### Wrap-up: "ODS Requirements Covered" (2 min)

| Requirement | How We Demonstrated It |
|-------------|----------------------|
| FR-3: Real-time ingestion | 17k+ events/sec → Kafka → 30s rolling snapshots → Atlas |
| FR-5: Schema flexibility | Nested metric objects (avg/min/max/p95 per field) |
| FR-9: API-based access | MLflow REST API, Atlas Data API, Charts |
| FR-11: Feature enablement | Windowed aggregates as ML features |
| FR-12: Operational workflows | Atlas Trigger → MLflow → automated predictions |
| FR-14: Operational scope | 30s snapshots in ODS; raw data stays in Kafka/archive |

---

## Troubleshooting During Demo

| Problem | Quick Fix |
|---------|-----------|
| No new documents appearing | Check generator: `ssh ubuntu@<gen-ip> "pgrep -f generator && tail -1 /var/log/generator.log"` |
| Predictions not flowing | Check trigger logs in Atlas UI > App Services > TelcoODS > Logs |
| MLflow returning errors | `ssh ubuntu@<mlflow-ip> "curl localhost:5003/health"` |
| Charts not updating | Check auto-refresh is on (top-right toggle in Charts) |
| SSH connection refused | Security group rule expired — re-run: `MY_IP=$(curl -s checkip.amazonaws.com); aws ec2 authorize-security-group-ingress --group-id <sg-id> --protocol tcp --port 22 --cidr ${MY_IP}/32 --region ap-southeast-2` |

---

## Post-Demo Teardown

```bash
./infrastructure/scripts/teardown.sh
```

This destroys all EC2 resources. MongoDB Atlas data and the Charts dashboard persist.
