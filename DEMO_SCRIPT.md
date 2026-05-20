# Live Demo Script - Telco ODS Autonomous Networks

Target audience: Engineering/architecture teams evaluating ODS platforms
Duration: 15-20 minutes
Goal: Prove MongoDB Atlas as ODS for real-time ML-driven network health monitoring

---

## Pre-Demo Checklist (5 min before)

```bash
# 1. Deploy (if not already running)
./infrastructure/scripts/deploy.sh

# 2. Open these tabs in browser:
#    - Dashboard: http://<mlflow-ip>:8050
#    - Flink UI: http://<flink-ip>:8081
#    - Atlas UI > Browse Collections > ods_demo_db
#    - MLflow tracking UI: http://<mlflow-ip>:5002
```

The demo starts with the dashboard showing an empty state. You will click Start during the presentation.

---

## Demo Flow

### Opening: "The Problem" (2 min)
*Talking points — no live demo yet*

- Telco networks operate 50,000+ cell towers, generating millions of telemetry events per second
- Current approach: batch analytics with 30-60 min delay
- Need: real-time anomaly detection, sub-second query latency, operational ML
- "Let me show you how MongoDB Atlas as an ODS enables this in real-time."

---

### "Why MongoDB for the ODS?" (2 min)
*Key differentiation — this is the pitch*

Other databases can store time-series data. What they can't do is **react** to it:

- **Atlas Triggers (Change Streams)** — the act of writing data fires ML inference. No Airflow, no Step Functions, no Lambda. Data arrives → model runs → prediction stored. Cassandra and TimescaleDB can't do this natively.
- **One cluster, three roles** — ODS writes, feature store, and predictions in one platform. Without MongoDB you need Cassandra + Redis + Postgres (three systems to operate).
- **Document model** — nested per-metric stats in one document. New KPI? Add a field. No migrations, no downtime.
- **Operational + ML on the same data** — Feast online store backed by Atlas. No separate caching tier.

> "The ODS doesn't just *store* things — it *does* things."

---

### "Architecture Overview" (2 min)
*Show Details page (slide 3) or the architecture diagram*

```
Generator (~1k eps) -> Kafka -> Apache Flink (30s windows) -> MongoDB Atlas (ODS)
                                                                    |
                         Dashboard <- predictions <- Atlas Trigger -> MLflow
```

Key points:
- MongoDB Atlas IS the ODS — not just a cache or sink
- Schema-flexible: windowed metrics with nested stats (avg/min/max/p95)
- Atlas Triggers enable event-driven ML without external orchestration
- Single platform: storage, compute (triggers), serving, all in Atlas

---

### Live Demo: "One-Click Pipeline Start" (3 min)

**Show the dashboard (empty state):**
- "This is our operations dashboard. Right now the pipeline is stopped — no data flowing."
- "Watch me start an entire ML pipeline with one click."

**Click the "Start Demo" button:**
- "Behind the scenes, this is:"
  - "Restarting Apache Flink (clean state for the streaming job)"
  - "Starting the data generator at ~1,000 events per second"
  - "Clearing previous results"

**Wait ~30 seconds for data to appear:**
- "Each cell tower emits a statistical snapshot every 30 seconds."
- "50 towers means ~100 documents per minute hitting MongoDB, each triggering ML inference."

> "One click. Generator, Flink, inference — all running. This is what operational simplicity looks like."

---

### Live Demo: "Data Flowing" (3 min)

**Dashboard — show real-time data appearing:**
- Point out predictions appearing in real-time
- Show the health distribution updating
- "The model classifies each cell tower as excellent, good, or poor in real-time"

**Flink Web UI (separate tab):**
- Show the running job
- Point out throughput metrics, records processed
- "Flink computes rolling statistics — avg, min, max, p95 — for every metric on every cell tower"

**Atlas UI — Show documents arriving:**
- Browse Collections -> `ods_demo_db.windowed_network_metrics`
- Expand a document, show nested metric objects
- "Every 30 seconds per cell, a rich statistical snapshot lands in Atlas"

> "The schema is fully flexible — nested objects, arrays, no migration required. Try adding a new metric to a relational time-series DB without downtime."

---

### Live Demo: "Atlas Trigger in Action" (3 min)

**Atlas UI — Show predictions collection:**
- Browse Collections -> `ods_demo_db.network_health_predictions`
- Sort by timestamp descending
- Expand a prediction document — show input features, label, cell_id, region

> "These predictions are written automatically by the Atlas Trigger — ~100 per minute. No Airflow, no cron, no Lambda. Atlas itself is the event-driven compute layer. The trigger fires within milliseconds of each new cell snapshot arriving."

**This is the key differentiator moment:**
> "With any other database, you'd need an external scheduler or event bus to make this happen. With Atlas, the database reacts to its own writes. That's what makes it an *operational* data store, not just a *storage* layer."

---

### Live Demo: "Feature Store" (2 min)

**Dashboard — show Feast panel:**
- Point out cells materialized, retrieval time (<15ms)
- "Feast is using MongoDB as the online store — same cluster, different collection"
- "No Redis, no DynamoDB — one less system to manage"

> "The same Atlas cluster that handles operational writes also serves ML features at sub-15ms latency. That's consolidation without compromise."

---

### Live Demo: "MLflow Model Management" (1 min)

**Open MLflow UI (http://<mlflow-ip>:5002):**
- Show experiment tracking, model registry
- "Full model lifecycle — versioning, metrics, artifacts"

---

### Wrap-up: "The MongoDB Difference" (2 min)

| What You Need | Without MongoDB | With Atlas |
|---------------|-----------------|------------|
| Ingest at speed | Cassandra / InfluxDB | Atlas |
| React to data | + Lambda + EventBridge | Atlas Triggers (native) |
| Serve ML features | + Redis / DynamoDB | Feast + Atlas (same cluster) |
| Store predictions | + Postgres | Atlas (same cluster) |
| Query operationally | + yet another API | MongoDB Query API |
| **Total systems** | **4-5** | **1** |

> "One platform. Ingest, react, serve, query. That's the ODS value proposition."

---

### Close: "Stop Demo" (30 sec)

**Click "Stop Demo" on the dashboard:**
- "Clean shutdown — generator stops, Flink stops, data cleared."
- "Ready to run again any time with one click."

> "Everything you've seen runs on 4 EC2 instances and MongoDB Atlas. Deploys in 5 minutes, tears down with one command. This architecture scales horizontally — MongoDB sharding, Kafka partitions, Flink parallelism."

---

## Troubleshooting During Demo

| Problem | Quick Fix |
|---------|-----------|
| No new documents appearing | Check generator via dashboard or `ssh ubuntu@<gen-ip> "tail /var/log/generator.log"` |
| Predictions not flowing | Check trigger logs in Atlas UI > App Services > Logs (ap-southeast-2) |
| MLflow returning errors | `ssh ubuntu@<mlflow-ip> "curl localhost:5003/health"` — services auto-restart in ~5s |
| Dashboard not updating | Hard refresh browser (Ctrl+Shift+R) |
| Flink not processing | Check Flink UI :8081 — may need Stop → Start cycle |

---

## Post-Demo Teardown

```bash
./infrastructure/scripts/teardown.sh
```

This destroys all EC2 resources. MongoDB Atlas data persists until manually dropped.
