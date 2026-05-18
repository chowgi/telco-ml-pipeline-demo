# Demo Walkthrough - Telco ODS Autonomous Networks

## Overview

The demo is now managed via a live web dashboard with Start/Stop buttons. The typical flow is:

1. Ensure infrastructure is deployed (or just update security group if already running)
2. Open the dashboard
3. Click "Start Demo"
4. Watch data flow in real-time
5. Click "Stop Demo" when done

---

## First-Time Setup (one-off, ~30 minutes)

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

### 3. Configure Atlas Trigger (ap-southeast-2)

1. Atlas > App Services > Create Application (name: `telco-ods-demo`, region: **ap-southeast-2**)
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

---

## Running the Demo

### If Infrastructure is Already Deployed

Just update the security group with your current IP:

```bash
# Quick SSH access update (or use the full start_demo.sh which does this automatically)
MY_IP=$(curl -s https://checkip.amazonaws.com)
# The start script handles this — just run:
./infrastructure/scripts/start_demo.sh
```

### Using the Dashboard (Recommended)

1. Open the dashboard: `http://<mlflow-ip>:8050`
2. Click **Start Demo** — this will:
   - Clear MongoDB collections
   - Restart Flink (hard-kill + fresh start)
   - Start the data generator (~1k events/sec)
3. Watch real-time data appear within ~30 seconds
4. Each cell tower emits a snapshot every 30 seconds
5. The Atlas Trigger fires on each insert, runs ML inference
6. Click **Stop Demo** when finished — this will:
   - Kill the generator
   - Hard-stop Flink
   - Clear MongoDB collections

### Using CLI Scripts (Alternative)

```bash
# Start the demo
./infrastructure/scripts/start_demo.sh

# Stop the demo
./infrastructure/scripts/stop_demo.sh
```

---

## Demo Endpoints

| Endpoint | URL | Purpose |
|----------|-----|---------|
| Dashboard | `http://<mlflow-ip>:8050` | Start/Stop controls, real-time visualization |
| Flink Web UI | `http://<flink-ip>:8081` | Job monitoring, task metrics |
| MLflow Tracking | `http://<mlflow-ip>:5002` | Experiment tracking, model registry |
| MLflow Inference | `http://<mlflow-ip>:5003/invocations` | REST prediction endpoint |

---

## Pipeline Details

- **Generator**: ~1k events/sec (1 thread, 100 per batch, 0.1s sleep between batches)
- **Flink**: Apache Flink (PyFlink 1.18) with 30-second emission timers per cell tower
- **Cell degradation**: 30% excellent, 12% degraded, 6% poor -- cells rotate every 30s
- **Flink lifecycle**: Must be hard-killed between runs (PyFlink/Beam leaves stale state)
- **Dashboard SSH**: Uses private IPs for intra-VPC communication (10.0.1.x)

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

## Troubleshooting

| Issue | Fix |
|-------|-----|
| No data appearing | Check generator: `ssh ubuntu@<gen-ip> "tail /var/log/generator.log"` |
| No predictions | Check trigger logs in App Services > Logs (ap-southeast-2) |
| MLflow not responding | `ssh ubuntu@<mlflow-ip> "sudo systemctl restart mlflow-tracking"` |
| Flink not processing | Check Flink UI at :8081 or `ssh ubuntu@<flink-ip> "tail /var/log/flink-job.log"` |
| Dashboard not loading | `ssh ubuntu@<mlflow-ip> "tail /var/log/dashboard.log"` |
| SSH connection refused | Security group expired -- run start_demo.sh or update manually |

## Key Talking Points

- MongoDB as ODS handles both the ingestion (writes) and serving (reads) in one platform
- No ETL pipeline needed between ODS and ML -- Atlas Triggers close the loop
- Schema flexibility means new telemetry fields don't require migration
- Time series collections provide optimized storage and query for windowed metrics
- Feast + MongoDB = production-grade feature store with sub-ms retrieval
- The entire pipeline is event-driven and real-time (no batch processing)
- One-click Start/Stop via the dashboard for seamless demo experience
