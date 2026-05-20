# Telco ODS - Autonomous Networks ML Pipeline

End-to-end streaming ML pipeline demonstrating MongoDB Atlas as an Operational Data Store for real-time network health anomaly detection in telco autonomous networks.

## Architecture

```
EC2 Generator ──> Kafka (EC2) ──> Apache Flink (PyFlink 1.18, 30s sliding window) ──> MongoDB Atlas (ODS)
  (~1k eps)      single broker     aggregates metrics per cell tower            |
                                                                                |
                  Live Dashboard <── predictions <── Atlas Trigger (ap-southeast-2) ──> MLflow EC2
                  (port 8050)       collection      (on insert)                         (/invocations)
                                                         |
                                               Feast Feature Store
                                               (MongoDB online store)
```

## Components

| Component | Location | Purpose |
|-----------|----------|---------|
| Data Generator | `data-generator/` | Kafka producer simulating cell tower telemetry (~1k eps, configurable) |
| Stream Processor | `flink-processor/` | Apache Flink (PyFlink 1.18) with 30s emission timers per cell tower |
| ML Model | `ml-model/` | RandomForest network health classifier served via MLflow |
| Live Dashboard | `dashboard/` | Web dashboard (port 8050) with Start/Stop Demo buttons |
| Feast Feature Store | `feast-feature-store/` | MongoDB online store for feature serving |
| Atlas Trigger | `atlas-trigger/` | Database trigger calling MLflow on new windowed data (ap-southeast-2) |
| Infrastructure | `infrastructure/` | CloudFormation + deploy/teardown scripts |

## Prerequisites

- AWS CLI configured with ap-southeast-2 access
- An EC2 Key Pair in ap-southeast-2 (see below)
- SSH client

### Create an EC2 Key Pair

```bash
aws ec2 create-key-pair \
  --region ap-southeast-2 \
  --key-name telco-demo \
  --query 'KeyMaterial' \
  --output text > telco-demo.pem

chmod 400 telco-demo.pem
```

Or create one in the AWS Console: EC2 > Key Pairs > Create key pair.

## Quick Start

```bash
# 1. Set required variables
export KEY_PAIR_NAME=your-key-pair-name
export SSH_KEY_PATH=./your-key.pem

# 2. Deploy everything (CloudFormation + app setup)
./infrastructure/scripts/deploy.sh

# 3. Validate pipeline is running
./infrastructure/scripts/validate.sh

# 4. Open the dashboard
open http://<mlflow-ip>:8050

# 5. Tear down when done
./infrastructure/scripts/teardown.sh
```

The deploy script will:
1. Deploy a CloudFormation stack with 4 EC2 instances (Kafka, Generator, Flink, MLflow)
2. Prompt you to whitelist IPs in MongoDB Atlas
3. Train and deploy the ML model
4. Set up Flink with helper scripts (restart.sh, stop.sh, start.sh) and start the job
5. Set up the generator with start.sh and begin producing events
6. Deploy the live dashboard to the MLflow instance
7. Print instructions for the Atlas trigger setup

## AWS Resources (ap-southeast-2)

| Resource | Instance Type | Purpose | Est. Cost |
|----------|---------------|---------|-----------|
| VPC | - | Networking | $0 |
| EC2 Kafka | t3.xlarge | Single-broker Kafka | ~$1.50/day |
| EC2 Generator | t3.small | ~1k events/sec producer (configurable) | ~$0.50/day |
| EC2 Processor | c5.4xlarge | Apache Flink (PyFlink 1.18) streaming | ~$6/day |
| EC2 MLflow | t3.large | Model tracking + serving + dashboard | ~$2/day |
| **Total** | | | **~$10/day** |

## Data Flow

1. **Generator** produces ~1k events/sec of simulated cell tower telemetry (50 towers across Sydney, Melbourne, Brisbane, Perth, Adelaide) with rotating cell-level degradation (30% excellent, 12% degraded, 6% poor; cells rotate every 30s)
2. **Kafka** buffers events on topic `telco-raw-telemetry` (12 partitions)
3. **Apache Flink** consumes from Kafka, applies 30-second emission timers per cell tower, computes avg/min/max/p95 for all metrics
4. **MongoDB Atlas** receives windowed aggregates into `windowed_network_metrics` time series collection
5. **Atlas Trigger** (ap-southeast-2) fires on insert, extracts avg features, calls MLflow `/invocations`
6. **MLflow** returns prediction (excellent/good/poor), trigger writes to `network_health_predictions`
7. **Live Dashboard** (port 8050) displays real-time results with Start/Stop controls

## Live Dashboard

The dashboard runs on the MLflow instance at port 8050 and provides:
- Start/Stop Demo buttons for one-click pipeline management
- Real-time visualization of predictions and network health
- Connection to Flink and Generator instances via private IPs (intra-VPC SSH)

The dashboard's `run_ssh` passes commands as `["ssh", ..., "bash", "-c", command]`.

## Model Details

**Type:** RandomForest Classifier (scikit-learn Pipeline with StandardScaler)

**Features (6):**
- `signal_strength_dbm` -- avg signal strength in window
- `throughput_mbps` -- avg throughput in window
- `latency_ms` -- avg latency in window
- `call_drop_rate_percent` -- avg call drop rate in window
- `packet_loss_percent` -- avg packet loss in window
- `jitter_ms` -- avg jitter in window

**Predictions:**
- `0` = excellent network health
- `1` = good network health
- `2` = poor network health

## Inference API

```bash
curl -X POST http://<mlflow-ip>:5003/invocations \
  -H "Content-Type: application/json" \
  -d '{"dataframe_records": [{"signal_strength_dbm": -65, "throughput_mbps": 75, "latency_ms": 35, "call_drop_rate_percent": 0.8, "packet_loss_percent": 0.8, "jitter_ms": 2.5}]}'

# Response: {"predictions": [1.0]}  (good)
```

## Feast Feature Store

Demonstrates the MongoDB online store integration for real-time feature serving:

```bash
cd feast-feature-store
pip install -r requirements.txt
python materialize.py  # Push features + demo retrieval
```

Features are materialized from `windowed_network_metrics` into the Feast online store (backed by MongoDB), enabling low-latency feature retrieval at inference time.

## Atlas Trigger Setup (Manual)

1. Go to Atlas > App Services > Create Application (region: ap-southeast-2)
2. Create a Database Trigger:
   - Collection: `ods_demo_db.windowed_network_metrics`
   - Operation: Insert
   - Full Document: enabled
3. Paste `atlas-trigger/trigger_function.js` as the function
4. Create a Value named `MLFLOW_ENDPOINT` with `http://<mlflow-ip>:5003/invocations`

## MongoDB Collections

| Collection | Type | Purpose |
|------------|------|---------|
| `windowed_network_metrics` | Time series | Flink-produced 30s window aggregates per cell |
| `network_health_predictions` | Standard | ML predictions from Atlas Trigger |
| `feast_online_features` | Standard | Feast online store |
| `training_windowed_metrics` | Standard | Synthetic training data |
| `customers` | Standard | Reference data (optional) |
| `cells` | Standard | Cell tower reference data (optional) |

## Flink Lifecycle Notes

Flink must be **hard-killed and restarted** between demo runs. PyFlink/Beam workers leave stale state after a `flink cancel`, so the pre-deployed helper scripts handle this:

- `/opt/flink-job/restart.sh` -- kills Flink, cleans PIDs, starts cluster, submits job
- `/opt/flink-job/stop.sh` -- kills Flink, cleans PIDs
- `/opt/flink-job/start.sh` -- just submits the job (assumes cluster is running)

## Troubleshooting

```bash
# Check if generator is producing
ssh -i $SSH_KEY_PATH ubuntu@<generator-ip> "tail -5 /var/log/generator.log"

# Check Flink job
ssh -i $SSH_KEY_PATH ubuntu@<flink-ip> "tail -5 /var/log/flink-job.log"

# Check Flink via REST API
curl http://<flink-ip>:8081/jobs/overview

# Check MLflow serving
curl http://<mlflow-ip>:5003/health

# Check dashboard
curl http://<mlflow-ip>:8050/

# Check MongoDB for data
# Use mongosh or Atlas UI to query:
#   db.windowed_network_metrics.countDocuments()
#   db.network_health_predictions.find().sort({timestamp: -1}).limit(5)
```
