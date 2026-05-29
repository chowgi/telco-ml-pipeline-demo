# Telco ODS - Autonomous Networks ML Pipeline

End-to-end streaming ML pipeline demonstrating MongoDB Atlas as an Operational Data Store for real-time network health anomaly detection in telco autonomous networks.

This demo showcases MongoDB's open-source integration capabilities — Apache Kafka, Apache Flink, MLflow, and Feast all connect natively to MongoDB Atlas, forming a production-grade ML pipeline without proprietary glue.

## Architecture

```
EC2 Generator ──> Kafka (EC2) ──> Apache Flink (PyFlink 1.18, 10s windows) ──> MongoDB Atlas (ODS)
  (~1k eps)      single broker     aggregates metrics per cell tower            |
                                                                                |
                  Live Dashboard <── predictions <── Atlas Trigger ──> MLflow EC2
                  (port 8050)       collection      (on insert)       (/invocations)
                                                         |
                                               Feast Feature Store
                                               (MongoDB online store)
```

## Open-Source Integrations

This demo highlights MongoDB's native integration with the open-source ML/data ecosystem:

| Integration | How It Connects |
|-------------|-----------------|
| **Apache Kafka** | Flink consumes from Kafka, writes to MongoDB via PyMongo |
| **Apache Flink** | PyFlink keyed process function with direct MongoDB sink |
| **MLflow** | Model registry + serving; Atlas Trigger calls the REST API |
| **Feast** | MongoDB as the online feature store (`MongoDBOnlineStore`) |

No proprietary connectors or middleware — each tool connects to Atlas using its standard MongoDB driver or native integration.

## Components

| Component | Location | Purpose |
|-----------|----------|---------|
| Data Generator | `data-generator/` | Kafka producer simulating cell tower telemetry (~1k eps, configurable) |
| Stream Processor | `flink-processor/` | Apache Flink (PyFlink 1.18) with 10s emission timers per cell tower |
| ML Model | `ml-model/` | RandomForest network health classifier served via MLflow |
| Live Dashboard | `dashboard/` | Web dashboard (port 8050) with Start/Stop Demo buttons |
| Feast Feature Store | `feast-feature-store/` | MongoDB online store for feature serving |
| Atlas Trigger | `atlas-trigger/` | Database trigger calling MLflow on new windowed data (ap-southeast-2) |
| Infrastructure | `infrastructure/` | CloudFormation + deploy/teardown scripts |

## Prerequisites

- AWS CLI configured for ap-southeast-2
- EC2 Key Pair in ap-southeast-2
- SSH client
- `.env` file with `MONGODB_URI`, `ATLAS_PUBLIC_KEY`, `ATLAS_PRIVATE_KEY`, `ATLAS_PROJECT_ID`

## Quick Start

```bash
# 1. Set required variables
export KEY_PAIR_NAME=your-key-pair-name
export SSH_KEY_PATH=./your-key.pem

# 2. Deploy everything (CloudFormation + app setup, ~5 min)
./infrastructure/scripts/deploy.sh

# 3. Start the pipeline
./infrastructure/scripts/start_demo.sh

# 4. Open the dashboard
open http://<mlflow-ip>:8050

# 5. Tear down when done
./infrastructure/scripts/teardown.sh
```

The deploy script will:
1. Deploy a CloudFormation stack with 5 EC2 instances (Kafka, Generator, Flink, MLflow, Feast)
2. Whitelist EC2 IPs in MongoDB Atlas automatically
3. Train and deploy the ML model
4. Configure Flink with helper scripts and the streaming job
5. Configure the data generator
6. Deploy the live dashboard
7. Set up Feast feature store (apply + initial materialization)
8. Create/update the Atlas Trigger via the App Services Admin API

## AWS Resources (ap-southeast-2)

| Resource | Instance Type | Purpose | Est. Cost |
|----------|---------------|---------|-----------|
| VPC | - | Networking | $0 |
| EC2 Kafka | t3.xlarge | Single-broker Kafka | ~$1.50/day |
| EC2 Generator | t3.small | ~1k events/sec producer | ~$0.50/day |
| EC2 Processor | c5.4xlarge | Apache Flink (PyFlink 1.18) streaming | ~$6/day |
| EC2 MLflow | t3.large | Model tracking + serving + dashboard | ~$2/day |
| EC2 Feast | t3.small | Feast feature server + materialization | ~$0.50/day |
| **Total** | | | **~$10.50/day** |

## Data Flow

1. **Generator** produces ~1k events/sec of simulated cell tower telemetry (50 towers across Sydney, Melbourne, Brisbane, Perth, Adelaide) with rotating cell-level degradation (30% excellent, 12% degraded, 6% poor; cells rotate every 30s)
2. **Kafka** buffers events on topic `telco-raw-telemetry` (12 partitions)
3. **Apache Flink** consumes from Kafka, applies 10-second emission timers per cell tower, computes avg/min/max/p95 for all metrics
4. **MongoDB Atlas** receives windowed aggregates into `windowed_network_metrics`
5. **Atlas Trigger** fires on insert, extracts avg features, calls MLflow `/invocations`
6. **MLflow** returns prediction (excellent/good/poor), trigger writes to `network_health_predictions`
7. **Live Dashboard** (port 8050) displays real-time results with Start/Stop controls

## Demo Flow

1. Deploy: `./infrastructure/scripts/deploy.sh` (~5 min)
2. Open the dashboard: `http://<mlflow-ip>:8050`
3. Click **Start Demo** — generator, Flink, and inference all begin
4. Data appears within ~10 seconds (50 cells, predictions flowing in real-time)
5. Show the Atlas Trigger in action: Atlas UI > App Services > Logs
6. Show Flink windowing: Flink UI at `http://<flink-ip>:8081`
7. Show Feast feature store panel on the dashboard (MongoDB online store)
8. Click **Stop Demo** when finished
9. Tear down: `./infrastructure/scripts/teardown.sh`

## Live Dashboard

The dashboard runs on the MLflow instance at port 8050 and provides:
- Start/Stop Demo buttons for one-click pipeline management
- Real-time visualization of predictions and network health
- Architecture & Details slides (linked from service bar)
- Connection to Flink and Generator instances via private IPs (intra-VPC SSH)

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

## Atlas Trigger

The deploy script automatically creates and configures the Atlas Trigger via the App Services Admin API. No manual setup needed. The trigger watches `ods_demo_db.windowed_network_metrics` for inserts, calls MLflow, and writes predictions back to Atlas.

## MongoDB Collections

| Collection | Type | Purpose |
|------------|------|---------|
| `windowed_network_metrics` | Standard | Flink-produced 10s window aggregates per cell |
| `network_health_predictions` | Standard | ML predictions from Atlas Trigger |
| `telco_ods_online` | Standard | Feast online store (materialized features) |
| `training_windowed_metrics` | Standard | Synthetic training data for model |

## Flink Lifecycle Notes

Flink must be **restarted** between demo runs — PyFlink/Beam workers leak direct buffers after a `flink cancel`. The Start/Stop Demo buttons and scripts handle this automatically:

- `/opt/flink-job/restart.sh` -- restarts Flink cluster, submits job
- `/opt/flink-job/stop.sh` -- cancels running jobs (leaves cluster up)

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
