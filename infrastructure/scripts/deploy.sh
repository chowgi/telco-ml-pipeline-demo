#!/bin/bash
# Deploy the full Telco ODS streaming ML pipeline demo.
# All instances bootstrap from scratch via CloudFormation userdata.
# Flink binary pulled from S3 (fast, in-region) with Apache fallback.
#
# Total deploy time: ~5 min (Flink PyFlink pip install is the bottleneck).
#
# Prerequisites:
#   - .env file in project root (ATLAS_PUBLIC_KEY, ATLAS_PRIVATE_KEY, MONGODB_URI)
#   - bennyk_aws_key.pem in project root
#   - AWS CLI configured for ap-southeast-2
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
CFN_TEMPLATE="$PROJECT_ROOT/infrastructure/cloudformation/stack.yaml"
STACK_NAME="telco-ods-demo"
REGION="ap-southeast-2"

echo "============================================================"
echo "Telco ODS - Autonomous Networks ML Pipeline Demo"
echo "  Full bootstrap deploy (no AMIs)"
echo "============================================================"
echo ""

# Load .env (Atlas API keys, MongoDB URI)
if [ -f "$PROJECT_ROOT/.env" ]; then
  export $(grep -v '^#' "$PROJECT_ROOT/.env" | xargs)
  echo "  Loaded .env"
else
  echo "  Error: No .env file found. Required for MONGODB_URI and Atlas keys."
  exit 1
fi

# Prerequisites
for cmd in aws ssh scp; do
  if ! command -v $cmd &> /dev/null; then
    echo "Error: $cmd is required but not installed."
    exit 1
  fi
done

# SSH key
KEY_PAIR_NAME="${KEY_PAIR_NAME:-bennyk_aws_key}"
SSH_KEY_PATH="${SSH_KEY_PATH:-$PROJECT_ROOT/bennyk_aws_key.pem}"
if [ ! -f "$SSH_KEY_PATH" ]; then
  echo "Error: SSH key not found at $SSH_KEY_PATH"
  exit 1
fi
SSH_OPTS="-o ConnectTimeout=15 -o ServerAliveInterval=5 -o StrictHostKeyChecking=no -i $SSH_KEY_PATH"

# Auto-detect public IP
MY_IP=$(curl -s --max-time 5 https://checkip.amazonaws.com)
ALLOWED_SSH_CIDR="${MY_IP}/32"
echo "  SSH access from: $MY_IP"
echo "  Key pair: $KEY_PAIR_NAME"

# Deploy CloudFormation
echo ""
echo "[1/5] Deploying CloudFormation stack..."
aws cloudformation deploy \
  --template-file "$CFN_TEMPLATE" \
  --stack-name "$STACK_NAME" \
  --region "$REGION" \
  --parameter-overrides \
    KeyPairName=$KEY_PAIR_NAME \
    AllowedSSHCidr=$ALLOWED_SSH_CIDR \
  --capabilities CAPABILITY_IAM \
  --no-fail-on-empty-changeset

echo "  Stack created."

# Get outputs
echo ""
echo "[2/5] Retrieving instance IPs..."
get_output() {
  aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$REGION" \
    --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue" --output text
}

KAFKA_PUBLIC_IP=$(get_output "KafkaPublicIP")
KAFKA_PRIVATE_IP=$(get_output "KafkaPrivateIP")
GENERATOR_IP=$(get_output "GeneratorPublicIP")
GENERATOR_PRIVATE_IP=$(get_output "GeneratorPrivateIP")
FLINK_IP=$(get_output "FlinkPublicIP")
FLINK_PRIVATE_IP=$(get_output "FlinkPrivateIP")
MLFLOW_IP=$(get_output "MLflowPublicIP")
FEAST_IP=$(get_output "FeastPublicIP")
FEAST_PRIVATE_IP=$(get_output "FeastPrivateIP")

echo "  Kafka:     $KAFKA_PUBLIC_IP (private: $KAFKA_PRIVATE_IP)"
echo "  Generator: $GENERATOR_IP (private: $GENERATOR_PRIVATE_IP)"
echo "  Flink:     $FLINK_IP (private: $FLINK_PRIVATE_IP)"
echo "  MLflow:    $MLFLOW_IP"
echo "  Feast:     $FEAST_IP (private: $FEAST_PRIVATE_IP)"

# Wait for userdata to complete on all instances
echo ""
echo "[3/5] Waiting for instances to bootstrap (~5 min)..."
echo "  (Flink PyFlink install is the bottleneck)"

# Poll until all instances report ready (max 8 min)
for i in $(seq 1 32); do
  KAFKA_OK=$(ssh $SSH_OPTS ubuntu@$KAFKA_PUBLIC_IP "cat /tmp/kafka-status 2>/dev/null" 2>/dev/null || echo "")
  FLINK_OK=$(ssh $SSH_OPTS ubuntu@$FLINK_IP "cat /tmp/flink-status 2>/dev/null" 2>/dev/null || echo "")
  GEN_OK=$(ssh $SSH_OPTS ubuntu@$GENERATOR_IP "cat /tmp/generator-status 2>/dev/null" 2>/dev/null || echo "")
  MLFLOW_OK=$(ssh $SSH_OPTS ubuntu@$MLFLOW_IP "cat /tmp/mlflow-status 2>/dev/null" 2>/dev/null || echo "")
  FEAST_OK=$(ssh $SSH_OPTS ubuntu@$FEAST_IP "cat /tmp/feast-status 2>/dev/null" 2>/dev/null || echo "")

  READY=0
  [ "$KAFKA_OK" = "KAFKA_READY" ] && READY=$((READY+1))
  [ "$FLINK_OK" = "FLINK_READY" ] && READY=$((READY+1))
  [ "$GEN_OK" = "GENERATOR_READY" ] && READY=$((READY+1))
  [ "$MLFLOW_OK" = "MLFLOW_READY" ] && READY=$((READY+1))
  [ "$FEAST_OK" = "FEAST_READY" ] && READY=$((READY+1))

  echo "  [$((i*15))s] $READY/5 ready (kafka:${KAFKA_OK:-pending} flink:${FLINK_OK:-pending} gen:${GEN_OK:-pending} mlflow:${MLFLOW_OK:-pending} feast:${FEAST_OK:-pending})"

  if [ $READY -eq 5 ]; then
    echo "  All instances ready!"
    break
  fi
  if [ $i -eq 32 ]; then
    echo "  Warning: Not all instances ready after 8 min. Check userdata logs."
  fi
  sleep 15
done

# Upload code and configure
echo ""
echo "[4/5] Uploading code and configuring..."

# Fix ownership (userdata runs as root, scp runs as ubuntu)
ssh $SSH_OPTS ubuntu@$FLINK_IP "sudo chown -R ubuntu:ubuntu /opt/flink-job /opt/flink-env" 2>/dev/null
ssh $SSH_OPTS ubuntu@$MLFLOW_IP "sudo chown -R ubuntu:ubuntu /opt/mlflow /opt/dashboard" 2>/dev/null
ssh $SSH_OPTS ubuntu@$GENERATOR_IP "sudo chown -R ubuntu:ubuntu /opt/telco-generator" 2>/dev/null

# Upload Flink job
scp $SSH_OPTS -r "$PROJECT_ROOT/flink-processor/"* ubuntu@${FLINK_IP}:/opt/flink-job/ 2>/dev/null
ssh $SSH_OPTS ubuntu@$FLINK_IP "source /opt/flink-env/bin/activate && cd /opt/flink-job && pip install -r requirements.txt -q 2>/dev/null" 2>/dev/null
echo "  Flink job uploaded"

# Create Flink helper scripts
ssh $SSH_OPTS ubuntu@$FLINK_IP 'cat > /opt/flink-job/restart.sh << '\''SCRIPT'\''
#!/bin/bash
set -e
# Cancel existing jobs
for JOB_ID in $(/opt/flink/bin/flink list -r 2>/dev/null | grep -oP "[a-f0-9]{32}"); do
  /opt/flink/bin/flink cancel $JOB_ID 2>/dev/null || true
done
pkill -f "flink_job.py" || true
sleep 2

# Ensure cluster is running (via systemd so it runs as ubuntu)
if ! /opt/flink/bin/flink list 2>&1 | grep -q "Running\|No running"; then
  sudo systemctl restart flink
  sleep 5
fi

# Submit job
cd /opt/flink-job
source /opt/flink-env/bin/activate
export $(cat /opt/flink-job-config.env | xargs)
/opt/flink/bin/flink run -py flink_job.py -pyexec /opt/flink-env/bin/python3 >> /var/log/flink-job.log 2>&1 &
sleep 5
RUNNING=$(/opt/flink/bin/flink list -r 2>/dev/null | grep -c "RUNNING" || echo "0")
echo "Flink: $RUNNING running"
SCRIPT
chmod +x /opt/flink-job/restart.sh' 2>/dev/null

ssh $SSH_OPTS ubuntu@$FLINK_IP 'cat > /opt/flink-job/stop.sh << '\''SCRIPT'\''
#!/bin/bash
# Cancel all running Flink jobs but leave the cluster up
for JOB_ID in $(/opt/flink/bin/flink list -r 2>/dev/null | grep -oP "[a-f0-9]{32}"); do
  /opt/flink/bin/flink cancel $JOB_ID 2>/dev/null
done
pkill -f "flink_job.py" || true
echo "Flink jobs cancelled (cluster still running)"
SCRIPT
chmod +x /opt/flink-job/stop.sh' 2>/dev/null
echo "  Flink helper scripts created"

# Upload Generator
scp $SSH_OPTS -r "$PROJECT_ROOT/data-generator/"* ubuntu@${GENERATOR_IP}:/opt/telco-generator/ 2>/dev/null
ssh $SSH_OPTS ubuntu@$GENERATOR_IP "cd /opt/telco-generator && source venv/bin/activate && pip install -r requirements.txt -q 2>/dev/null" 2>/dev/null
echo "  Generator uploaded"

# Create Generator start script
ssh $SSH_OPTS ubuntu@$GENERATOR_IP 'cat > /opt/telco-generator/start.sh << '\''SCRIPT'\''
#!/bin/bash
pkill -f generator.py || true
sleep 2
cd /opt/telco-generator
source venv/bin/activate
source env.sh
nohup python -u generator.py >> /var/log/generator.log 2>&1 < /dev/null &
sleep 3
pgrep -f generator.py > /dev/null && echo "Generator started" || echo "ERROR: Generator failed"
SCRIPT
chmod +x /opt/telco-generator/start.sh' 2>/dev/null
echo "  Generator start script created"

# Upload MLflow model + dashboard
scp $SSH_OPTS -r "$PROJECT_ROOT/ml-model/"* ubuntu@${MLFLOW_IP}:/opt/mlflow/app/ 2>/dev/null
scp $SSH_OPTS -r "$PROJECT_ROOT/dashboard/"* ubuntu@${MLFLOW_IP}:/opt/dashboard/ 2>/dev/null
scp $SSH_OPTS "$SSH_KEY_PATH" ubuntu@${MLFLOW_IP}:/opt/dashboard/bennyk_aws_key.pem 2>/dev/null
ssh $SSH_OPTS ubuntu@$MLFLOW_IP "chmod 400 /opt/dashboard/bennyk_aws_key.pem" 2>/dev/null
echo "  MLflow model + dashboard uploaded"

# Install dashboard deps
ssh $SSH_OPTS ubuntu@$MLFLOW_IP "source /opt/mlflow/venv/bin/activate && pip install -r /opt/dashboard/requirements.txt -q 2>/dev/null" 2>/dev/null

# Train model and start serving
ssh $SSH_OPTS ubuntu@$MLFLOW_IP "
cd /opt/mlflow/app
source /opt/mlflow/venv/bin/activate
export MLFLOW_TRACKING_URI=http://localhost:5002
export MONGODB_URI='${MONGODB_URI}'
pip install -r requirements.txt -q 2>/dev/null
python generate_training_data.py
python train_model.py
" 2>/dev/null
echo "  Model trained and registered"

# Start model serving
ssh $SSH_OPTS ubuntu@$MLFLOW_IP "
source /opt/mlflow/venv/bin/activate
export MLFLOW_TRACKING_URI=http://localhost:5002
nohup mlflow models serve -m 'models:/telco_ods_network_health_classifier/1' --host 0.0.0.0 --port 5003 --no-conda >> /var/log/mlflow-serve.log 2>&1 < /dev/null &
" 2>/dev/null
echo "  Model serving started"

# Start dashboard
ssh $SSH_OPTS ubuntu@$MLFLOW_IP "
source /opt/dashboard/env.sh
cd /opt/dashboard
source /opt/mlflow/venv/bin/activate
nohup python app.py >> /var/log/dashboard.log 2>&1 < /dev/null &
" 2>/dev/null
echo "  Dashboard started"

# Upload and configure Feast
echo ""
echo "  Configuring Feast feature server..."
ssh $SSH_OPTS ubuntu@$FEAST_IP "sudo chown -R ubuntu:ubuntu /opt/feast /opt/feast-env" 2>/dev/null
scp $SSH_OPTS -r "$PROJECT_ROOT/feast-feature-store/"* ubuntu@${FEAST_IP}:/opt/feast/ 2>/dev/null

# Create env file for Feast (do this first so we can source it for substitution)
ssh $SSH_OPTS ubuntu@$FEAST_IP "cat > /opt/feast/env.sh << ENVEOF
export MONGODB_URI='${MONGODB_URI}'
ENVEOF" 2>/dev/null

# Substitute MONGODB_URI into feature_store.yaml
# Uses Python to avoid sed issues with & and special chars in connection strings
ssh $SSH_OPTS ubuntu@$FEAST_IP 'source /opt/feast/env.sh && python3 -c "
import pathlib, os
p = pathlib.Path(\"/opt/feast/feature_store.yaml\")
content = p.read_text().replace(chr(36)+\"{MONGODB_URI}\", os.environ[\"MONGODB_URI\"])
p.write_text(content)
"' 2>/dev/null

# Create empty parquet with schema (feast apply needs it for source validation)
ssh $SSH_OPTS ubuntu@$FEAST_IP "
source /opt/feast-env/bin/activate
mkdir -p /opt/feast/data
python3 << 'PYEOF'
import pyarrow as pa
import pyarrow.parquet as pq
schema = pa.schema([
    ('cell_id', pa.string()),
    ('window_end', pa.timestamp('us')),
    ('avg_signal_strength_dbm', pa.float64()),
    ('avg_throughput_mbps', pa.float64()),
    ('avg_latency_ms', pa.float64()),
    ('avg_call_drop_rate_percent', pa.float64()),
    ('avg_packet_loss_percent', pa.float64()),
    ('avg_jitter_ms', pa.float64()),
    ('event_count', pa.int64()),
    ('anomaly_event_count', pa.int64()),
    ('region', pa.string()),
])
pq.write_table(pa.table({name: [] for name in schema.names}, schema=schema), '/opt/feast/data/windowed_metrics.parquet')
PYEOF
" 2>/dev/null

# Downgrade protobuf (Feast 0.63 uses MessageToDict float_precision removed in protobuf 5+)
ssh $SSH_OPTS ubuntu@$FEAST_IP "source /opt/feast-env/bin/activate && pip install 'protobuf>=4.25,<5' -q" 2>/dev/null

# Run feast apply to register feature views
ssh $SSH_OPTS ubuntu@$FEAST_IP "
cd /opt/feast
source /opt/feast-env/bin/activate
feast apply
" 2>/dev/null
echo "  Feast feature views registered"

# Run initial materialization
ssh $SSH_OPTS ubuntu@$FEAST_IP "
cd /opt/feast
source /opt/feast-env/bin/activate
source env.sh
python materialize.py
" 2>/dev/null
echo "  Initial feature materialization complete"

# Create systemd service for feast serve
ssh $SSH_OPTS ubuntu@$FEAST_IP 'sudo bash -c "cat > /etc/systemd/system/feast-serve.service << SVCEOF
[Unit]
Description=Feast Feature Server
After=network.target
[Service]
Type=simple
User=ubuntu
WorkingDirectory=/opt/feast
ExecStart=/opt/feast-env/bin/feast serve -h 0.0.0.0 -p 6566
Restart=on-failure
RestartSec=5
[Install]
WantedBy=multi-user.target
SVCEOF"' 2>/dev/null

# Create systemd timer for periodic materialization
ssh $SSH_OPTS ubuntu@$FEAST_IP 'sudo bash -c "cat > /etc/systemd/system/feast-materialize.service << SVCEOF
[Unit]
Description=Feast Feature Materialization
[Service]
Type=oneshot
User=ubuntu
WorkingDirectory=/opt/feast
ExecStart=/bin/bash -c \"source /opt/feast-env/bin/activate && source /opt/feast/env.sh && python /opt/feast/materialize.py >> /var/log/feast-materialize.log 2>&1\"
SVCEOF"' 2>/dev/null

ssh $SSH_OPTS ubuntu@$FEAST_IP 'sudo bash -c "cat > /etc/systemd/system/feast-materialize.timer << SVCEOF
[Unit]
Description=Feast materialization every 2 minutes
[Timer]
OnBootSec=30
OnUnitActiveSec=120
[Install]
WantedBy=timers.target
SVCEOF"' 2>/dev/null

# Start Feast services
ssh $SSH_OPTS ubuntu@$FEAST_IP "
sudo systemctl daemon-reload
sudo systemctl enable feast-serve feast-materialize.timer
sudo systemctl start feast-serve feast-materialize.timer
" 2>/dev/null
echo "  Feast feature server started (port 6566)"
echo "  Feast materialization timer started (every 2 min)"

# Atlas setup (pipeline NOT started — use Start Demo button or start_demo.sh)
echo ""
echo "[5/5] Configuring Atlas..."

# Whitelist IPs
for IP in $KAFKA_PUBLIC_IP $GENERATOR_IP $FLINK_IP $MLFLOW_IP $FEAST_IP; do
  atlas accessLists create "$IP/32" --profile bk --comment "telco-ods-demo" 2>/dev/null || true
done
echo "  Atlas IP whitelist updated"

# Update trigger
if [ -n "$ATLAS_PUBLIC_KEY" ] && [ -n "$ATLAS_PRIVATE_KEY" ]; then
  export MLFLOW_ENDPOINT="http://${MLFLOW_IP}:5003/invocations"
  echo "  Updating Atlas Trigger → $MLFLOW_ENDPOINT"
  "$PROJECT_ROOT/atlas-trigger/setup_trigger.sh" 2>&1 | grep -E "^\[|Set MLFLOW|Created|Updated|Error"
else
  echo "  Warning: No Atlas API keys — trigger not updated."
fi

echo ""
echo "============================================================"
echo "DEPLOYMENT COMPLETE — Ready to demo"
echo "============================================================"
echo ""
echo "  Dashboard:   http://$MLFLOW_IP:8050"
echo "  Flink UI:    http://$FLINK_IP:8081"
echo "  MLflow:      http://$MLFLOW_IP:5002"
echo "  MLflow API:  http://$MLFLOW_IP:5003/invocations"
echo "  Feast:       http://$FEAST_IP:6566 (internal: $FEAST_PRIVATE_IP:6566)"
echo ""
echo "  Pipeline is NOT running yet. Start it with:"
echo "    - 'Start Demo' button on the dashboard"
echo "    - ./infrastructure/scripts/start_demo.sh"
echo ""
echo "  To tear down: ./infrastructure/scripts/teardown.sh"
echo "============================================================"
