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
  echo "  Warning: No .env file found. Atlas trigger setup will be skipped."
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

echo "  Kafka:     $KAFKA_PUBLIC_IP (private: $KAFKA_PRIVATE_IP)"
echo "  Generator: $GENERATOR_IP (private: $GENERATOR_PRIVATE_IP)"
echo "  Flink:     $FLINK_IP (private: $FLINK_PRIVATE_IP)"
echo "  MLflow:    $MLFLOW_IP"

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

  READY=0
  [ "$KAFKA_OK" = "KAFKA_READY" ] && READY=$((READY+1))
  [ "$FLINK_OK" = "FLINK_READY" ] && READY=$((READY+1))
  [ "$GEN_OK" = "GENERATOR_READY" ] && READY=$((READY+1))
  [ "$MLFLOW_OK" = "MLFLOW_READY" ] && READY=$((READY+1))

  echo "  [$((i*15))s] $READY/4 ready (kafka:${KAFKA_OK:-pending} flink:${FLINK_OK:-pending} gen:${GEN_OK:-pending} mlflow:${MLFLOW_OK:-pending})"

  if [ $READY -eq 4 ]; then
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
pkill -9 -f "org.apache.flink" || true
pkill -9 -f "flink_job.py" || true
sleep 2
rm -f /opt/flink/log/*.pid 2>/dev/null || true
/opt/flink/bin/start-cluster.sh
sleep 5
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
pkill -9 -f "org.apache.flink" || true
pkill -9 -f "flink_job.py" || true
rm -f /opt/flink/log/*.pid 2>/dev/null || true
echo "Flink stopped"
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

# Train model and start serving
ssh $SSH_OPTS ubuntu@$MLFLOW_IP "
cd /opt/mlflow/app
source /opt/mlflow/venv/bin/activate
export MLFLOW_TRACKING_URI=http://localhost:5002
export MONGODB_URI='${MongoDBUri}'
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
nohup /opt/mlflow/venv/bin/python app.py >> /var/log/dashboard.log 2>&1 < /dev/null &
" 2>/dev/null
echo "  Dashboard started"

# Atlas setup
echo ""
echo "[5/5] Configuring Atlas..."

# Whitelist IPs
for IP in $KAFKA_PUBLIC_IP $GENERATOR_IP $FLINK_IP $MLFLOW_IP; do
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
echo "DEPLOYMENT COMPLETE"
echo "============================================================"
echo ""
echo "  Dashboard:   http://$MLFLOW_IP:8050"
echo "  Flink UI:    http://$FLINK_IP:8081"
echo "  MLflow:      http://$MLFLOW_IP:5002"
echo "  MLflow API:  http://$MLFLOW_IP:5003/invocations"
echo ""
echo "  Use the dashboard 'Start Demo' button, or:"
echo "    ./infrastructure/scripts/start_demo.sh"
echo ""
echo "  To tear down: ./infrastructure/scripts/teardown.sh"
echo "============================================================"
