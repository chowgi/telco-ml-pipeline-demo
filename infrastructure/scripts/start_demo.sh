#!/bin/bash
# Starts the demo pipeline — mirrors the dashboard's "Start Demo" button.
# Clears previous results, restarts Flink (hard-kill required between runs),
# starts the generator, and ensures the dashboard is running.
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SSH_KEY="${SSH_KEY_PATH:-$PROJECT_ROOT/bennyk_aws_key.pem}"
SSH_OPTS="-o ConnectTimeout=15 -o ServerAliveInterval=5 -o StrictHostKeyChecking=no -i $SSH_KEY"
REGION="ap-southeast-2"
STACK_NAME="telco-ods-demo"

echo "============================================================"
echo "  Telco ODS - Start Demo"
echo "============================================================"
echo ""

# Get instance IPs from CloudFormation
echo "[1/7] Getting instance IPs..."
GENERATOR_IP=$(aws cloudformation describe-stacks --stack-name $STACK_NAME --region $REGION \
  --query "Stacks[0].Outputs[?OutputKey=='GeneratorPublicIP'].OutputValue" --output text)
FLINK_IP=$(aws cloudformation describe-stacks --stack-name $STACK_NAME --region $REGION \
  --query "Stacks[0].Outputs[?OutputKey=='FlinkPublicIP'].OutputValue" --output text)
MLFLOW_IP=$(aws cloudformation describe-stacks --stack-name $STACK_NAME --region $REGION \
  --query "Stacks[0].Outputs[?OutputKey=='MLflowPublicIP'].OutputValue" --output text)
FEAST_IP=$(aws cloudformation describe-stacks --stack-name $STACK_NAME --region $REGION \
  --query "Stacks[0].Outputs[?OutputKey=='FeastPublicIP'].OutputValue" --output text)
GENERATOR_PRIVATE_IP=$(aws cloudformation describe-stacks --stack-name $STACK_NAME --region $REGION \
  --query "Stacks[0].Outputs[?OutputKey=='GeneratorPrivateIP'].OutputValue" --output text)
FLINK_PRIVATE_IP=$(aws cloudformation describe-stacks --stack-name $STACK_NAME --region $REGION \
  --query "Stacks[0].Outputs[?OutputKey=='FlinkPrivateIP'].OutputValue" --output text)

echo "  Generator: $GENERATOR_IP (private: $GENERATOR_PRIVATE_IP)"
echo "  Flink:     $FLINK_IP (private: $FLINK_PRIVATE_IP)"
echo "  MLflow:    $MLFLOW_IP"
echo "  Feast:     $FEAST_IP"
echo ""

# Ensure SSH access (update security group with current IP)
echo "[2/7] Ensuring SSH access..."
MY_IP=$(curl -s --max-time 5 https://checkip.amazonaws.com)
for SG_NAME in telco-ods-demo-pipeline-sg telco-ods-demo-kafka-sg telco-ods-demo-mlflow-sg telco-ods-demo-feast-sg; do
  SG_ID=$(aws ec2 describe-security-groups --region $REGION \
    --filters "Name=tag:Name,Values=$SG_NAME" \
    --query "SecurityGroups[0].GroupId" --output text 2>/dev/null)
  if [ -n "$SG_ID" ] && [ "$SG_ID" != "None" ]; then
    aws ec2 authorize-security-group-ingress --region $REGION \
      --group-id $SG_ID --protocol tcp --port 22 --cidr ${MY_IP}/32 2>/dev/null || true
  fi
done
echo "  SSH allowed from $MY_IP"
echo ""

# Clear previous results from MongoDB
echo "[3/7] Clearing previous results from MongoDB..."
ssh $SSH_OPTS ubuntu@$MLFLOW_IP "bash -c '
source /opt/dashboard/env.sh
/opt/mlflow/venv/bin/python3 -c \"
from pymongo import MongoClient
import os
client = MongoClient(os.environ[\\\"MONGODB_URI\\\"])
db = client[\\\"ods_demo_db\\\"]
p = db.network_health_predictions.delete_many({})
w = db.windowed_network_metrics.delete_many({})
print(f\\\"  Cleared {p.deleted_count} predictions, {w.deleted_count} windowed metrics\\\")
client.close()
\"
'"
echo ""

# Restart Flink (hard-kill required — PyFlink/Beam workers leave stale state after cancel)
echo "[4/7] Restarting Flink stream processor..."
ssh $SSH_OPTS ubuntu@$FLINK_IP "/opt/flink-job/restart.sh"
echo "  Flink restarted and job submitted"
echo ""

# Start generator (kills any existing process first)
echo "[5/7] Starting data generator (~1k events/sec)..."
ssh $SSH_OPTS ubuntu@$GENERATOR_IP "/opt/telco-generator/start.sh"
echo "  Generator started"
echo ""

# Ensure Feast materialization timer is active
echo "[6/7] Ensuring Feast feature store is running..."
ssh $SSH_OPTS ubuntu@$FEAST_IP "
sudo systemctl start feast-serve feast-materialize.timer 2>/dev/null
cd /opt/feast && source /opt/feast-env/bin/activate && source env.sh && python materialize.py 2>/dev/null
echo '  Feast materialization triggered'
" 2>/dev/null || true
echo ""

# Ensure dashboard is running on MLflow instance
echo "[7/7] Ensuring dashboard is running..."
ssh $SSH_OPTS ubuntu@$MLFLOW_IP '
if ! pgrep -f "app.py" > /dev/null 2>&1; then
  source /opt/dashboard/env.sh
  cd /opt/dashboard
  nohup /opt/mlflow/venv/bin/python app.py >> /var/log/dashboard.log 2>&1 &
  sleep 2
  echo "  Dashboard started"
else
  echo "  Dashboard already running"
fi
' 2>/dev/null || true
echo ""

echo "============================================================"
echo "  DEMO READY"
echo "============================================================"
echo ""
echo "  Dashboard:   http://$MLFLOW_IP:8050"
echo "  Flink UI:    http://$FLINK_IP:8081"
echo "  MLflow UI:   http://$MLFLOW_IP:5002"
echo "  MLflow API:  http://$MLFLOW_IP:5003/invocations"
echo ""
echo "  Data will appear on the dashboard after the first 5-minute"
echo "  Flink window completes. The Atlas Trigger fires on each"
echo "  insert to windowed_network_metrics to run ML inference."
echo ""
echo "  To stop: ./infrastructure/scripts/stop_demo.sh"
echo "============================================================"
