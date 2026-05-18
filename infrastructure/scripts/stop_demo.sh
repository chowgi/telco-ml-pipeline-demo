#!/bin/bash
# Stops the demo pipeline — mirrors the dashboard's "Stop Demo" button.
# Kills the generator, hard-stops Flink, and clears MongoDB collections.
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SSH_KEY="${SSH_KEY_PATH:-$PROJECT_ROOT/bennyk_aws_key.pem}"
SSH_OPTS="-o ConnectTimeout=15 -o ServerAliveInterval=5 -o StrictHostKeyChecking=no -i $SSH_KEY"
REGION="ap-southeast-2"
STACK_NAME="telco-ods-demo"

echo "============================================================"
echo "  Telco ODS - Stop Demo"
echo "============================================================"
echo ""

# Get instance IPs
GENERATOR_IP=$(aws cloudformation describe-stacks --stack-name $STACK_NAME --region $REGION \
  --query "Stacks[0].Outputs[?OutputKey=='GeneratorPublicIP'].OutputValue" --output text)
FLINK_IP=$(aws cloudformation describe-stacks --stack-name $STACK_NAME --region $REGION \
  --query "Stacks[0].Outputs[?OutputKey=='FlinkPublicIP'].OutputValue" --output text)
MLFLOW_IP=$(aws cloudformation describe-stacks --stack-name $STACK_NAME --region $REGION \
  --query "Stacks[0].Outputs[?OutputKey=='MLflowPublicIP'].OutputValue" --output text)

# Kill the generator
echo "[1/3] Stopping data generator..."
ssh $SSH_OPTS ubuntu@$GENERATOR_IP "bash -c 'pkill -f generator.py || true'" 2>/dev/null
echo "  Generator stopped"
echo ""

# Hard-stop Flink (must be killed, not cancelled — stale state after cancel)
echo "[2/3] Stopping Flink..."
ssh $SSH_OPTS ubuntu@$FLINK_IP "bash -c 'sudo /opt/flink-job/stop.sh'" 2>/dev/null
echo "  Flink stopped"
echo ""

# Clear MongoDB collections
echo "[3/3] Clearing MongoDB collections..."
ssh $SSH_OPTS ubuntu@$MLFLOW_IP "bash -c '
export \$(cat /opt/mlflow/.env | xargs)
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
'" 2>/dev/null
echo ""

echo "============================================================"
echo "  DEMO STOPPED"
echo "============================================================"
echo ""
echo "  All pipeline components stopped. MongoDB cleared."
echo "  Infrastructure remains running (instances still up)."
echo "  Dashboard remains accessible at http://$MLFLOW_IP:8050"
echo ""
echo "  To restart: ./infrastructure/scripts/start_demo.sh"
echo "  To tear down: ./infrastructure/scripts/teardown.sh"
echo "============================================================"
