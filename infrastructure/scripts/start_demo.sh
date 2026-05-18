#!/bin/bash
# Clears previous results and starts the pipeline fresh for a live demo.
# Run this 2-3 minutes before presenting — data will start flowing immediately.
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
echo "[1/5] Getting instance IPs..."
GENERATOR_IP=$(aws cloudformation describe-stacks --stack-name $STACK_NAME --region $REGION \
  --query "Stacks[0].Outputs[?OutputKey=='GeneratorPublicIP'].OutputValue" --output text)
FLINK_IP=$(aws cloudformation describe-stacks --stack-name $STACK_NAME --region $REGION \
  --query "Stacks[0].Outputs[?OutputKey=='FlinkPublicIP'].OutputValue" --output text)
MLFLOW_IP=$(aws cloudformation describe-stacks --stack-name $STACK_NAME --region $REGION \
  --query "Stacks[0].Outputs[?OutputKey=='MLflowPublicIP'].OutputValue" --output text)
KAFKA_PRIVATE_IP=$(aws cloudformation describe-stacks --stack-name $STACK_NAME --region $REGION \
  --query "Stacks[0].Outputs[?OutputKey=='KafkaPrivateIP'].OutputValue" --output text)

echo "  Generator: $GENERATOR_IP"
echo "  Processor: $FLINK_IP"
echo "  MLflow:    $MLFLOW_IP"
echo ""

# Ensure SSH access
echo "[2/5] Ensuring SSH access..."
MY_IP=$(curl -s --max-time 5 https://checkip.amazonaws.com)
for SG_NAME in telco-ods-demo-pipeline-sg telco-ods-demo-kafka-sg telco-ods-demo-mlflow-sg; do
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

# Clear previous results
echo "[3/5] Clearing previous results from MongoDB..."
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
'"
echo ""

# Stop existing processes and restart
echo "[4/5] Starting Flink stream processor..."
cat > /tmp/_start_processor.sh << 'PROCSCRIPT'
#!/bin/bash
# Cancel any existing Flink jobs
/opt/flink/bin/flink list -r 2>/dev/null | grep -oP '[0-9a-f]{32}' | while read JOB_ID; do
  /opt/flink/bin/flink cancel $JOB_ID 2>/dev/null || true
done
sleep 2

# Ensure Flink cluster is running
/opt/flink/bin/start-cluster.sh 2>/dev/null || true
sleep 3

# Submit PyFlink job
cd /opt/flink-job
source /opt/flink-env/bin/activate
export $(cat /opt/flink-job-config.env | xargs)
/opt/flink/bin/flink run -py flink_job.py \
  -pyexec /opt/flink-env/bin/python3 \
  >> /var/log/flink-job.log 2>&1 &
sleep 5

# Verify job is running
RUNNING=$(/opt/flink/bin/flink list -r 2>/dev/null | grep -c "RUNNING" || echo "0")
if [ "$RUNNING" -gt "0" ]; then
  echo "  Flink job running ($RUNNING job(s) active)"
else
  echo "  WARNING: Flink job may still be starting — check Web UI at :8081"
fi
PROCSCRIPT
scp $SSH_OPTS /tmp/_start_processor.sh ubuntu@$FLINK_IP:/tmp/_start_processor.sh > /dev/null
ssh $SSH_OPTS ubuntu@$FLINK_IP "bash /tmp/_start_processor.sh"
echo ""

echo "[5/5] Starting data generator..."
cat > /tmp/_start_generator.sh << 'GENSCRIPT'
#!/bin/bash
pkill -f generator.py || true
sleep 2
cd /opt/telco-generator
source venv/bin/activate
source /opt/telco-generator/env.sh
nohup python -u generator.py >> /var/log/generator.log 2>&1 &
sleep 3
pgrep -f generator.py > /dev/null && echo "  Generator started (PID $(pgrep -f generator.py))" || echo "  ERROR: Generator failed to start"
tail -5 /var/log/generator.log 2>/dev/null | grep -E "Cell states|events/sec" || true
GENSCRIPT
scp $SSH_OPTS /tmp/_start_generator.sh ubuntu@$GENERATOR_IP:/tmp/_start_generator.sh > /dev/null
ssh $SSH_OPTS ubuntu@$GENERATOR_IP "bash /tmp/_start_generator.sh"
echo ""

# Start dashboard if not running
ssh $SSH_OPTS ubuntu@$MLFLOW_IP "pgrep -f 'app.py.*8050' > /dev/null || bash -c '
cd /opt/dashboard
export \$(cat /opt/mlflow/.env | xargs)
nohup /opt/mlflow/venv/bin/python app.py >> /var/log/dashboard.log 2>&1 &
sleep 2
echo \"  Dashboard started\"
'" 2>/dev/null || true

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
echo "  Data will appear on the dashboard within ~30 seconds."
echo "  Predictions will start flowing once the first cell"
echo "  snapshots hit MongoDB and the Atlas Trigger fires."
echo ""
echo "  To stop: ./infrastructure/scripts/stop_demo.sh"
echo "============================================================"
