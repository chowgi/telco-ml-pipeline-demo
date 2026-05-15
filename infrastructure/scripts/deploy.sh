#!/bin/bash
# Deploy the full Telco ODS streaming ML pipeline demo via CloudFormation
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
CFN_TEMPLATE="$PROJECT_ROOT/infrastructure/cloudformation/stack.yaml"
STACK_NAME="telco-ods-demo"
REGION="ap-southeast-2"

echo "============================================================"
echo "Telco ODS - Autonomous Networks ML Pipeline Demo"
echo "============================================================"
echo ""

# Prerequisites check
echo "[1/8] Checking prerequisites..."
for cmd in aws ssh scp; do
  if ! command -v $cmd &> /dev/null; then
    echo "Error: $cmd is required but not installed."
    exit 1
  fi
done
echo "  All prerequisites found."

# Auto-detect public IP for SSH security group rules
if [ -z "$ALLOWED_SSH_CIDR" ]; then
  MY_IP=$(curl -s --max-time 5 https://checkip.amazonaws.com)
  if [ -n "$MY_IP" ]; then
    ALLOWED_SSH_CIDR="${MY_IP}/32"
    echo "  Detected public IP: $MY_IP (using ${ALLOWED_SSH_CIDR} for SSH)"
  else
    echo "  Warning: Could not detect public IP. Set ALLOWED_SSH_CIDR manually."
    exit 1
  fi
fi

# Check for pre-baked AMIs (created by create-amis.sh)
echo ""
echo "  Checking for pre-baked AMIs..."
KAFKA_AMI=$(aws ec2 describe-images --owners self --filters "Name=tag:TelcoODS,Values=kafka" "Name=tag:Latest,Values=true" --region "$REGION" --query 'Images[0].ImageId' --output text 2>/dev/null || echo "None")
FLINK_AMI=$(aws ec2 describe-images --owners self --filters "Name=tag:TelcoODS,Values=flink" "Name=tag:Latest,Values=true" --region "$REGION" --query 'Images[0].ImageId' --output text 2>/dev/null || echo "None")
MLFLOW_AMI=$(aws ec2 describe-images --owners self --filters "Name=tag:TelcoODS,Values=mlflow" "Name=tag:Latest,Values=true" --region "$REGION" --query 'Images[0].ImageId' --output text 2>/dev/null || echo "None")
GENERATOR_AMI=$(aws ec2 describe-images --owners self --filters "Name=tag:TelcoODS,Values=generator" "Name=tag:Latest,Values=true" --region "$REGION" --query 'Images[0].ImageId' --output text 2>/dev/null || echo "None")

if [ "$KAFKA_AMI" != "None" ] && [ "$KAFKA_AMI" != "null" ] && [ -n "$KAFKA_AMI" ]; then
  echo "  Found pre-baked AMIs! Using fast deploy path."
  echo "    Kafka:     $KAFKA_AMI"
  echo "    Flink:     $FLINK_AMI"
  echo "    MLflow:    $MLFLOW_AMI"
  echo "    Generator: $GENERATOR_AMI"
  USE_CUSTOM_AMIS=true
else
  echo "  No pre-baked AMIs found. Using default Ubuntu (first deploy will be slower)."
  echo "  After deploy, run: ./infrastructure/scripts/create-amis.sh"
  USE_CUSTOM_AMIS=false
fi

# Check for key pair
if [ -z "$KEY_PAIR_NAME" ]; then
  echo ""
  echo "Error: Set KEY_PAIR_NAME to your EC2 key pair name in ap-southeast-2"
  echo "  export KEY_PAIR_NAME=your-key-pair"
  echo "  export SSH_KEY_PATH=~/.ssh/your-key.pem"
  exit 1
fi

SSH_KEY_PATH="${SSH_KEY_PATH:-~/.ssh/${KEY_PAIR_NAME}.pem}"

if [ ! -f "$SSH_KEY_PATH" ]; then
  # Check project directory
  if [ -f "$PROJECT_ROOT/${KEY_PAIR_NAME}.pem" ]; then
    SSH_KEY_PATH="$PROJECT_ROOT/${KEY_PAIR_NAME}.pem"
  else
    echo "Error: SSH key not found at $SSH_KEY_PATH"
    echo "  Set SSH_KEY_PATH to the correct .pem file location"
    exit 1
  fi
fi

echo "  Key pair: $KEY_PAIR_NAME"
echo "  SSH key:  $SSH_KEY_PATH"

# Deploy CloudFormation stack
echo ""
echo "[2/8] Deploying CloudFormation stack..."

CFN_PARAMS="KeyPairName=$KEY_PAIR_NAME AllowedSSHCidr=$ALLOWED_SSH_CIDR"

if [ "$USE_CUSTOM_AMIS" = true ]; then
  CFN_PARAMS="$CFN_PARAMS KafkaAMI=$KAFKA_AMI FlinkAMI=$FLINK_AMI MLflowAMI=$MLFLOW_AMI GeneratorAMI=$GENERATOR_AMI"
fi

aws cloudformation deploy \
  --template-file "$CFN_TEMPLATE" \
  --stack-name "$STACK_NAME" \
  --region "$REGION" \
  --parameter-overrides $CFN_PARAMS \
  --capabilities CAPABILITY_IAM \
  --no-fail-on-empty-changeset

echo "  Stack deployed successfully."

# Get outputs
echo ""
echo "[3/8] Retrieving instance IPs..."

get_output() {
  aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --region "$REGION" \
    --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue" \
    --output text
}

KAFKA_PRIVATE_IP=$(get_output "KafkaPrivateIP")
KAFKA_PUBLIC_IP=$(get_output "KafkaPublicIP")
GENERATOR_IP=$(get_output "GeneratorPublicIP")
FLINK_IP=$(get_output "FlinkPublicIP")
MLFLOW_IP=$(get_output "MLflowPublicIP")

echo "  Kafka:     $KAFKA_PUBLIC_IP (private: $KAFKA_PRIVATE_IP)"
echo "  Generator: $GENERATOR_IP"
echo "  Flink:     $FLINK_IP"
echo "  MLflow:    $MLFLOW_IP"

# Wait for instances
echo ""
echo "[4/8] Waiting for instances to initialize (90s)..."
sleep 90

# Atlas IP whitelist
echo ""
echo "[5/8] MongoDB Atlas IP Whitelist"
echo "  Add these IPs to Atlas Network Access (or 0.0.0.0/0 for demo):"
echo "    - $KAFKA_PUBLIC_IP/32"
echo "    - $GENERATOR_IP/32"
echo "    - $FLINK_IP/32"
echo "    - $MLFLOW_IP/32"
echo ""
echo "  Press Enter when done..."
read -r

# Upload and start MLflow model
echo ""
echo "[6/8] Setting up MLflow model..."
scp -i "$SSH_KEY_PATH" -o StrictHostKeyChecking=no -r \
  "$PROJECT_ROOT/ml-model/"* ubuntu@${MLFLOW_IP}:/opt/mlflow/app/

ssh -i "$SSH_KEY_PATH" -o StrictHostKeyChecking=no ubuntu@${MLFLOW_IP} << 'REMOTEOF'
  cd /opt/mlflow
  source venv/bin/activate
  source .env
  export MONGODB_URI MLFLOW_TRACKING_URI
  cd app
  pip install -r requirements.txt -q 2>/dev/null
  python generate_training_data.py
  python train_model.py
  nohup bash serve_model.sh > /var/log/mlflow-serve.log 2>&1 &
  sleep 5
  echo "MLflow model serving started"
REMOTEOF

# Upload and start Flink job
echo ""
echo "[7/8] Setting up Flink processor..."
scp -i "$SSH_KEY_PATH" -o StrictHostKeyChecking=no -r \
  "$PROJECT_ROOT/flink-processor/"* ubuntu@${FLINK_IP}:/tmp/flink-job/

ssh -i "$SSH_KEY_PATH" -o StrictHostKeyChecking=no ubuntu@${FLINK_IP} << REMOTEOF
  source /opt/flink-env/bin/activate
  cp /tmp/flink-job/* /opt/flink-job/
  cd /opt/flink-job
  pip install -r requirements.txt -q 2>/dev/null
  export \$(cat /opt/flink-job-config.env | xargs)
  nohup python -u flink_job.py > /var/log/flink-job.log 2>&1 &
  echo "Flink job started"
REMOTEOF

# Upload and start generator
echo ""
echo "[8/8] Starting data generator..."
scp -i "$SSH_KEY_PATH" -o StrictHostKeyChecking=no -r \
  "$PROJECT_ROOT/data-generator/"* ubuntu@${GENERATOR_IP}:/tmp/generator/

ssh -i "$SSH_KEY_PATH" -o StrictHostKeyChecking=no ubuntu@${GENERATOR_IP} << REMOTEOF
  cd /opt/telco-generator
  source venv/bin/activate
  cp -r /tmp/generator/* .
  pip install -r requirements.txt -q 2>/dev/null
  source env.sh
  export KAFKA_BROKER KAFKA_TOPIC
  nohup python -u generator.py > /var/log/generator.log 2>&1 &
  echo "Generator started"
REMOTEOF

# Summary
echo ""
echo "============================================================"
echo "DEPLOYMENT COMPLETE"
echo "============================================================"
echo ""
echo "Endpoints:"
echo "  MLflow Tracking:  http://${MLFLOW_IP}:5002"
echo "  MLflow Inference: http://${MLFLOW_IP}:5003/invocations"
echo ""
echo "Next step - configure Atlas Trigger:"
echo "  1. Atlas > App Services > Triggers"
echo "  2. Collection: ods_demo_db.windowed_network_metrics (Insert)"
echo "  3. Paste: atlas-trigger/trigger_function.js"
echo "  4. Set MLFLOW_ENDPOINT value: http://${MLFLOW_IP}:5003/invocations"
echo ""
echo "Data will appear in MongoDB within 5 minutes:"
echo "  db.windowed_network_metrics.countDocuments()"
echo "  db.network_health_predictions.find().sort({timestamp:-1}).limit(5)"
echo ""
echo "To tear down: ./infrastructure/scripts/teardown.sh"
