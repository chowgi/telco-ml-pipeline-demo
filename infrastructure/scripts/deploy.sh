#!/bin/bash
# Deploy the full Telco ODS streaming ML pipeline demo.
#
# Two modes:
#   - AMI deploy (default): Flink/MLflow/Generator from pre-baked AMIs, Kafka from userdata.
#     Instances boot with code pre-installed; userdata just updates configs (Kafka IP, etc).
#     Total time: ~90s (waiting for Kafka to bootstrap).
#   - Fresh deploy (no AMIs found): Full install via userdata. Slower (~5 min).
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
echo "  Apache Flink (PyFlink 1.18) | ~1k eps | 30s emission"
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
for cmd in aws ssh; do
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

# Check for pre-baked AMIs
echo ""
echo "[1/5] Checking for pre-baked AMIs..."
FLINK_AMI=$(aws ec2 describe-images --owners self --filters "Name=tag:TelcoODS,Values=flink" "Name=tag:Latest,Values=true" --region "$REGION" --query 'Images[0].ImageId' --output text 2>/dev/null || echo "None")
MLFLOW_AMI=$(aws ec2 describe-images --owners self --filters "Name=tag:TelcoODS,Values=mlflow" "Name=tag:Latest,Values=true" --region "$REGION" --query 'Images[0].ImageId' --output text 2>/dev/null || echo "None")
GENERATOR_AMI=$(aws ec2 describe-images --owners self --filters "Name=tag:TelcoODS,Values=generator" "Name=tag:Latest,Values=true" --region "$REGION" --query 'Images[0].ImageId' --output text 2>/dev/null || echo "None")

CFN_PARAMS="KeyPairName=$KEY_PAIR_NAME AllowedSSHCidr=$ALLOWED_SSH_CIDR"

if [ "$FLINK_AMI" != "None" ] && [ "$FLINK_AMI" != "null" ] && [ -n "$FLINK_AMI" ]; then
  echo "  AMI deploy (fast path):"
  echo "    Flink:     $FLINK_AMI"
  echo "    MLflow:    $MLFLOW_AMI"
  echo "    Generator: $GENERATOR_AMI"
  echo "    Kafka:     (bootstrapped from userdata — fast via Confluent APT)"
  CFN_PARAMS="$CFN_PARAMS FlinkAMI=$FLINK_AMI MLflowAMI=$MLFLOW_AMI GeneratorAMI=$GENERATOR_AMI"
  USE_AMIS=true
else
  echo "  No AMIs found — fresh install (will take ~5 min)."
  echo "  After deploy, run: ./infrastructure/scripts/create-amis.sh"
  USE_AMIS=false
fi

# Deploy CloudFormation
echo ""
echo "[2/5] Deploying CloudFormation stack..."
aws cloudformation deploy \
  --template-file "$CFN_TEMPLATE" \
  --stack-name "$STACK_NAME" \
  --region "$REGION" \
  --parameter-overrides $CFN_PARAMS \
  --capabilities CAPABILITY_IAM \
  --no-fail-on-empty-changeset

echo "  Stack created."

# Get outputs
echo ""
echo "[3/5] Retrieving instance IPs..."
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

# Wait for Kafka (the bottleneck — others are instant from AMI)
echo ""
echo "[4/5] Waiting for Kafka to bootstrap..."
for i in $(seq 1 12); do
  KAFKA_STATUS=$(ssh $SSH_OPTS ubuntu@$KAFKA_PUBLIC_IP "cat /tmp/kafka-status 2>/dev/null" 2>/dev/null || echo "")
  if [ "$KAFKA_STATUS" = "KAFKA_READY" ]; then
    echo "  Kafka ready!"
    break
  fi
  if [ $i -eq 12 ]; then
    echo "  Warning: Kafka not confirmed ready after 3 min. Check /var/log/kafka-setup.log"
  fi
  sleep 15
done

# Verify other instances
echo "  Verifying Flink..."
ssh $SSH_OPTS ubuntu@$FLINK_IP "cat /tmp/flink-status 2>/dev/null" 2>/dev/null || echo "  (Flink still initializing)"
echo "  Verifying MLflow..."
ssh $SSH_OPTS ubuntu@$MLFLOW_IP "cat /tmp/mlflow-status 2>/dev/null" 2>/dev/null || echo "  (MLflow still initializing)"

# Atlas whitelist + trigger
echo ""
echo "[5/5] Configuring Atlas..."

# Add IPs to Atlas network access
for IP in $KAFKA_PUBLIC_IP $GENERATOR_IP $FLINK_IP $MLFLOW_IP; do
  atlas accessLists create "$IP/32" --profile bk --comment "telco-ods-demo" 2>/dev/null || true
done
echo "  Atlas IP whitelist updated"

# Update trigger with new MLflow endpoint
if [ -n "$ATLAS_PUBLIC_KEY" ] && [ -n "$ATLAS_PRIVATE_KEY" ]; then
  export MLFLOW_ENDPOINT="http://${MLFLOW_IP}:5003/invocations"
  echo "  Updating Atlas Trigger → $MLFLOW_ENDPOINT"
  "$PROJECT_ROOT/atlas-trigger/setup_trigger.sh" 2>&1 | grep -E "^\[|Set MLFLOW|Created|Updated|Error"
else
  echo "  Warning: No Atlas API keys — trigger not updated."
  echo "  Set ATLAS_PUBLIC_KEY and ATLAS_PRIVATE_KEY in .env"
fi

echo ""
echo "============================================================"
echo "DEPLOYMENT COMPLETE"
echo "============================================================"
echo ""
echo "  Dashboard:   http://$MLFLOW_IP:8050"
echo "  Flink UI:    http://$FLINK_IP:8081"
echo "  MLflow:      http://$MLFLOW_IP:5002"
echo ""
echo "  Use the dashboard 'Start Demo' button, or:"
echo "    ./infrastructure/scripts/start_demo.sh"
echo ""
echo "  To tear down: ./infrastructure/scripts/teardown.sh"
echo "============================================================"
