#!/bin/bash
# Stops the generator and stream processor (leaves infrastructure running).
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SSH_KEY="${SSH_KEY_PATH:-$PROJECT_ROOT/bennyk_aws_key.pem}"
SSH_OPTS="-o ConnectTimeout=15 -o ServerAliveInterval=5 -o StrictHostKeyChecking=no -i $SSH_KEY"
REGION="ap-southeast-2"
STACK_NAME="telco-ods-demo"

echo "Stopping demo pipeline..."

GENERATOR_IP=$(aws cloudformation describe-stacks --stack-name $STACK_NAME --region $REGION \
  --query "Stacks[0].Outputs[?OutputKey=='GeneratorPublicIP'].OutputValue" --output text)
FLINK_IP=$(aws cloudformation describe-stacks --stack-name $STACK_NAME --region $REGION \
  --query "Stacks[0].Outputs[?OutputKey=='FlinkPublicIP'].OutputValue" --output text)

ssh $SSH_OPTS ubuntu@$GENERATOR_IP "pkill -f generator.py || true" 2>/dev/null
echo "  Generator stopped"

ssh $SSH_OPTS ubuntu@$FLINK_IP '/opt/flink/bin/flink list -r 2>/dev/null | grep -oP "[0-9a-f]{32}" | while read JOB_ID; do /opt/flink/bin/flink cancel $JOB_ID 2>/dev/null; done' 2>/dev/null
echo "  Flink job(s) cancelled"

echo "Done. Infrastructure remains running (Flink cluster still up at :8081)."
