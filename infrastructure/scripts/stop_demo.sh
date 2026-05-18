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

ssh $SSH_OPTS ubuntu@$FLINK_IP "pkill -f flink_job.py || true" 2>/dev/null
echo "  Processor stopped"

echo "Done. Infrastructure remains running."
