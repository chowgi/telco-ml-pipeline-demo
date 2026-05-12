#!/bin/bash
# Creates AMIs from running demo instances for faster subsequent deploys.
# Run this AFTER a successful first deploy when all instances are configured.
set -e

STACK_NAME="telco-ods-demo"
REGION="ap-southeast-2"
TIMESTAMP=$(date +%Y%m%d%H%M)

echo "============================================================"
echo "Telco ODS Demo - Create AMIs for Fast Redeploy"
echo "============================================================"
echo ""

get_output() {
  aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --region "$REGION" \
    --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue" \
    --output text
}

# Get instance IDs from the stack
get_instance_id() {
  aws cloudformation describe-stack-resource \
    --stack-name "$STACK_NAME" \
    --logical-resource-id "$1" \
    --region "$REGION" \
    --query 'StackResourceDetail.PhysicalResourceId' \
    --output text
}

KAFKA_ID=$(get_instance_id "KafkaInstance")
FLINK_ID=$(get_instance_id "FlinkInstance")
MLFLOW_ID=$(get_instance_id "MLflowInstance")
GENERATOR_ID=$(get_instance_id "GeneratorInstance")

echo "Creating AMIs from:"
echo "  Kafka:     $KAFKA_ID"
echo "  Flink:     $FLINK_ID"
echo "  MLflow:    $MLFLOW_ID"
echo "  Generator: $GENERATOR_ID"
echo ""

# Create AMIs (no-reboot to avoid disrupting running demo)
echo "[1/4] Creating Kafka AMI..."
KAFKA_AMI=$(aws ec2 create-image \
  --instance-id "$KAFKA_ID" \
  --name "telco-ods-kafka-${TIMESTAMP}" \
  --description "Pre-configured Kafka broker for Telco ODS demo" \
  --no-reboot \
  --region "$REGION" \
  --query 'ImageId' \
  --output text)
echo "  AMI: $KAFKA_AMI"

echo "[2/4] Creating Flink AMI..."
FLINK_AMI=$(aws ec2 create-image \
  --instance-id "$FLINK_ID" \
  --name "telco-ods-flink-${TIMESTAMP}" \
  --description "Pre-configured PyFlink cluster for Telco ODS demo" \
  --no-reboot \
  --region "$REGION" \
  --query 'ImageId' \
  --output text)
echo "  AMI: $FLINK_AMI"

echo "[3/4] Creating MLflow AMI..."
MLFLOW_AMI=$(aws ec2 create-image \
  --instance-id "$MLFLOW_ID" \
  --name "telco-ods-mlflow-${TIMESTAMP}" \
  --description "Pre-configured MLflow server for Telco ODS demo" \
  --no-reboot \
  --region "$REGION" \
  --query 'ImageId' \
  --output text)
echo "  AMI: $MLFLOW_AMI"

echo "[4/4] Creating Generator AMI..."
GENERATOR_AMI=$(aws ec2 create-image \
  --instance-id "$GENERATOR_ID" \
  --name "telco-ods-generator-${TIMESTAMP}" \
  --description "Pre-configured data generator for Telco ODS demo" \
  --no-reboot \
  --region "$REGION" \
  --query 'ImageId' \
  --output text)
echo "  AMI: $GENERATOR_AMI"

# Wait for AMIs to become available
echo ""
echo "Waiting for AMIs to become available (this takes 5-10 minutes)..."
aws ec2 wait image-available --image-ids $KAFKA_AMI $FLINK_AMI $MLFLOW_AMI $GENERATOR_AMI --region "$REGION"

# Tag the AMIs for easy lookup
aws ec2 create-tags --resources $KAFKA_AMI --tags Key=TelcoODS,Value=kafka Key=Latest,Value=true --region "$REGION"
aws ec2 create-tags --resources $FLINK_AMI --tags Key=TelcoODS,Value=flink Key=Latest,Value=true --region "$REGION"
aws ec2 create-tags --resources $MLFLOW_AMI --tags Key=TelcoODS,Value=mlflow Key=Latest,Value=true --region "$REGION"
aws ec2 create-tags --resources $GENERATOR_AMI --tags Key=TelcoODS,Value=generator Key=Latest,Value=true --region "$REGION"

# Remove "Latest" tag from old AMIs
OLD_AMIS=$(aws ec2 describe-images \
  --owners self \
  --filters "Name=tag:TelcoODS,Values=*" "Name=tag:Latest,Values=true" \
  --region "$REGION" \
  --query "Images[?ImageId!=\`${KAFKA_AMI}\` && ImageId!=\`${FLINK_AMI}\` && ImageId!=\`${MLFLOW_AMI}\` && ImageId!=\`${GENERATOR_AMI}\`].ImageId" \
  --output text)

for ami in $OLD_AMIS; do
  aws ec2 delete-tags --resources "$ami" --tags Key=Latest --region "$REGION" 2>/dev/null || true
done

echo ""
echo "============================================================"
echo "AMIs Created Successfully"
echo "============================================================"
echo ""
echo "  Kafka:     $KAFKA_AMI"
echo "  Flink:     $FLINK_AMI"
echo "  MLflow:    $MLFLOW_AMI"
echo "  Generator: $GENERATOR_AMI"
echo ""
echo "Next deploy will use these automatically (deploy.sh checks for them)."
echo "AMI creation timestamp: $TIMESTAMP"
