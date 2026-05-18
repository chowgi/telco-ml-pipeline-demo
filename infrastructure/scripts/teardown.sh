#!/bin/bash
# Tear down all AWS resources for the Telco ODS demo
set -e

STACK_NAME="telco-ods-demo"
REGION="ap-southeast-2"

echo "============================================================"
echo "Telco ODS Demo - Teardown"
echo "============================================================"
echo ""
echo "This will DELETE the CloudFormation stack and ALL resources:"
echo "  - 5 EC2 instances (Kafka, Generator, Flink, MLflow, Feast)"
echo "  - VPC, subnet, security groups"
echo ""
echo "Press Ctrl+C to cancel, or Enter to continue..."
read -r

echo ""
echo "[1/2] Deleting CloudFormation stack..."
aws cloudformation delete-stack \
  --stack-name "$STACK_NAME" \
  --region "$REGION"

echo "  Waiting for stack deletion..."
aws cloudformation wait stack-delete-complete \
  --stack-name "$STACK_NAME" \
  --region "$REGION"

echo ""
echo "[2/2] Done."
echo ""
echo "All AWS resources destroyed."
echo ""
echo "NOTE: MongoDB Atlas data is NOT deleted. To clean up:"
echo "  - Drop ods_demo_db.windowed_network_metrics"
echo "  - Drop ods_demo_db.network_health_predictions"
echo "  - Drop ods_demo_db.telco_ods_online"
echo "  - Remove Atlas trigger via App Services UI"
echo "  - Remove IP whitelist entries"
