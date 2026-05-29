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
echo "[2/3] Clearing MongoDB Atlas collections..."
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
if [ -f "$PROJECT_ROOT/.env" ]; then
  MONGODB_URI=$(grep '^MONGODB_URI=' "$PROJECT_ROOT/.env" | cut -d'=' -f2-)
fi

if [ -n "$MONGODB_URI" ]; then
  if python3 -c "import pymongo" 2>/dev/null; then
    python3 -c "
from pymongo import MongoClient
client = MongoClient('${MONGODB_URI}')
db = client['ods_demo_db']
for coll in ['windowed_network_metrics', 'network_health_predictions', 'telco_ods_online']:
    result = db[coll].delete_many({})
    print(f'  Cleared {coll}: {result.deleted_count} docs')
client.close()
"
    echo "  Collections cleared (not dropped — trigger resume token preserved)"
  else
    echo "  Warning: pymongo not installed. Install with: pip3 install pymongo"
    echo "  Skipping Atlas cleanup — clear collections manually via Atlas UI."
  fi
else
  echo "  Warning: No MONGODB_URI found. Skipping Atlas cleanup."
fi

echo ""
echo "[3/3] Done."
echo ""
echo "All AWS resources destroyed. Atlas collections cleared."
echo ""
echo "NOTE: The following are NOT removed automatically:"
echo "  - Atlas trigger (preserved for next deploy)"
echo "  - IP whitelist entries (stale but harmless)"
