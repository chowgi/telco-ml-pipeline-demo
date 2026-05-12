#!/bin/bash
# Validates all pipeline components are running
set -e

STACK_NAME="telco-ods-demo"
REGION="ap-southeast-2"

echo "============================================================"
echo "Telco ODS Demo - Pipeline Validation"
echo "============================================================"
echo ""

get_output() {
  aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --region "$REGION" \
    --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue" \
    --output text 2>/dev/null
}

KAFKA_PUBLIC_IP=$(get_output "KafkaPublicIP")
FLINK_IP=$(get_output "FlinkPublicIP")
MLFLOW_IP=$(get_output "MLflowPublicIP")

if [ -z "$MLFLOW_IP" ]; then
  echo "Error: Could not read stack outputs. Is the stack deployed?"
  echo "  aws cloudformation describe-stacks --stack-name $STACK_NAME --region $REGION"
  exit 1
fi

PASS=0
FAIL=0

check() {
  local name=$1
  local cmd=$2
  printf "  %-35s" "$name..."
  if eval "$cmd" > /dev/null 2>&1; then
    echo "PASS"
    PASS=$((PASS + 1))
  else
    echo "FAIL"
    FAIL=$((FAIL + 1))
  fi
}

echo "[Connectivity]"
check "Kafka EC2 reachable (SSH)" "nc -z -w5 $KAFKA_PUBLIC_IP 22"
check "Flink EC2 reachable (SSH)" "nc -z -w5 $FLINK_IP 22"
check "MLflow EC2 reachable (SSH)" "nc -z -w5 $MLFLOW_IP 22"

echo ""
echo "[Services]"
check "MLflow tracking (5002)" "curl -sf --max-time 5 http://$MLFLOW_IP:5002/"
check "MLflow serving (5003)" "curl -sf --max-time 5 http://$MLFLOW_IP:5003/version"
check "Flink Web UI (8081)" "curl -sf --max-time 5 http://$FLINK_IP:8081/overview"

echo ""
echo "[MLflow Inference Test]"
INFERENCE_RESULT=$(curl -sf --max-time 10 -X POST http://$MLFLOW_IP:5003/invocations \
  -H "Content-Type: application/json" \
  -d '{"dataframe_records": [{"signal_strength_dbm": -65, "throughput_mbps": 75, "latency_ms": 35, "call_drop_rate_percent": 0.8, "packet_loss_percent": 0.8, "jitter_ms": 2.5}]}' 2>/dev/null || echo "")

if [ -n "$INFERENCE_RESULT" ]; then
  printf "  %-35s" "Inference endpoint..."
  echo "PASS ($INFERENCE_RESULT)"
  PASS=$((PASS + 1))
else
  printf "  %-35s" "Inference endpoint..."
  echo "FAIL (no response)"
  FAIL=$((FAIL + 1))
fi

echo ""
echo "============================================================"
echo "Results: $PASS passed, $FAIL failed"
echo "============================================================"

[ $FAIL -gt 0 ] && exit 1 || exit 0
