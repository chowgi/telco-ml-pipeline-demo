#!/bin/bash
# Validates all pipeline components are running correctly
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TERRAFORM_DIR="$SCRIPT_DIR/../terraform"

cd "$TERRAFORM_DIR"

echo "============================================================"
echo "Telco ODS Demo - Pipeline Validation"
echo "============================================================"
echo ""

MLFLOW_IP=$(terraform output -raw mlflow_public_ip 2>/dev/null || echo "")
FLINK_IP=$(terraform output -raw flink_public_ip 2>/dev/null || echo "")
KAFKA_PUBLIC_IP=$(terraform output -raw kafka_public_ip 2>/dev/null || echo "")

if [ -z "$MLFLOW_IP" ]; then
  echo "Error: Could not read Terraform outputs. Is the infrastructure deployed?"
  exit 1
fi

PASS=0
FAIL=0

check() {
  local name=$1
  local cmd=$2
  printf "  %-30s" "$name..."
  if eval "$cmd" > /dev/null 2>&1; then
    echo "PASS"
    PASS=$((PASS + 1))
  else
    echo "FAIL"
    FAIL=$((FAIL + 1))
  fi
}

echo "[Connectivity]"
check "Kafka EC2 reachable" "nc -z -w5 $KAFKA_PUBLIC_IP 22"
check "Flink EC2 reachable" "nc -z -w5 $FLINK_IP 22"
check "MLflow EC2 reachable" "nc -z -w5 $MLFLOW_IP 22"

echo ""
echo "[Services]"
check "MLflow tracking (5002)" "curl -sf http://$MLFLOW_IP:5002/health"
check "MLflow serving (5003)" "curl -sf http://$MLFLOW_IP:5003/health"
check "Flink Web UI (8081)" "curl -sf http://$FLINK_IP:8081/overview"

echo ""
echo "[MLflow Inference Test]"
INFERENCE_RESULT=$(curl -sf -X POST http://$MLFLOW_IP:5003/invocations \
  -H "Content-Type: application/json" \
  -d '{"dataframe_records": [{"signal_strength_dbm": -65, "throughput_mbps": 75, "latency_ms": 35, "call_drop_rate_percent": 0.8, "packet_loss_percent": 0.8, "jitter_ms": 2.5}]}' 2>/dev/null || echo "")

if [ -n "$INFERENCE_RESULT" ]; then
  echo "  Inference response: $INFERENCE_RESULT"
  PASS=$((PASS + 1))
else
  echo "  Inference: FAIL (no response)"
  FAIL=$((FAIL + 1))
fi

echo ""
echo "============================================================"
echo "Results: $PASS passed, $FAIL failed"
echo "============================================================"

if [ $FAIL -gt 0 ]; then
  exit 1
fi
