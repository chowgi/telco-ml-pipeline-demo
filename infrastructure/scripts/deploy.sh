#!/bin/bash
# Deploy the full Telco ODS streaming ML pipeline demo
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
TERRAFORM_DIR="$PROJECT_ROOT/infrastructure/terraform"

echo "============================================================"
echo "Telco ODS - Autonomous Networks ML Pipeline Demo"
echo "============================================================"
echo ""

# Prerequisites check
echo "[1/8] Checking prerequisites..."
for cmd in terraform aws ssh scp; do
  if ! command -v $cmd &> /dev/null; then
    echo "Error: $cmd is required but not installed."
    exit 1
  fi
done
echo "  All prerequisites found."

# Terraform variables
if [ -z "$TF_VAR_key_pair_name" ]; then
  echo ""
  echo "Error: Set TF_VAR_key_pair_name to your EC2 key pair name"
  echo "  export TF_VAR_key_pair_name=your-key-pair"
  echo ""
  echo "Optional variables:"
  echo "  export TF_VAR_allowed_ssh_cidr=your.ip/32"
  echo "  export SSH_KEY_PATH=~/.ssh/your-key.pem"
  exit 1
fi

SSH_KEY_PATH="${SSH_KEY_PATH:-~/.ssh/${TF_VAR_key_pair_name}.pem}"

# Terraform apply
echo ""
echo "[2/8] Provisioning AWS infrastructure..."
cd "$TERRAFORM_DIR"
terraform init -input=false
terraform apply -auto-approve

# Get outputs
KAFKA_IP=$(terraform output -raw kafka_private_ip)
KAFKA_PUBLIC_IP=$(terraform output -raw kafka_public_ip)
GENERATOR_IP=$(terraform output -raw generator_public_ip)
FLINK_IP=$(terraform output -raw flink_public_ip)
MLFLOW_IP=$(terraform output -raw mlflow_public_ip)

echo ""
echo "  Kafka:     $KAFKA_PUBLIC_IP (private: $KAFKA_IP)"
echo "  Generator: $GENERATOR_IP"
echo "  Flink:     $FLINK_IP"
echo "  MLflow:    $MLFLOW_IP"

# Wait for instances to be ready
echo ""
echo "[3/8] Waiting for instances to initialize (60s)..."
sleep 60

# Whitelist IPs in Atlas
echo ""
echo "[4/8] Whitelisting EC2 IPs in MongoDB Atlas..."
echo "  NOTE: Add these IPs to Atlas Network Access:"
for ip in $KAFKA_PUBLIC_IP $GENERATOR_IP $FLINK_IP $MLFLOW_IP; do
  echo "    - $ip/32"
done
echo ""
echo "  Or whitelist 0.0.0.0/0 temporarily for the demo."
echo "  Press Enter when done..."
read -r

# Upload and start MLflow
echo ""
echo "[5/8] Setting up MLflow server..."
scp -i "$SSH_KEY_PATH" -o StrictHostKeyChecking=no -r \
  "$PROJECT_ROOT/ml-model/" ubuntu@${MLFLOW_IP}:/opt/mlflow/app/
ssh -i "$SSH_KEY_PATH" -o StrictHostKeyChecking=no ubuntu@${MLFLOW_IP} << 'EOF'
  cd /opt/mlflow
  source venv/bin/activate
  cd app
  pip install -r requirements.txt -q
  python generate_training_data.py
  python train_model.py
  nohup bash serve_model.sh > /var/log/mlflow-serve.log 2>&1 &
  sleep 5
  echo "MLflow model serving started"
EOF

# Upload and start Flink job
echo ""
echo "[6/8] Setting up Flink processor..."
scp -i "$SSH_KEY_PATH" -o StrictHostKeyChecking=no -r \
  "$PROJECT_ROOT/flink-processor/" ubuntu@${FLINK_IP}:/opt/flink-job/
ssh -i "$SSH_KEY_PATH" -o StrictHostKeyChecking=no ubuntu@${FLINK_IP} << EOF
  source /opt/flink-env/bin/activate
  cd /opt/flink-job
  pip install -r requirements.txt -q
  export KAFKA_BROKER=${KAFKA_IP}:9092
  export KAFKA_TOPIC=telco-raw-telemetry
  export MONGODB_URI="${TF_VAR_mongodb_uri:-mongodb+srv://admin:IpAQeOM854tLQ5rP@democluster.hyszr.mongodb.net/?retryWrites=true&w=majority&appName=DemoCluster}"
  export MONGODB_DB=ods_demo_db
  export MONGODB_COLLECTION=windowed_network_metrics
  export WINDOW_SIZE_MINUTES=5
  nohup python flink_job.py > /var/log/flink-job.log 2>&1 &
  echo "Flink job submitted"
EOF

# Upload and start generator
echo ""
echo "[7/8] Starting data generator..."
scp -i "$SSH_KEY_PATH" -o StrictHostKeyChecking=no -r \
  "$PROJECT_ROOT/data-generator/" ubuntu@${GENERATOR_IP}:/opt/telco-generator/app/
ssh -i "$SSH_KEY_PATH" -o StrictHostKeyChecking=no ubuntu@${GENERATOR_IP} << EOF
  cd /opt/telco-generator
  source venv/bin/activate
  cd app
  pip install -r requirements.txt -q
  export KAFKA_BROKER=${KAFKA_IP}:9092
  nohup python generator.py > /var/log/generator.log 2>&1 &
  echo "Generator started"
EOF

# Summary
echo ""
echo "[8/8] Setup Atlas trigger..."
echo "  Configure the Atlas Database Trigger manually:"
echo "  1. Go to Atlas > App Services > Triggers"
echo "  2. Create trigger on: ods_demo_db.windowed_network_metrics (Insert)"
echo "  3. Paste code from: atlas-trigger/trigger_function.js"
echo "  4. Set MLFLOW_ENDPOINT value to: http://${MLFLOW_IP}:5003/invocations"
echo ""
echo "============================================================"
echo "DEPLOYMENT COMPLETE"
echo "============================================================"
echo ""
echo "Endpoints:"
echo "  MLflow Tracking:  http://${MLFLOW_IP}:5002"
echo "  MLflow Inference: http://${MLFLOW_IP}:5003/invocations"
echo "  Flink Web UI:     http://${FLINK_IP}:8081"
echo ""
echo "Data should appear in MongoDB within 5 minutes:"
echo "  Collection: ods_demo_db.windowed_network_metrics"
echo "  Predictions: ods_demo_db.network_health_predictions"
echo ""
echo "To tear down: ./teardown.sh"
