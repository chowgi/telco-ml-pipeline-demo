#!/bin/bash
# Deploy the full Telco ODS streaming ML pipeline demo via CloudFormation.
# Sets up all 4 EC2 instances, deploys code, creates helper scripts, and starts the pipeline.
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

# Prerequisites check
echo "[1/9] Checking prerequisites..."
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
  echo "  export SSH_KEY_PATH=./bennyk_aws_key.pem"
  exit 1
fi

SSH_KEY_PATH="${SSH_KEY_PATH:-$PROJECT_ROOT/${KEY_PAIR_NAME}.pem}"

if [ ! -f "$SSH_KEY_PATH" ]; then
  # Check project directory
  if [ -f "$PROJECT_ROOT/${KEY_PAIR_NAME}.pem" ]; then
    SSH_KEY_PATH="$PROJECT_ROOT/${KEY_PAIR_NAME}.pem"
  elif [ -f "$PROJECT_ROOT/bennyk_aws_key.pem" ]; then
    SSH_KEY_PATH="$PROJECT_ROOT/bennyk_aws_key.pem"
  else
    echo "Error: SSH key not found at $SSH_KEY_PATH"
    echo "  Set SSH_KEY_PATH to the correct .pem file location"
    exit 1
  fi
fi

echo "  Key pair: $KEY_PAIR_NAME"
echo "  SSH key:  $SSH_KEY_PATH"

SSH_OPTS="-o ConnectTimeout=15 -o ServerAliveInterval=5 -o StrictHostKeyChecking=no -i $SSH_KEY_PATH"

# Deploy CloudFormation stack
echo ""
echo "[2/9] Deploying CloudFormation stack..."

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
echo "[3/9] Retrieving instance IPs..."

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
GENERATOR_PRIVATE_IP=$(get_output "GeneratorPrivateIP")
FLINK_IP=$(get_output "FlinkPublicIP")
FLINK_PRIVATE_IP=$(get_output "FlinkPrivateIP")
MLFLOW_IP=$(get_output "MLflowPublicIP")

echo "  Kafka:     $KAFKA_PUBLIC_IP (private: $KAFKA_PRIVATE_IP)"
echo "  Generator: $GENERATOR_IP (private: $GENERATOR_PRIVATE_IP)"
echo "  Flink:     $FLINK_IP (private: $FLINK_PRIVATE_IP)"
echo "  MLflow:    $MLFLOW_IP"

# Wait for instances
echo ""
echo "[4/9] Waiting for instances to initialize (90s)..."
sleep 90

# Atlas IP whitelist
echo ""
echo "[5/9] MongoDB Atlas IP Whitelist"
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
echo "[6/9] Setting up MLflow model..."
scp $SSH_OPTS -r \
  "$PROJECT_ROOT/ml-model/"* ubuntu@${MLFLOW_IP}:/opt/mlflow/app/

ssh $SSH_OPTS ubuntu@${MLFLOW_IP} << 'REMOTEOF'
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

# Upload and set up Flink with helper scripts
echo ""
echo "[7/9] Setting up Apache Flink (PyFlink 1.18) processor..."
scp $SSH_OPTS -r \
  "$PROJECT_ROOT/flink-processor/"* ubuntu@${FLINK_IP}:/tmp/flink-job/

ssh $SSH_OPTS ubuntu@${FLINK_IP} << 'REMOTEOF'
  # Copy job files
  sudo mkdir -p /opt/flink-job
  sudo cp /tmp/flink-job/* /opt/flink-job/
  sudo chown -R ubuntu:ubuntu /opt/flink-job

  # Install requirements
  source /opt/flink-env/bin/activate
  cd /opt/flink-job
  pip install -r requirements.txt -q 2>/dev/null

  # Create restart.sh — kills Flink hard, cleans PIDs, starts cluster, submits job
  cat > /opt/flink-job/restart.sh << 'SCRIPT'
#!/bin/bash
# Hard-kill Flink and restart fresh (required between runs — stale state after cancel)
set -e
echo "Killing Flink processes..."
pkill -9 -f "org.apache.flink" || true
pkill -9 -f "flink_job.py" || true
sleep 2

# Clean PID files
rm -f /opt/flink/log/*.pid 2>/dev/null || true

echo "Starting Flink cluster..."
/opt/flink/bin/start-cluster.sh
sleep 5

echo "Submitting PyFlink job..."
cd /opt/flink-job
source /opt/flink-env/bin/activate
export $(cat /opt/flink-job-config.env | xargs)
/opt/flink/bin/flink run -py flink_job.py \
  -pyexec /opt/flink-env/bin/python3 \
  >> /var/log/flink-job.log 2>&1 &
sleep 5

RUNNING=$(/opt/flink/bin/flink list -r 2>/dev/null | grep -c "RUNNING" || echo "0")
echo "Flink job status: $RUNNING running"
SCRIPT

  # Create stop.sh — kills Flink hard, cleans PIDs
  cat > /opt/flink-job/stop.sh << 'SCRIPT'
#!/bin/bash
# Hard-kill Flink (do not use 'flink cancel' — leaves stale state)
echo "Killing Flink processes..."
pkill -9 -f "org.apache.flink" || true
pkill -9 -f "flink_job.py" || true
rm -f /opt/flink/log/*.pid 2>/dev/null || true
echo "Flink stopped"
SCRIPT

  # Create start.sh — just submits the job (assumes cluster is running)
  cat > /opt/flink-job/start.sh << 'SCRIPT'
#!/bin/bash
# Submit the PyFlink job (cluster must already be running)
set -e
cd /opt/flink-job
source /opt/flink-env/bin/activate
export $(cat /opt/flink-job-config.env | xargs)
/opt/flink/bin/flink run -py flink_job.py \
  -pyexec /opt/flink-env/bin/python3 \
  >> /var/log/flink-job.log 2>&1 &
sleep 5
RUNNING=$(/opt/flink/bin/flink list -r 2>/dev/null | grep -c "RUNNING" || echo "0")
echo "Flink job status: $RUNNING running"
SCRIPT

  chmod +x /opt/flink-job/restart.sh /opt/flink-job/stop.sh /opt/flink-job/start.sh

  # Start the first job
  /opt/flink-job/restart.sh
  echo "Flink processor setup complete"
REMOTEOF

# Upload and set up Generator with helper script
echo ""
echo "[8/9] Setting up data generator (~1k events/sec)..."
scp $SSH_OPTS -r \
  "$PROJECT_ROOT/data-generator/"* ubuntu@${GENERATOR_IP}:/tmp/generator/

ssh $SSH_OPTS ubuntu@${GENERATOR_IP} << 'REMOTEOF'
  # Copy files
  sudo mkdir -p /opt/telco-generator
  sudo cp -r /tmp/generator/* /opt/telco-generator/
  sudo chown -R ubuntu:ubuntu /opt/telco-generator
  cd /opt/telco-generator
  source venv/bin/activate
  pip install -r requirements.txt -q 2>/dev/null

  # Create start.sh — kills old generator, starts new one (properly daemonized)
  cat > /opt/telco-generator/start.sh << 'SCRIPT'
#!/bin/bash
# Kill existing generator and start fresh
pkill -f generator.py || true
sleep 2
cd /opt/telco-generator
source venv/bin/activate
source env.sh
export KAFKA_BROKER KAFKA_TOPIC
nohup python -u generator.py >> /var/log/generator.log 2>&1 &
sleep 3
if pgrep -f generator.py > /dev/null; then
  echo "Generator started (PID $(pgrep -f generator.py))"
else
  echo "ERROR: Generator failed to start"
  exit 1
fi
SCRIPT

  chmod +x /opt/telco-generator/start.sh

  # Start the generator
  /opt/telco-generator/start.sh
  echo "Generator setup complete"
REMOTEOF

# Deploy the live dashboard to the MLflow instance
echo ""
echo "[9/9] Setting up live dashboard..."
scp $SSH_OPTS -r \
  "$PROJECT_ROOT/dashboard/"* ubuntu@${MLFLOW_IP}:/tmp/dashboard/

# Copy SSH key for intra-VPC communication (dashboard SSHes to generator/flink)
scp $SSH_OPTS "$SSH_KEY_PATH" ubuntu@${MLFLOW_IP}:/tmp/dashboard_key.pem

ssh $SSH_OPTS ubuntu@${MLFLOW_IP} << REMOTEOF
  sudo mkdir -p /opt/dashboard
  sudo cp -r /tmp/dashboard/* /opt/dashboard/
  sudo cp /tmp/dashboard_key.pem /opt/dashboard/bennyk_aws_key.pem
  sudo chmod 400 /opt/dashboard/bennyk_aws_key.pem
  sudo chown -R ubuntu:ubuntu /opt/dashboard

  # Install dashboard dependencies
  cd /opt/dashboard
  /opt/mlflow/venv/bin/pip install flask "pymongo[srv]" dash plotly -q 2>/dev/null

  # Start the dashboard
  export \$(cat /opt/mlflow/.env | xargs)
  export SSH_KEY=/opt/dashboard/bennyk_aws_key.pem
  export GENERATOR_IP=${GENERATOR_PRIVATE_IP}
  export FLINK_IP=${FLINK_PRIVATE_IP}
  nohup /opt/mlflow/venv/bin/python /opt/dashboard/app.py >> /var/log/dashboard.log 2>&1 &
  sleep 2
  echo "Dashboard started on port 8050"
REMOTEOF

# Summary
echo ""
echo "============================================================"
echo "DEPLOYMENT COMPLETE"
echo "============================================================"
echo ""
echo "Endpoints:"
echo "  Dashboard:        http://${MLFLOW_IP}:8050 (Start/Stop Demo buttons)"
echo "  Flink Web UI:     http://${FLINK_IP}:8081"
echo "  MLflow Tracking:  http://${MLFLOW_IP}:5002"
echo "  MLflow Inference: http://${MLFLOW_IP}:5003/invocations"
echo ""
echo "Next step - configure Atlas Trigger (ap-southeast-2):"
echo "  1. Atlas > App Services > Triggers"
echo "  2. Collection: ods_demo_db.windowed_network_metrics (Insert)"
echo "  3. Paste: atlas-trigger/trigger_function.js"
echo "  4. Set MLFLOW_ENDPOINT value: http://${MLFLOW_IP}:5003/invocations"
echo ""
echo "Pipeline details:"
echo "  - Generator: ~1k events/sec (1 thread, 100/batch, 0.1s sleep)"
echo "  - Flink: 30-second emission per cell tower (sliding window)"
echo "  - Cell degradation: 30% excellent, 12% degraded, 6% poor (rotates every 30s)"
echo ""
echo "Data will appear on the dashboard within ~30 seconds."
echo ""
echo "To manage the demo, use the dashboard or:"
echo "  Start: ./infrastructure/scripts/start_demo.sh"
echo "  Stop:  ./infrastructure/scripts/stop_demo.sh"
echo "  Tear down: ./infrastructure/scripts/teardown.sh"
