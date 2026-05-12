#!/bin/bash
set -e

exec > /var/log/generator-setup.log 2>&1

apt-get update
apt-get install -y python3-pip python3-venv git

mkdir -p /opt/telco-generator
cd /opt/telco-generator

python3 -m venv venv
source venv/bin/activate

pip install confluent-kafka numpy

cat > /opt/telco-generator/config.py << 'PYEOF'
KAFKA_BROKER = "${kafka_broker}:9092"
KAFKA_TOPIC = "telco-raw-telemetry"
NUM_CELL_TOWERS = 50
NUM_PRODUCER_THREADS = 8
BATCH_SIZE = 65536
LINGER_MS = 5
ANOMALY_RATE = 0.05
PYEOF

echo "Generator setup complete. Upload generator.py and start manually."
