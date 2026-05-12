#!/bin/bash
set -e

exec > /var/log/flink-setup.log 2>&1

apt-get update
apt-get install -y default-jdk python3-pip python3-venv wget

FLINK_VERSION="1.18.1"

cd /opt
wget "https://archive.apache.org/dist/flink/flink-${FLINK_VERSION}/flink-${FLINK_VERSION}-bin-scala_2.12.tgz"
tar -xzf "flink-${FLINK_VERSION}-bin-scala_2.12.tgz"
ln -s "flink-${FLINK_VERSION}" flink

# Download Kafka connector JAR
wget -P /opt/flink/lib/ "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar"

# Configure Flink
cat > /opt/flink/conf/flink-conf.yaml << EOF
jobmanager.rpc.address: localhost
jobmanager.rpc.port: 6123
jobmanager.memory.process.size: 4096m
taskmanager.memory.process.size: 12288m
taskmanager.numberOfTaskSlots: 8
parallelism.default: 4
rest.port: 8081
rest.address: 0.0.0.0
EOF

# Setup Python environment for PyFlink
python3 -m venv /opt/flink-env
source /opt/flink-env/bin/activate
pip install apache-flink==1.18.1 pymongo[srv] numpy

# Store config for the Flink job
cat > /opt/flink-job-config.env << EOF
KAFKA_BROKER=${kafka_broker}:9092
KAFKA_TOPIC=telco-raw-telemetry
MONGODB_URI=${mongodb_uri}
MONGODB_DB=ods_demo_db
MONGODB_COLLECTION=windowed_network_metrics
WINDOW_SIZE_MINUTES=5
EOF

# Start Flink cluster
/opt/flink/bin/start-cluster.sh

echo "Flink setup complete. Upload flink_job.py and submit."
