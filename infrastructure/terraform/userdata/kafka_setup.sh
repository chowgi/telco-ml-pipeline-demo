#!/bin/bash
set -e

exec > /var/log/kafka-setup.log 2>&1

apt-get update
apt-get install -y default-jdk wget

KAFKA_VERSION="3.7.0"
SCALA_VERSION="2.13"

cd /opt
wget "https://downloads.apache.org/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
tar -xzf "kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
ln -s "kafka_${SCALA_VERSION}-${KAFKA_VERSION}" kafka

PRIVATE_IP=$(curl -s http://169.254.169.254/latest/meta-data/local-ipv4)

cat > /opt/kafka/config/server.properties << EOF
broker.id=0
listeners=PLAINTEXT://${PRIVATE_IP}:9092
advertised.listeners=PLAINTEXT://${PRIVATE_IP}:9092
num.network.threads=8
num.io.threads=16
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
log.dirs=/var/kafka-logs
num.partitions=12
num.recovery.threads.per.data.dir=2
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
log.retention.hours=24
log.segment.bytes=1073741824
log.retention.check.interval.ms=300000
zookeeper.connect=localhost:2181
zookeeper.connection.timeout.ms=18000
EOF

mkdir -p /var/kafka-logs

# Start Zookeeper
cat > /etc/systemd/system/zookeeper.service << EOF
[Unit]
Description=Apache Zookeeper
After=network.target

[Service]
Type=simple
ExecStart=/opt/kafka/bin/zookeeper-server-start.sh /opt/kafka/config/zookeeper.properties
ExecStop=/opt/kafka/bin/zookeeper-server-stop.sh
Restart=on-failure

[Install]
WantedBy=multi-user.target
EOF

# Start Kafka
cat > /etc/systemd/system/kafka.service << EOF
[Unit]
Description=Apache Kafka
After=zookeeper.service
Requires=zookeeper.service

[Service]
Type=simple
ExecStart=/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties
ExecStop=/opt/kafka/bin/kafka-server-stop.sh
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable zookeeper kafka
systemctl start zookeeper
sleep 10
systemctl start kafka
sleep 10

# Create topic
/opt/kafka/bin/kafka-topics.sh --create \
  --bootstrap-server ${PRIVATE_IP}:9092 \
  --topic telco-raw-telemetry \
  --partitions 12 \
  --replication-factor 1 \
  --config retention.ms=86400000

echo "Kafka setup complete"
