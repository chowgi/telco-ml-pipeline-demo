#!/bin/bash
set -e

exec > /var/log/mlflow-setup.log 2>&1

apt-get update
apt-get install -y python3-pip python3-venv

mkdir -p /opt/mlflow
cd /opt/mlflow

python3 -m venv venv
source venv/bin/activate

pip install mlflow scikit-learn pandas numpy pymongo[srv] python-dotenv

cat > /opt/mlflow/.env << EOF
MONGODB_URI=${mongodb_uri}
MLFLOW_TRACKING_URI=http://0.0.0.0:5002
EOF

# Start MLflow tracking server
cat > /etc/systemd/system/mlflow-tracking.service << EOF
[Unit]
Description=MLflow Tracking Server
After=network.target

[Service]
Type=simple
WorkingDirectory=/opt/mlflow
ExecStart=/opt/mlflow/venv/bin/mlflow server --host 0.0.0.0 --port 5002 --backend-store-uri sqlite:///mlflow.db --default-artifact-root /opt/mlflow/artifacts
Restart=on-failure

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable mlflow-tracking
systemctl start mlflow-tracking

echo "MLflow setup complete. Upload model training code and train."
