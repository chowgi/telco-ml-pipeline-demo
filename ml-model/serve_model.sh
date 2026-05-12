#!/bin/bash
# Serve the trained MLflow model for real-time inference
set -e

export MLFLOW_TRACKING_URI="${MLFLOW_TRACKING_URI:-http://localhost:5002}"

echo "Starting MLflow model serving on port 5003..."
echo "Tracking URI: $MLFLOW_TRACKING_URI"

mlflow models serve \
  -m "models:/telco_ods_network_health_classifier/latest" \
  -p 5003 \
  --host 0.0.0.0 \
  --no-conda \
  --workers 4
