#!/bin/bash
# Sets up the Atlas Database Trigger via the Atlas Admin API.
# Requires: ATLAS_PUBLIC_KEY, ATLAS_PRIVATE_KEY, ATLAS_PROJECT_ID, ATLAS_APP_ID
set -e

ATLAS_PUBLIC_KEY="${ATLAS_PUBLIC_KEY}"
ATLAS_PRIVATE_KEY="${ATLAS_PRIVATE_KEY}"
ATLAS_PROJECT_ID="${ATLAS_PROJECT_ID}"
ATLAS_APP_ID="${ATLAS_APP_ID}"
MLFLOW_ENDPOINT="${MLFLOW_ENDPOINT}"

if [ -z "$ATLAS_PUBLIC_KEY" ] || [ -z "$ATLAS_PRIVATE_KEY" ] || [ -z "$ATLAS_PROJECT_ID" ]; then
  echo "Error: Set ATLAS_PUBLIC_KEY, ATLAS_PRIVATE_KEY, ATLAS_PROJECT_ID"
  echo ""
  echo "Manual setup instructions:"
  echo "1. Go to Atlas UI > App Services > Create Application"
  echo "2. Create a Database Trigger on 'ods_demo_db.windowed_network_metrics'"
  echo "3. Operation Type: Insert"
  echo "4. Full Document: enabled"
  echo "5. Paste trigger_function.js as the function body"
  echo "6. Create a Value named 'MLFLOW_ENDPOINT' with: ${MLFLOW_ENDPOINT:-http://<mlflow-ip>:5003/invocations}"
  exit 1
fi

echo "Setting up Atlas trigger..."
echo "  Project: $ATLAS_PROJECT_ID"
echo "  MLflow endpoint: $MLFLOW_ENDPOINT"

# Create App Services Value for MLflow endpoint
curl -s --digest -u "${ATLAS_PUBLIC_KEY}:${ATLAS_PRIVATE_KEY}" \
  -X POST "https://services.cloud.mongodb.com/api/admin/v3.0/groups/${ATLAS_PROJECT_ID}/apps/${ATLAS_APP_ID}/values" \
  -H "Content-Type: application/json" \
  -d "{
    \"name\": \"MLFLOW_ENDPOINT\",
    \"value\": \"${MLFLOW_ENDPOINT}\"
  }"

echo "Trigger setup complete. Verify in Atlas UI > App Services > Triggers."
echo ""
echo "NOTE: The trigger function code must be uploaded manually or via realm-cli:"
echo "  realm-cli push --project=${ATLAS_PROJECT_ID}"
