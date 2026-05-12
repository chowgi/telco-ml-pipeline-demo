#!/bin/bash
# Tear down all AWS resources for the Telco ODS demo
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TERRAFORM_DIR="$SCRIPT_DIR/../terraform"

echo "============================================================"
echo "Telco ODS Demo - Teardown"
echo "============================================================"
echo ""
echo "This will destroy ALL AWS resources created by this demo."
echo "Press Ctrl+C to cancel, or Enter to continue..."
read -r

echo ""
echo "[1/3] Destroying AWS infrastructure..."
cd "$TERRAFORM_DIR"
terraform destroy -auto-approve

echo ""
echo "[2/3] Cleaning up..."
echo "  NOTE: MongoDB Atlas collections are NOT deleted."
echo "  To clean up MongoDB data manually:"
echo "    - Drop ods_demo_db.windowed_network_metrics"
echo "    - Drop ods_demo_db.network_health_predictions"
echo "    - Drop ods_demo_db.feast_online_features"
echo "    - Remove Atlas trigger via App Services UI"
echo "    - Remove IP whitelist entries"

echo ""
echo "[3/3] Done."
echo ""
echo "All AWS resources have been destroyed."
echo "Estimated savings: ~\$12.50/day"
