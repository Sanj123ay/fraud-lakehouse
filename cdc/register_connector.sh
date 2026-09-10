#!/usr/bin/env bash
# ============================================================================
#  Switches CDC ON with a single REST call to Kafka Connect (port 8083).
#  After this, every INSERT/UPDATE/DELETE in the bank DB flows into Kafka.
#  Run:  bash cdc/register_connector.sh      (or: make register-cdc)
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

curl -s -X POST http://localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  -d @"$SCRIPT_DIR/debezium/customers-connector.json" | python3 -m json.tool

echo ""
echo "Connector registered. Check status with:"
echo "  curl -s http://localhost:8083/connectors/shopdb-connector/status | python3 -m json.tool"
