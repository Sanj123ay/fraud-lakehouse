#!/usr/bin/env bash
# ============================================================================
#  Switches CDC ON with an idempotent REST call to Kafka Connect (port 8083).
#  After this, every INSERT/UPDATE/DELETE in the bank DB flows into Kafka.
#  Run:  bash cdc/register_connector.sh      (or: make register-cdc)
# ============================================================================
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="$SCRIPT_DIR/debezium/customers-connector.json"
CONNECT_URL="${KAFKA_CONNECT_URL:-http://localhost:8083}"
echo "Checking Kafka Connect availability at ${CONNECT_URL}..."
for i in {1..30}; do
  if curl -s -f "${CONNECT_URL}/connectors" >/dev/null 2>&1; then
    echo "Kafka Connect is ready."
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo "Error: Kafka Connect at ${CONNECT_URL} is not responding." >&2
    echo "Ensure the connect container is running: docker compose ps connect" >&2
    exit 1
  fi
  sleep 2
done
CONNECTOR_NAME=$(python3 -c "import json; print(json.load(open('${CONFIG_FILE}'))['name'])")
# Check if connector already exists
EXISTING=$(curl -s -o /dev/null -w "%{http_code}" "${CONNECT_URL}/connectors/${CONNECTOR_NAME}" || true)
if [ "$EXISTING" = "200" ]; then
  echo "Connector '${CONNECTOR_NAME}' already registered. Updating configuration..."
  CONFIG_BODY=$(python3 -c "import json; print(json.dumps(json.load(open('${CONFIG_FILE}'))['config']))")
  curl -s -X PUT "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/config" \
    -H 'Content-Type: application/json' \
    -d "${CONFIG_BODY}" | python3 -m json.tool || true
else
  echo "Registering new connector '${CONNECTOR_NAME}'..."
  curl -s -X POST "${CONNECT_URL}/connectors" \
    -H 'Content-Type: application/json' \
    -d @"${CONFIG_FILE}" | python3 -m json.tool || true
fi
echo ""
echo "Waiting 3 seconds for connector initialization..."
sleep 3
echo "=== Connector Status ==="
curl -s "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status" | python3 -m json.tool || true
echo "========================"
