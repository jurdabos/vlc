#!/usr/bin/env bash
#
# bootstrap_kafka.sh - Initialize Kafka topics and deploy connectors
#
# This script:
#   1. Waits for Kafka broker to be ready
#   2. Creates data topics (vlc.air, vlc.weather)
#   3. Creates Kafka Connect internal topics
#   4. Waits for Schema Registry (required for JSON Schema)
#   5. Waits for Connect and deploys JDBC sink connectors
#
# Usage:
#   ./scripts/bootstrap_kafka.sh
#
# Prerequisites:
#   - Docker Compose infra profile running
#   - ./scripts/sync-dev-secrets.sh executed
#
set -euo pipefail
cd "$(dirname "$0")/.."

# --- Defaults (override via env) ---
BROKER_CONTAINER="${BROKER_CONTAINER:-kafka}"
BOOTSTRAP="${BOOTSTRAP:-kafka:9092}"

DATA_TOPIC="${DATA_TOPIC:-vlc.air}"
DATA_TOPIC_2="${DATA_TOPIC_2:-vlc.weather}"
DATA_PARTITIONS="${DATA_PARTITIONS:-3}"
DATA_RF="${DATA_RF:-1}"
DATA_RETENTION_MS="${DATA_RETENTION_MS:-2592000000}"     # 30 days

CFG_TOPIC="${CFG_TOPIC:-_connect-configs}"
OFF_TOPIC="${OFF_TOPIC:-_connect-offsets}"
STS_TOPIC="${STS_TOPIC:-_connect-status}"

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://schema-registry:8081}"

# --- Helpers ---
exec_in_broker() {
  # Using /bin/sh to avoid bash dependency
  docker exec "${BROKER_CONTAINER}" sh -lc "$*"
}

http_ok() {
  docker exec connect curl -s -o /dev/null -w "%{http_code}" "$1" | grep -qE '^(200|201)$'
}

wait_for_broker() {
  echo "[bootstrap] Waiting for broker at ${BOOTSTRAP} ..."
  until exec_in_broker "kafka-topics --bootstrap-server ${BOOTSTRAP} --list >/dev/null 2>&1"; do
    sleep 2
  done
  echo "[bootstrap] Broker is up."
}

create_topic () {
  local name="$1" parts="$2" rf="$3" cfg="$4"
  echo "[bootstrap] Ensuring topic '${name}' exists ..."
  exec_in_broker "kafka-topics --bootstrap-server ${BOOTSTRAP} \
    --create --if-not-exists --topic '${name}' --partitions ${parts} \
    --replication-factor ${rf} ${cfg}"
}

wait_for_connect() {
  echo "[bootstrap] Waiting for Connect at ${CONNECT_URL} ..."
  for _ in $(seq 1 60); do
    if http_ok "${CONNECT_URL}/connectors"; then
      echo "[bootstrap] Connect is up at ${CONNECT_URL}"
      return 0
    fi
    sleep 2
  done
  echo "[bootstrap] WARN: Connect not reachable at ${CONNECT_URL}; continuing without connector creation."
  return 1
}

wait_for_schema_registry() {
  echo "[bootstrap] Waiting for Schema Registry at ${SCHEMA_REGISTRY_URL} ..."
  for _ in $(seq 1 30); do
    if docker exec connect curl -s -o /dev/null -w "%{http_code}" "${SCHEMA_REGISTRY_URL}/subjects" 2>/dev/null | grep -q "200"; then
      echo "[bootstrap] Schema Registry is up at ${SCHEMA_REGISTRY_URL}"
      return 0
    fi
    sleep 2
  done
  echo "[bootstrap] WARN: Schema Registry not reachable; connectors may fail if using JSON Schema."
  return 1
}

upsert_connector() {
  # Uses PUT for idempotent updates; falls back to POST if needed
  local cfg_file="$1"
  local name
  name="$(jq -r '.name // empty' "${cfg_file}")"
  if [ -z "${name}" ] || [ "${name}" = "null" ]; then
    echo "[bootstrap] ERROR: connector config '${cfg_file}' must include a 'name' field." >&2
    return 1
  fi
  # 1) Try PUT (update-or-create), streaming only .config from the host into the container
  local code
  code=$(
    jq -c '.config' "${cfg_file}" | docker exec -i connect sh -lc "
      curl -s -o /dev/null -w '%{http_code}' \
           -X PUT -H 'Content-Type: application/json' \
           --data @- '${CONNECT_URL}/connectors/${name}/config'
    " || true
  )
  # 2) If 404, try POST (create) with full payload
  if [ "${code}" = "404" ]; then
    code=$(
      docker exec -i connect sh -lc "
        curl -s -o /dev/null -w '%{http_code}' \
             -X POST -H 'Content-Type: application/json' \
             --data @- '${CONNECT_URL}/connectors'
      " < "${cfg_file}" || true
    )
  fi
  if ! grep -qE '^(200|201)$' <<< "${code}"; then
    echo "[bootstrap] WARN: connector upsert for '${name}' returned HTTP ${code}"
  else
    echo "[bootstrap] Connector '${name}' ensured (HTTP ${code})."
  fi
}

# --- Run sequence ---
wait_for_broker

# Data topics (delete policy + retention)
create_topic "${DATA_TOPIC}"   "${DATA_PARTITIONS}" "${DATA_RF}" "--config cleanup.policy=delete --config retention.ms=${DATA_RETENTION_MS}"
create_topic "${DATA_TOPIC_2}" "${DATA_PARTITIONS}" "${DATA_RF}" "--config cleanup.policy=delete --config retention.ms=${DATA_RETENTION_MS}"

# Connect internal topics (compact, single partition)
create_topic "${CFG_TOPIC}" 1 "${DATA_RF}" "--config cleanup.policy=compact"
create_topic "${OFF_TOPIC}" 1 "${DATA_RF}" "--config cleanup.policy=compact"
create_topic "${STS_TOPIC}" 1 "${DATA_RF}" "--config cleanup.policy=compact"

echo "[bootstrap] Topics present:"
exec_in_broker "kafka-topics --bootstrap-server ${BOOTSTRAP} --describe --topic ${DATA_TOPIC}; kafka-topics --bootstrap-server ${BOOTSTRAP} --describe --topic ${DATA_TOPIC_2}; kafka-topics --bootstrap-server ${BOOTSTRAP} --describe --topic ${CFG_TOPIC}; kafka-topics --bootstrap-server ${BOOTSTRAP} --describe --topic ${OFF_TOPIC}; kafka-topics --bootstrap-server ${BOOTSTRAP} --describe --topic ${STS_TOPIC}"

# Schema Registry (required for JSON Schema serialization)
wait_for_schema_registry || true

# Connectors (after broker + topics + schema registry)
if wait_for_connect; then
  echo "[bootstrap] Deploying JDBC sink connectors ..."
  upsert_connector "connect/config/jdbc-sink.timescale.air.json"
  upsert_connector "connect/config/jdbc-sink.timescale.weather.json"
  
  # Showing connector status
  sleep 3
  echo "[bootstrap] Connector status:"
  docker exec connect curl -s "${CONNECT_URL}/connectors?expand=status" 2>/dev/null | \
    jq -r 'to_entries[] | "  \(.key): connector=\(.value.status.connector.state), task=\(.value.status.tasks[0].state // "NO_TASK")"' || true
fi

echo ""
echo "[bootstrap] Done."
echo ""
echo "[bootstrap] Quick reference:"
echo "  - Kafka UI:     http://localhost:8080/kafka-ui/"
echo "  - Grafana:      http://localhost:8080/grafana/"
echo "  - Connect API:  http://localhost:8083/connectors"
echo ""
echo "[bootstrap] To start producers:"
echo "  docker compose --profile producer up -d"
