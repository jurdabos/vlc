#!/usr/bin/env bash
set -euo pipefail

# --- Defaults (override via env) ---
BROKER_CONTAINER="${BROKER_CONTAINER:-broker}"
BOOTSTRAP="${BOOTSTRAP:-broker:9092}"

DATA_TOPIC="${DATA_TOPIC:-valencia.air}"
DATA_TOPIC_2="${DATA_TOPIC_2:-valencia.weather}"
DATA_PARTITIONS="${DATA_PARTITIONS:-3}"
DATA_RF="${DATA_RF:-1}"
DATA_RETENTION_MS="${DATA_RETENTION_MS:-2592000000}"     # 30 days

CFG_TOPIC="${CFG_TOPIC:-connect-configs}"
OFF_TOPIC="${OFF_TOPIC:-connect-offsets}"
STS_TOPIC="${STS_TOPIC:-connect-status}"

CONNECT_URL="${CONNECT_URL:-http://connect:8083}"
FALLBACK_CONNECT_URL="http://127.0.0.1:8083"             # on VM host or via SSH -L

# --- Helpers ---
exec_in_broker() {
  # Using /bin/sh to avoid bash dependency
  docker exec "${BROKER_CONTAINER}" sh -lc "$*"
}

http_ok() {
  curl -s -o /dev/null -w "%{http_code}" "$1" | grep -qE '^(200|201)$'
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
  # Choosing best URL
  local url="${CONNECT_URL}"
  if ! http_ok "${url}/connectors"; then
    url="${FALLBACK_CONNECT_URL}"
  fi
  echo "[bootstrap] Waiting for Connect at ${url} ..."
  for _ in $(seq 1 60); do
    if http_ok "${url}/connectors"; then
      echo "[bootstrap] Connect is up at ${url}"
      CONNECT_URL="${url}"
      return 0
    fi
    sleep 2
  done
  echo "[bootstrap] WARN: Connect not reachable at ${CONNECT_URL} or ${FALLBACK_CONNECT_URL}; continuing without connector creation."
  return 1
}

upsert_connector() {
  # Uses PUT for idempotent updates; falls back to POST if needed
  local cfg_file="$1"
  local name
  name="$(jq -r '.name // empty' < "${cfg_file}")"
  if [ -z "${name}" ]; then
    echo "[bootstrap] ERROR: connector config '${cfg_file}' must include a 'name' field." >&2
    return 1
  fi
  # Try PUT (update-or-create), then POST if 404
  local code
  code=$(curl -s -o /dev/null -w "%{http_code}" -X PUT -H "Content-Type: application/json" \
         --data @"${cfg_file}" "${CONNECT_URL}/connectors/${name}/config" || true)
  if [ "${code}" = "404" ]; then
    code=$(curl -s -o /dev/null -w "%{http_code}" -X POST -H "Content-Type: application/json" \
           --data @"${cfg_file}" "${CONNECT_URL}/connectors" || true)
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
exec_in_broker "kafka-topics --bootstrap-server ${BOOTSTRAP} --describe --topic ${DATA_TOPIC} ${DATA_TOPIC_2} ${CFG_TOPIC} ${OFF_TOPIC} ${STS_TOPIC}"

# Connectors (after broker + topics)
if wait_for_connect; then
  upsert_connector "connect/config/jdbc-sink.timescale.air.json"
  upsert_connector "connect/config/jdbc-sink.timescale.weather.json"
fi

echo "[bootstrap] Done."
