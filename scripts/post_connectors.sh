#!/usr/bin/env bash
#
# post_connectors.sh - Deploy and manage JDBC sink connectors
#
# Usage:
#   ./scripts/post_connectors.sh [deploy|delete|status|restart]
#
# Commands:
#   deploy  - Deploy connectors (default if no command given)
#   delete  - Delete all connectors
#   status  - Show connector status
#   restart - Delete then deploy connectors
#
set -euo pipefail
cd "$(dirname "$0")/.."

# --- Configuration ---
CONNECT_CONTAINER="${CONNECT_CONTAINER:-connect}"
CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
CONFIG_DIR="${CONFIG_DIR:-connect/config}"
SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://schema-registry:8081}"

# Connector config files to deploy
CONNECTOR_CONFIGS=(
  "jdbc-sink.timescale.air.json"
  "jdbc-sink.timescale.weather.json"
)

# --- Helpers ---
log() { echo "[connectors] $*"; }
err() { echo "[connectors] ERROR: $*" >&2; }

exec_in_connect() {
  docker exec "${CONNECT_CONTAINER}" sh -c "$*"
}

http_code() {
  exec_in_connect "curl -s -o /dev/null -w '%{http_code}' '$1'" 2>/dev/null || echo "000"
}

wait_for_connect() {
  log "Waiting for Connect at ${CONNECT_URL} ..."
  local attempts=0
  while [ $attempts -lt 60 ]; do
    if [ "$(http_code "${CONNECT_URL}/connectors")" = "200" ]; then
      log "Connect is ready."
      return 0
    fi
    sleep 2
    attempts=$((attempts + 1))
  done
  err "Connect not reachable after 120 seconds."
  return 1
}

wait_for_schema_registry() {
  log "Waiting for Schema Registry at ${SCHEMA_REGISTRY_URL} ..."
  local attempts=0
  while [ $attempts -lt 30 ]; do
    if exec_in_connect "curl -s -o /dev/null -w '%{http_code}' '${SCHEMA_REGISTRY_URL}/subjects'" 2>/dev/null | grep -q "200"; then
      log "Schema Registry is ready."
      return 0
    fi
    sleep 2
    attempts=$((attempts + 1))
  done
  err "Schema Registry not reachable after 60 seconds."
  return 1
}

get_connector_name() {
  local cfg_file="$1"
  jq -r '.name // empty' "${cfg_file}" 2>/dev/null || echo ""
}

delete_connector() {
  local name="$1"
  log "Deleting connector '${name}' ..."
  local code
  code=$(exec_in_connect "curl -s -o /dev/null -w '%{http_code}' -X DELETE '${CONNECT_URL}/connectors/${name}'")
  if [ "${code}" = "204" ] || [ "${code}" = "404" ]; then
    log "Connector '${name}' deleted (HTTP ${code})."
  else
    err "Failed to delete '${name}' (HTTP ${code})."
  fi
}

deploy_connector() {
  local cfg_file="$1"
  local name
  name=$(get_connector_name "${cfg_file}")
  
  if [ -z "${name}" ]; then
    err "Config '${cfg_file}' missing 'name' field, skipping."
    return 1
  fi
  
  log "Deploying connector '${name}' from ${cfg_file} ..."
  
  # Using PUT for idempotent upsert
  local code
  code=$(jq -c '.config' "${cfg_file}" | docker exec -i "${CONNECT_CONTAINER}" sh -c "
    curl -s -o /dev/null -w '%{http_code}' \
         -X PUT -H 'Content-Type: application/json' \
         --data @- '${CONNECT_URL}/connectors/${name}/config'
  " 2>/dev/null || echo "000")
  
  # If connector doesn't exist, try POST
  if [ "${code}" = "404" ]; then
    code=$(docker exec -i "${CONNECT_CONTAINER}" sh -c "
      curl -s -o /dev/null -w '%{http_code}' \
           -X POST -H 'Content-Type: application/json' \
           --data @- '${CONNECT_URL}/connectors'
    " < "${cfg_file}" 2>/dev/null || echo "000")
  fi
  
  if echo "${code}" | grep -qE '^(200|201)$'; then
    log "Connector '${name}' deployed successfully (HTTP ${code})."
  else
    err "Failed to deploy '${name}' (HTTP ${code})."
    return 1
  fi
}

show_status() {
  log "Connector status:"
  local connectors
  connectors=$(exec_in_connect "curl -s '${CONNECT_URL}/connectors'" | jq -r '.[]' 2>/dev/null)
  
  if [ -z "${connectors}" ]; then
    log "No connectors deployed."
    return
  fi
  
  for name in ${connectors}; do
    local status
    status=$(exec_in_connect "curl -s '${CONNECT_URL}/connectors/${name}/status'" 2>/dev/null)
    local conn_state task_state
    conn_state=$(echo "${status}" | jq -r '.connector.state // "UNKNOWN"')
    task_state=$(echo "${status}" | jq -r '.tasks[0].state // "NO TASK"')
    
    if [ "${conn_state}" = "RUNNING" ] && [ "${task_state}" = "RUNNING" ]; then
      log "  ✓ ${name}: connector=${conn_state}, task=${task_state}"
    else
      log "  ✗ ${name}: connector=${conn_state}, task=${task_state}"
      # Showing error trace if task failed
      if [ "${task_state}" = "FAILED" ]; then
        local trace
        trace=$(echo "${status}" | jq -r '.tasks[0].trace // empty' | head -3)
        if [ -n "${trace}" ]; then
          echo "      Error: ${trace}" | head -c 200
          echo "..."
        fi
      fi
    fi
  done
}

restart_failed_tasks() {
  log "Restarting failed tasks ..."
  local connectors
  connectors=$(exec_in_connect "curl -s '${CONNECT_URL}/connectors'" | jq -r '.[]' 2>/dev/null)
  
  for name in ${connectors}; do
    local task_state
    task_state=$(exec_in_connect "curl -s '${CONNECT_URL}/connectors/${name}/status'" | jq -r '.tasks[0].state // "UNKNOWN"' 2>/dev/null)
    
    if [ "${task_state}" = "FAILED" ]; then
      log "Restarting task 0 for '${name}' ..."
      exec_in_connect "curl -s -X POST '${CONNECT_URL}/connectors/${name}/tasks/0/restart'" >/dev/null
    fi
  done
}

# --- Commands ---
cmd_deploy() {
  wait_for_connect
  wait_for_schema_registry
  
  for cfg in "${CONNECTOR_CONFIGS[@]}"; do
    local cfg_path="${CONFIG_DIR}/${cfg}"
    if [ -f "${cfg_path}" ]; then
      deploy_connector "${cfg_path}"
    else
      err "Config file not found: ${cfg_path}"
    fi
  done
  
  sleep 3
  show_status
}

cmd_delete() {
  wait_for_connect
  
  for cfg in "${CONNECTOR_CONFIGS[@]}"; do
    local cfg_path="${CONFIG_DIR}/${cfg}"
    if [ -f "${cfg_path}" ]; then
      local name
      name=$(get_connector_name "${cfg_path}")
      if [ -n "${name}" ]; then
        delete_connector "${name}"
      fi
    fi
  done
}

cmd_status() {
  wait_for_connect
  show_status
}

cmd_restart() {
  log "Restarting connectors (delete + deploy) ..."
  cmd_delete
  sleep 2
  cmd_deploy
}

cmd_restart_tasks() {
  wait_for_connect
  restart_failed_tasks
  sleep 2
  show_status
}

# --- Main ---
main() {
  local cmd="${1:-deploy}"
  
  case "${cmd}" in
    deploy)       cmd_deploy ;;
    delete)       cmd_delete ;;
    status)       cmd_status ;;
    restart)      cmd_restart ;;
    restart-tasks) cmd_restart_tasks ;;
    *)
      echo "Usage: $0 [deploy|delete|status|restart|restart-tasks]"
      exit 1
      ;;
  esac
}

main "$@"