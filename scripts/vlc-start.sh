#!/usr/bin/env bash
# Starts the VLC data pipeline stack.
# Portable across WSL, native Linux, etc.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_DIR="${SCRIPT_DIR}/../compose"

cd "${COMPOSE_DIR}"

# Default profiles - override with VLC_PROFILES env var if needed
# Lean-stack (small VMs): infra,producer,alt-sink
# Full-stack: infra,schema,producer,ui
PROFILES="${VLC_PROFILES:-infra,schema,producer,ui}"

# Building profile flags
PROFILE_FLAGS=""
IFS=',' read -ra PROFILE_ARRAY <<< "${PROFILES}"
for p in "${PROFILE_ARRAY[@]}"; do
    PROFILE_FLAGS+="--profile ${p} "
done

echo "[vlc-start] Starting VLC stack with profiles: ${PROFILES}"
docker compose ${PROFILE_FLAGS} up -d

echo "[vlc-start] Stack started successfully"
