#!/usr/bin/env bash
# Stops the VLC data pipeline stack.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_DIR="${SCRIPT_DIR}/../compose"

cd "${COMPOSE_DIR}"

# Matching default from vlc-start.sh
PROFILES="${VLC_PROFILES:-infra,schema,producer,ui}"

PROFILE_FLAGS=""
IFS=',' read -ra PROFILE_ARRAY <<< "${PROFILES}"
for p in "${PROFILE_ARRAY[@]}"; do
    PROFILE_FLAGS+="--profile ${p} "
done

echo "[vlc-stop] Stopping VLC stack..."
docker compose ${PROFILE_FLAGS} down

echo "[vlc-stop] Stack stopped"
