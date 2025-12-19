#!/usr/bin/env bash
# Installs the VLC systemd service with paths resolved from current repo location.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VLC_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
SERVICE_TEMPLATE="${SCRIPT_DIR}/vlc.service"
SERVICE_NAME="vlc.service"
SYSTEMD_DIR="/etc/systemd/system"

echo "[install-service] Detected VLC root: ${VLC_ROOT}"

# Checking if running as root
if [[ $EUID -ne 0 ]]; then
    echo "[install-service] This script must be run as root (use sudo)"
    exit 1
fi

# Checking template exists
if [[ ! -f "${SERVICE_TEMPLATE}" ]]; then
    echo "[install-service] Error: Template not found at ${SERVICE_TEMPLATE}"
    exit 1
fi

# Generating service file with actual paths
echo "[install-service] Generating service file..."
sed "s|{{VLC_ROOT}}|${VLC_ROOT}|g" "${SERVICE_TEMPLATE}" > "${SYSTEMD_DIR}/${SERVICE_NAME}"

# Reloading systemd and enabling service
echo "[install-service] Reloading systemd daemon..."
systemctl daemon-reload

echo "[install-service] Enabling ${SERVICE_NAME}..."
systemctl enable "${SERVICE_NAME}"

echo "[install-service] Service installed successfully!"
echo "[install-service] Start with: sudo systemctl start ${SERVICE_NAME}"
echo "[install-service] Check status: systemctl status ${SERVICE_NAME}"
