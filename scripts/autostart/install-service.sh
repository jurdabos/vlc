#!/usr/bin/env bash
# Installs VLC autostart configuration with paths resolved from current repo location.
# Supports both systemd (native Linux) and WSL boot command.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VLC_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

echo "[install-service] Detected VLC root: ${VLC_ROOT}"

# Checking if running as root
if [[ $EUID -ne 0 ]]; then
    echo "[install-service] This script must be run as root (use sudo)"
    exit 1
fi

# Detecting environment
if grep -qi microsoft /proc/version 2>/dev/null; then
    IS_WSL=true
    echo "[install-service] Detected WSL environment"
else
    IS_WSL=false
    echo "[install-service] Detected native Linux environment"
fi

# Installing systemd service (works in both WSL with systemd and native Linux)
install_systemd_service() {
    local SERVICE_TEMPLATE="${SCRIPT_DIR}/vlc.service"
    local SERVICE_NAME="vlc.service"
    local SYSTEMD_DIR="/etc/systemd/system"

    if [[ ! -f "${SERVICE_TEMPLATE}" ]]; then
        echo "[install-service] Error: Template not found at ${SERVICE_TEMPLATE}"
        return 1
    fi

    echo "[install-service] Generating systemd service file..."
    sed "s|{{VLC_ROOT}}|${VLC_ROOT}|g" "${SERVICE_TEMPLATE}" > "${SYSTEMD_DIR}/${SERVICE_NAME}"

    echo "[install-service] Reloading systemd daemon..."
    systemctl daemon-reload

    echo "[install-service] Enabling ${SERVICE_NAME}..."
    systemctl enable "${SERVICE_NAME}"

    echo "[install-service] Systemd service installed!"
}

# Configuring WSL boot command
install_wsl_boot() {
    local WSL_BOOT_TEMPLATE="${SCRIPT_DIR}/wsl-boot.conf"
    local WSL_CONF="/etc/wsl.conf"

    if [[ ! -f "${WSL_BOOT_TEMPLATE}" ]]; then
        echo "[install-service] Warning: WSL boot template not found, skipping"
        return 0
    fi

    local BOOT_CMD="${VLC_ROOT}/scripts/vlc-start.sh"

    # Checking if wsl.conf exists and has a [boot] section
    if [[ -f "${WSL_CONF}" ]] && grep -q "^\[boot\]" "${WSL_CONF}"; then
        # Updating existing boot command or adding if not present
        if grep -q "^command=" "${WSL_CONF}"; then
            echo "[install-service] Updating existing boot command in ${WSL_CONF}..."
            sed -i "s|^command=.*|command=${BOOT_CMD}|" "${WSL_CONF}"
        else
            echo "[install-service] Adding boot command to existing [boot] section..."
            sed -i "/^\[boot\]/a command=${BOOT_CMD}" "${WSL_CONF}"
        fi
    else
        # Appending [boot] section
        echo "[install-service] Adding [boot] section to ${WSL_CONF}..."
        echo "" >> "${WSL_CONF}"
        echo "[boot]" >> "${WSL_CONF}"
        echo "command=${BOOT_CMD}" >> "${WSL_CONF}"
    fi

    echo "[install-service] WSL boot command configured!"
    echo "[install-service] Note: Restart WSL (wsl --shutdown) for changes to take effect"
}

# Installing based on environment
if [[ "${IS_WSL}" == true ]]; then
    # WSL: prefer boot command, but also install systemd if available
    install_wsl_boot
    if systemctl --version &>/dev/null; then
        echo "[install-service] Systemd detected in WSL, installing service as well..."
        install_systemd_service
    fi
else
    # Native Linux: systemd only
    install_systemd_service
fi

echo ""
echo "[install-service] Installation complete!"
echo "[install-service] Start manually: sudo systemctl start vlc.service"
echo "[install-service] Check status: systemctl status vlc.service"
