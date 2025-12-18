#!/usr/bin/env bash
# Detects the environment and installs the appropriate autostart mechanism.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INSTALL_PATH="${VLC_INSTALL_PATH:-/opt/vlc}"

print_usage() {
    echo "Usage: $0 [--install-path /path/to/vlc] [--dry-run]"
    echo ""
    echo "Options:"
    echo "  --install-path PATH   Where vlc repo is/will be installed (default: /opt/vlc)"
    echo "  --dry-run             Show what would be done without making changes"
}

DRY_RUN=false
while [[ $# -gt 0 ]]; do
    case $1 in
        --install-path) INSTALL_PATH="$2"; shift 2 ;;
        --dry-run) DRY_RUN=true; shift ;;
        -h|--help) print_usage; exit 0 ;;
        *) echo "Unknown option: $1"; print_usage; exit 1 ;;
    esac
done

run_cmd() {
    if [[ "${DRY_RUN}" == "true" ]]; then
        echo "[dry-run] $*"
    else
        "$@"
    fi
}

detect_environment() {
    if grep -qi microsoft /proc/version 2>/dev/null; then
        # Checking if systemd is enabled in WSL
        if systemctl --version &>/dev/null 2>&1 && [[ -d /run/systemd/system ]]; then
            echo "wsl-systemd"
        else
            echo "wsl"
        fi
    elif command -v systemctl &>/dev/null && [[ -d /run/systemd/system ]]; then
        echo "systemd"
    elif [[ "$(uname)" == "Darwin" ]]; then
        echo "macos"
    else
        echo "unknown"
    fi
}

install_systemd() {
    echo "[install] Installing systemd service..."
    
    # Updating paths in service file
    local service_content
    service_content=$(sed "s|/opt/vlc|${INSTALL_PATH}|g" "${SCRIPT_DIR}/autostart/vlc.service")
    
    if [[ "${DRY_RUN}" == "true" ]]; then
        echo "[dry-run] Would write to /etc/systemd/system/vlc.service:"
        echo "${service_content}"
        echo "[dry-run] Would run: systemctl daemon-reload"
        echo "[dry-run] Would run: systemctl enable vlc.service"
    else
        echo "${service_content}" | sudo tee /etc/systemd/system/vlc.service > /dev/null
        sudo systemctl daemon-reload
        sudo systemctl enable vlc.service
        echo "[install] Service installed and enabled"
        echo "[install] Start now with: sudo systemctl start vlc"
        echo "[install] Check status with: sudo systemctl status vlc"
    fi
}

install_wsl() {
    echo "[install] WSL detected (without systemd)"
    echo ""
    echo "Add the following to /etc/wsl.conf:"
    echo ""
    cat "${SCRIPT_DIR}/autostart/wsl-boot.conf" | sed "s|/opt/vlc|${INSTALL_PATH}|g"
    echo ""
    echo "Then restart WSL with: wsl --shutdown"
    echo ""
    if [[ "${DRY_RUN}" != "true" ]]; then
        read -p "Append to /etc/wsl.conf now? [y/N] " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            local boot_cmd="${INSTALL_PATH}/scripts/vlc-start.sh"
            if grep -q "^\[boot\]" /etc/wsl.conf 2>/dev/null; then
                echo "[install] [boot] section exists - please manually add:"
                echo "command=${boot_cmd}"
            else
                echo "" | sudo tee -a /etc/wsl.conf > /dev/null
                echo "[boot]" | sudo tee -a /etc/wsl.conf > /dev/null
                echo "command=${boot_cmd}" | sudo tee -a /etc/wsl.conf > /dev/null
                echo "[install] Added boot command to /etc/wsl.conf"
            fi
        fi
    fi
}

install_wsl_systemd() {
    echo "[install] WSL with systemd detected"
    install_systemd
}

# Main
ENV=$(detect_environment)
echo "[detect] Environment: ${ENV}"
echo "[detect] Install path: ${INSTALL_PATH}"
echo ""

# Ensuring scripts are executable
run_cmd chmod +x "${SCRIPT_DIR}/vlc-start.sh"
run_cmd chmod +x "${SCRIPT_DIR}/vlc-stop.sh"

case "${ENV}" in
    systemd)
        install_systemd
        ;;
    wsl-systemd)
        install_wsl_systemd
        ;;
    wsl)
        install_wsl
        ;;
    macos)
        echo "[install] macOS detected - launchd support not yet implemented"
        echo "[install] For now, use 'Login Items' in System Settings or create a launchd plist manually"
        ;;
    *)
        echo "[install] Unknown environment - manual setup required"
        echo "[install] Run ${INSTALL_PATH}/scripts/vlc-start.sh on boot"
        exit 1
        ;;
esac
