#!/usr/bin/env bash
set -euo pipefail

# Load TS_* variables from secrets.properties
while IFS='=' read -r key value; do
  case "$key" in
    TS_USERNAME) TS_USERNAME="$value" ;;
    TS_PASSWORD) TS_PASSWORD="$value" ;;
  esac
done < connect/secrets/secrets.properties

echo "Setting DB password for role '$TS_USERNAME'..."

docker compose -f /opt/vlc/compose/docker-compose.yml exec -T timescaledb \
  psql -U postgres -d vlc \
  -c "ALTER ROLE ${TS_USERNAME} PASSWORD '${TS_PASSWORD}';"
