#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

# Load .env
set -a
. .env
set +a

# 1) Nginx Basic Auth
printf "admin:$(openssl passwd -apr1 "$VLC_DEV_PASSWORD")\n" \
  > compose/.htpasswd

# 2) Kafka Connect secrets
mkdir -p connect/secrets
cat > connect/secrets/secrets.properties <<EOF
TS_JDBC_URL=jdbc:postgresql://timescaledb:5432/vlc
TS_USERNAME=vlc_dev
TS_PASSWORD=${VLC_DEV_PASSWORD}
EOF

# 3) DB override script (gitignored)
mkdir -p db/init
cat > db/init/020-vlc-password.override.sql <<EOF
ALTER ROLE vlc_dev WITH PASSWORD '${VLC_DEV_PASSWORD}';
EOF

