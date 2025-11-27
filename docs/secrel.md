# secrel.md — Security & reliability baseline for **VLC**

> A tiny baseline that encodes essentials: network segmentation, invariants as checks, and solid release habits.

---

## 1) System invariants ✅

- [✅] **No direct DB exposure.** Timescale/PostGIS is reachable only from internal services; no host‑published ports.
- [✅] **Auth in front of UIs.** Grafana/Kafka‑UI sit behind a reverse proxy with auth and rate‑limits.
- [✅] **Idempotent writes.** Upserts keyed by `(fiwareid, ts)`; replays never duplicate.
- [✅] **Backoff instead of crash.** Python producer sheds load and backs off on API slowness or DB pressure.
- [✅] **Config is versioned.** Connector JSON and producer offsets are tracked in VCS with rollbacks.
- [ ] **Keys rotate without drama.** Credentials can rotate in-place with a documented two‑step cutover.
- [ ] **Restore is proven.** Nightly backups exist and are periodically test‑restored.

---

## 2) Compose: two networks, no DB exposure, UIs behind proxy ✅

> Overlay for `compose/docker-compose.yml`.

```yaml
networks:
  vlc_internal:
    driver: bridge
    internal: true
  vlc_frontend:
    driver: bridge

services:

  kafka:
    image: confluentinc/cp-kafka:7.6.1
    networks: [vlc_internal]
    ports: []  # no host ports; we use kafka-ui via proxy for visibility
    environment:
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092

  schema-registry:
    image: confluentinc/cp-schema-registry:7.6.1
    depends_on: [kafka]
    networks: [vlc_internal]
    environment:
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: PLAINTEXT://kafka:9092

  connect:
    image: confluentinc/cp-kafka-connect:7.6.1
    depends_on: [kafka, schema-registry]
    networks: [vlc_internal]
    environment:
      CONNECT_BOOTSTRAP_SERVERS: kafka:9092
      CONNECT_REST_ADVERTISED_HOST_NAME: connect
      CONNECT_GROUP_ID: vlc-connect
      CONNECT_CONFIG_STORAGE_TOPIC: _connect-configs
      CONNECT_OFFSET_STORAGE_TOPIC: _connect-offsets
      CONNECT_STATUS_STORAGE_TOPIC: _connect-status
      CONNECT_KEY_CONVERTER: org.apache.kafka.connect.storage.StringConverter
      CONNECT_VALUE_CONVERTER: org.apache.kafka.connect.json.JsonConverter
      CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE: "false"
      CONNECT_CONFIG_PROVIDERS: file
      CONNECT_CONFIG_PROVIDERS_FILE_CLASS: org.apache.kafka.common.config.provider.FileConfigProvider
      # Mount secrets at /opt/kafka/connect/secrets and reference with ${file:/opt/kafka/connect/secrets/secrets.properties:KEY}
    volumes:
      - ./connect/secrets:/opt/kafka/connect/secrets:ro

  timescaledb:
    image: timescale/timescaledb-ha:pg16-latest
    networks: [vlc_internal]
    environment:
      POSTGRES_DB: vlc
      POSTGRES_USER: vlc
      POSTGRES_PASSWORD_FILE: /run/secrets/ts_password
    secrets:
      - ts_password
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U vlc -d vlc"]
      interval: 10s
      timeout: 5s
      retries: 6

  sidecar-producer:
    build: ./producer
    depends_on: [kafka, timescaledb]
    networks: [vlc_internal]
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka:9092
      VLC_DATASET_ID: estacions-atmosferiques-estaciones-atmosfericas
      VLC_TOPIC: valencia.air.canary
      VLC_OFFSET_PATH: /state/offset.txt
      VLC_MAX_INFLIGHT_POLLS: "3"
      VLC_BACKOFF_BASE_SECONDS: "5"
      VLC_BACKOFF_MAX_SECONDS: "120"
    volumes:
      - ./producer/state:/state
    restart: on-failure

  grafana:
    image: grafana/grafana:11.2.0
    networks: [vlc_internal, vlc_frontend]
    # NO host port here; expose only via reverse_proxy below

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    networks: [vlc_internal, vlc_frontend]
    environment:
      KAFKA_CLUSTERS_0_NAME: vlc
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9092

  reverse_proxy:
    image: nginx:1.27-alpine
    depends_on: [grafana, kafka-ui]
    networks: [vlc_frontend]
    ports:
      - "8080:8080"  # single entry point; bind to localhost in dev as an alternative
    volumes:
      - ./compose/nginx.conf:/etc/nginx/nginx.conf:ro

secrets:
  ts_password:
    file: ./compose/secrets/ts_password.txt
```

### `compose/nginx.conf` (auth + rate limits + IP allow‑list stub)

```nginx
worker_processes 1;
events { worker_connections 1024; }
http {
  limit_req_zone $binary_remote_addr zone=basic:10m rate=5r/s;
  server {
    listen 8080;
    # Allow only these subnets (!!!EDIT TO SPECIFICS!!!); deny all else
    allow 10.0.0.0/8; allow 172.16.0.0/12; allow 192.168.0.0/16; deny all;

    # Basic auth (create /etc/nginx/.htpasswd with htpasswd)
    auth_basic "VLC";
    auth_basic_user_file /etc/nginx/.htpasswd;

    location /grafana/ {
      proxy_pass http://grafana:3000/;
      limit_req zone=basic burst=20 nodelay;
    }
    location /kafka-ui/ {
      proxy_pass http://kafka-ui:8080/;
      limit_req zone=basic burst=20 nodelay;
    }
  }
}
```

---

## 3) Python producer backoff & shed contract  ✅

- Use **exponential backoff with jitter** on HTTP 429/5xx and timeouts.
- Cap concurrent polls via `VLC_MAX_INFLIGHT_POLLS`.
- Detect DB pressure (slow inserts, connection pool saturation) → **reduce produce rate**.
- Never drop data silently; surface DLQ or on-disk queue for retries when Kafka is down.

---

## 4) CI checks ✅

- Lint + unit tests for Python producers (schema changes, geo flattening, idempotent upserts).
- JSON schema validation for `connect/config/*.json`.

---

## 5) Notes & conventions ✅

- Topic names: `vlc.<domain>`
- Tables: `<domain>.hyper` where domain ∈ `{air, weather}`
- Primary key: `(fiwareid, ts)`; `ts` is UTC.
- Keep **`.env.example`** with non‑secret placeholders; commit it. Real secrets live in .env.

---

## 6) Config & producer versioning ✅

- For config rollbacks, we can use standard git operations.
- Each producer writes to /state/state.json inside its container, with separate volumes to prevent cross-contamination.
> For offset rollbacks, we need to: 1. Reset the state file in the volume, or 2. Use PG_BOOTSTRAP=true to re-bootstrap from TimescaleDB's max(ts)

## 7) Backups & restore drill (STILL PONDERING)

> Nightly dump; periodic restore proof.

```bash
# Backup (container shell or psql client from host)
pg_dump -h timescaledb -U vlc -d vlc -F c -f /backups/vlc_$(date +%F).dump
# Restore (wipe test DB, then)
pg_restore -h timescaledb -U vlc -d vlc --clean --if-exists /backups/vlc_YYYY-MM-DD.dump
```

Plan: Add a weekly GitHub Action to copy `/backups/*.dump` to durable storage.

---
*End. Keep current file short; treat it as a living entity, a baseline.

