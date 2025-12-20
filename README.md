# vlc/README.md
VLC air quality and weather data pipeline — a data engineering study project for IU.

## Data Flow
```
Valencia ODS API (v2.1)
         ↓ poll every 5 min
Producers (air_producer + weather_producer)
         ↓ fingerprint dedup, JSON Schema
Kafka (vlc.air/vlc.weather topics)
         ↓ JDBC Sink Connector (upsert on fiwareid+ts)
TimescaleDB (air.hyper/weather.hyper hypertables)
         ↓ SQL queries
Grafana Dashboards
```

## Prerequisites
- Docker & Docker Compose v2
- Bash (native on Linux/macOS, WSL on Windows)
- `jq` (for bootstrap scripts)
- `openssl` (for htpasswd generation)

## Local Setup
### 1. Clone and configure environment
```bash
git clone https://github.com/jurdabos/vlc.git && cd vlc
cp .env.example .env
# Edit .env and set VLC_DEV_PASSWORD to a strong password
# Symlink .env into compose/ so Docker Compose can read it
ln -sf ../.env compose/.env
```

### 2. Generate derived secrets
This creates `.htpasswd`, JDBC credentials, and DB password files:
```bash
chmod +x scripts/*.sh
./scripts/sync-dev-secrets.sh
chmod 644 compose/.htpasswd
```

### 3. Start infrastructure (Kafka, TimescaleDB)
```bash
docker compose -f compose/docker-compose.yml --profile infra up -d --build
```
Wait for services to be healthy (~30-60s):
```bash
docker ps --format 'table {{.Names}}\t{{.Status}}'
```

### 4. Start Schema Registry and Kafka Connect
```bash
docker compose -f compose/docker-compose.yml --profile infra --profile schema up -d --build
```

### 5. Bootstrap Kafka topics and deploy connectors
```bash
./scripts/bootstrap_kafka.sh 2>&1 | grep -v "WARNING.*metric names"
```
This creates topics (`vlc.air`, `vlc.weather`, Connect internal topics) and deploys JDBC sink connectors.

### 6. Start UI services (Grafana, Kafka UI, nginx proxy) and producers
```bash
docker compose -f compose/docker-compose.yml --profile infra --profile schema --profile ui --profile producer up -d --build
```

## Alternative: Python Sinks (alt-sink profile)
Instead of Kafka Connect, you can use Python-based sink consumers for simpler deployments.

### 1-3. Same as above (clone, secrets, infra)

### 4. Bootstrap Kafka (skip Connect)
```bash
./scripts/bootstrap_kafka.sh --skip-connect 2>&1 | grep -v "WARNING.*metric names"
```
This creates only the data topics (`vlc.air`, `vlc.weather`) without Connect internal topics.

### 5. Start producers, and alt-sink consumers
```bash
docker compose -f compose/docker-compose.yml --profile infra --profile producer --profile alt-sink up -d --build
```

### 6. Optionally start UI services (Grafana, Kafka UI, nginx proxy) – be careful as these might rely on info from Connect JVM
```bash
docker compose -f compose/docker-compose.yml --profile infra --profile ui --profile producer --profile alt-sink up -d --build
```

### 7. Verify data flow
- **Kafka UI**: http://localhost:8080/kafka-ui/ (admin / VLC_DEV_PASSWORD)
- **Grafana**: http://localhost:8080/grafana/ (admin / VLC_DEV_PASSWORD)
- **Connect API**:
```bash
docker compose -f compose/docker-compose.yml exec connect curl -s http://localhost:8083/connectors?expand=status | jq
```
Query TimescaleDB directly:
```bash
docker compose -f compose/docker-compose.yml exec timescaledb psql -U vlc_dev -d vlc -c "SELECT COUNT(*) FROM air.hyper;"
```

## Historical Backfill (Optional)
Load historical RVVCCA data into TimescaleDB before starting the streaming producers.

Historical CSVs are included in `backfill/`:
- `hourly_2021_2022.csv` — hourly readings (air + weather)
- `daily_2004_2022.csv` — daily aggregates
```bash
# Copying CSV into the container and running the backfill script
docker compose -f compose/docker-compose.yml cp backfill/hourly_2021_2022.csv timescaledb:/tmp/
docker compose -f compose/docker-compose.yml exec timescaledb psql -U vlc_dev -d vlc -f /tmp/backfill.sql
```
The script:
- Maps historical station names to current `fiwareid` values
- Loads both air quality and weather data from the same CSV
- Uses `ON CONFLICT DO NOTHING` so re-runs are safe and streaming data won't duplicate

## Quick Reference

| Action | Command |
|--------|----------|
| Start all (Connect) | `docker compose -f compose/docker-compose.yml --profile infra --profile schema --profile ui --profile producer up -d` |
| Start all (alt-sink) | `docker compose -f compose/docker-compose.yml --profile infra --profile ui --profile producer --profile alt-sink up -d` |
| Stop all | `docker compose -f compose/docker-compose.yml down` |
| View logs | `docker compose -f compose/docker-compose.yml logs -f <service>` |
| Connector status | `./scripts/post_connectors.sh status` |
| Bootstrap (Connect) | `./scripts/bootstrap_kafka.sh` |
| Bootstrap (alt-sink) | `./scripts/bootstrap_kafka.sh --skip-connect` |

## Docker Compose Profiles

| Profile | Services |
|---------|----------|
| `infra` | kafka, timescaledb |
| `schema` | schema-registry, connect |
| `ui` | grafana, prometheus, kafka-ui, reverse-proxy, alertmanager |
| `producer` | air-producer, weather-producer |
| `alt-sink` | air-sink, weather-sink (alternative to Kafka Connect) |

## Project Structure

```
vlc/
├── backfill/				# Historical data loading scripts
├── compose/				# Docker Compose + nginx config
├── connect/				# Kafka Connect Dockerfile + sink configs
├── consumer/				# (placeholder for future consumers)
├── db/init/				# TimescaleDB init scripts (extensions, schemas, views)
├── docs/
├── grafana/				# Grafana provisioning
├── infra/				# infrastructure setup for Azure deployment
├── jmx-exporter/
├── monitoring/
├── producer/				# air + weather Python producers
│   └── schemas/			# JSON schemas for validation
├── schemas/				# (placeholder for future Avro schemas)
├── scripts/				# bootstrap, secrets, analysis utilities
└── tests/				# pytest test suite
```

## Azure Deployment
1. Deploy VM via Bicep: `az deployment group create --resource-group vlc-rg --template-file infra/main.bicep ...`
2. SSH tunnel for UIs: `ssh -L 8080:localhost:8080 azureuser@<VM_IP>`
3. Copy `.env` to VM: `scp .env azureuser@<VM_IP>:/opt/vlc/`

## Monitoring
- Prometheus scrapes JMX exporters on Kafka, Connect, Schema Registry
- Alertmanager handles routing for critical alerts (offline partitions, failed tasks)
- Grafana dashboards in `grafana/provisioning/dashboards/`

## Schema Validation
Validate JSON data against air/weather schemas:
```bash
uv run python scripts/validate_schema.py -t air data.json
echo '{"fiwareid": "test", "ts": "2024-01-01T00:00:00Z"}' | uv run python scripts/validate_schema.py -t air
```
