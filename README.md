# vlc
Valencia air quality and weather data pipeline — a data engineering study project for IU.

## Data Flow
```
Valencia ODS API (v2.1)
         ↓ poll every 5 min
Producers (air_producer + weather_producer)
         ↓ fingerprint dedup, JSON Schema
Kafka (vlc.air / vlc.weather topics)
         ↓ JDBC Sink Connector (upsert on fiwareid+ts)
TimescaleDB (air.hyper / weather.hyper hypertables)
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
```

### 3. Start infrastructure (Kafka, TimescaleDB, Connect, Schema Registry)
```bash
docker compose -f compose/docker-compose.yml --profile infra up -d --build
```
Wait for services to be healthy (~30-60s):
```bash
docker ps --format 'table {{.Names}}\t{{.Status}}'
```

### 4. Bootstrap Kafka topics and deploy connectors
```bash
./scripts/bootstrap_kafka.sh
```
This creates topics (`vlc.air`, `vlc.weather`, Connect internal topics) and deploys JDBC sink connectors.

### 5. Start UI services (Grafana, Kafka UI, nginx proxy)
```bash
docker compose -f compose/docker-compose.yml --profile ui up -d
```

### 6. Start producers
```bash
docker compose -f compose/docker-compose.yml --profile producer up -d
```

### 7. Verify data flow
- **Kafka UI**: http://localhost:8080/kafka-ui/ (admin / your VLC_DEV_PASSWORD)
- **Grafana**: http://localhost:8080/grafana/ (admin / your VLC_DEV_PASSWORD)
- **Connect API**: `docker exec connect curl -s http://localhost:8083/connectors?expand=status | jq`
Query TimescaleDB directly:
```bash
docker exec timescaledb psql -U vlc_dev -d vlc -c "SELECT COUNT(*) FROM air.hyper;"
```

## Quick Reference

| Action 		| Command 												|
|-----------------------|-------------------------------------------------------------------------------------------------------|
| Start all 		| `docker compose -f compose/docker-compose.yml --profile infra --profile ui --profile producer up -d`  |
| Stop all 		| `docker compose -f compose/docker-compose.yml --profile infra --profile ui --profile producer down` 	|
| View logs 		| `docker compose -f compose/docker-compose.yml logs -f <service>` 					|
| Connector status 	| `./scripts/post_connectors.sh status` 								|
| Re-bootstrap Kafka 	| `./scripts/bootstrap_kafka.sh` 									|

## Project Structure

```
vlc/
├── compose/           # Docker Compose + nginx config
├── connect/           # Kafka Connect Dockerfile + sink configs
├── consumer/          # (placeholder for future consumers)
├── db/init/           # TimescaleDB init scripts (extensions, schemas, views)
├── grafana/           # Grafana provisioning
├── producer/          # air + weather Python producers
│   └── schemas/       # JSON schemas for validation
├── scripts/           # bootstrap, secrets, analysis utilities
├── schemas/           # (placeholder for future Avro schemas)
└── tests/             # pytest test suite
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
