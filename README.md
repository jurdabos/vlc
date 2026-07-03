# vlc/README.md
VLC air quality and weather data pipeline — a data engineering study project for IU.

## Data Flow
```
Valencia geoportal ArcGIS REST  (MedioAmbiente/MapServer/156, /157)
         ↓ poll every 5 min
Producers (air_producer + weather_producer)
         ↓ per-station offset + fingerprint dedup, Avro serialization
Kafka (vlc.air / vlc.weather topics)
         ↓ JDBC Sink Connector (Avro, upsert on (fiwareid, ts))
TimescaleDB (air.hyper / weather.hyper hypertables)
         ↓ SQL queries
Grafana Dashboards
```
The upstream source migrated from `valencia.opendatasoft.com` (Opendatasoft
Explore v2.1, decommissioned 2026-05) to the ArcGIS REST layers `156` (air
pollution) and `157` (weather) on `geoportal.valencia.es`. Field names and
station identifiers are unchanged — see `producer/README.md` for the full
details.

## Prerequisites
- Docker & Docker Compose v2
- Bash (native on Linux/macOS, WSL on Windows)
- `jq` (for bootstrap scripts)
- `openssl` (for htpasswd generation)

## Local Setup
### 1. Clone and configure environment
```
git clone https://github.com/jurdabos/vlc.git && cd vlc
cp .env.example .env
# Edit .env and set VLC_DEV_PASSWORD to a strong password
# Symlink .env into compose/ so Docker Compose can read it
ln -sf ../.env compose/.env
```

### 2. Generate derived secrets
This creates `.htpasswd`, JDBC credentials, and DB password files:
```
chmod +x scripts/*.sh
./scripts/sync-dev-secrets.sh
chmod 644 compose/.htpasswd
```

### 3. Start infrastructure (Kafka, TimescaleDB)
```
docker compose -f compose/docker-compose.yml --profile infra up -d --build
```
Wait for services to be healthy (~30-60s):
```
docker ps --format 'table {{.Names}}\t{{.Status}}'
```

### 4. Start Schema Registry and Kafka Connect
```
docker compose -f compose/docker-compose.yml --profile infra --profile schema up -d --build
```

### 5. Bootstrap Kafka topics and deploy connectors
```
./scripts/bootstrap_kafka.sh 2>&1 | grep -v "WARNING.*metric names"
```
This creates topics (`vlc.air`, `vlc.weather`, Connect internal topics) and deploys JDBC sink connectors.

### 6. Start UI services (Grafana, Kafka UI, nginx proxy) and producers
```
docker compose -f compose/docker-compose.yml --profile infra --profile schema --profile ui --profile producer up -d --build
```

## Alternative: Python Sinks (alt-sink profile)
Instead of Kafka Connect, you can use Python-based sink consumers for simpler deployments.

### 1-3. Same as above (clone, secrets, infra)

### 4. Bootstrap Kafka (skip Connect)
```
./scripts/bootstrap_kafka.sh --skip-connect 2>&1 | grep -v "WARNING.*metric names"
```
This creates only the data topics (`vlc.air`, `vlc.weather`) without Connect internal topics.

### 5. Start producers, and alt-sink consumers
```
docker compose -f compose/docker-compose.yml --profile infra --profile producer --profile alt-sink up -d --build
```

### 6. Optionally start UI services (Grafana, Kafka UI, nginx proxy) – be careful as these might rely on info from Connect JVM
```
docker compose -f compose/docker-compose.yml --profile infra --profile ui --profile producer --profile alt-sink up -d --build
```

### 7. Verify data flow
- **Kafka UI**: http://localhost:8080/kafka-ui/ (admin / VLC_DEV_PASSWORD)
- **Grafana**: http://localhost:8080/grafana/ (admin / VLC_DEV_PASSWORD)
- **Connect API**:
```
docker compose -f compose/docker-compose.yml exec connect curl -s http://localhost:8083/connectors?expand=status | jq
```
Query TimescaleDB directly:
```
docker compose -f compose/docker-compose.yml exec timescaledb psql -U vlc_dev -d vlc -c "SELECT COUNT(*) FROM air.hyper;"
```
Producers behaving:
```
docker compose -f compose/docker-compose.yml --profile infra --profile producer logs air-producer --tail=50
docker compose -f compose/docker-compose.yml --profile infra --profile producer logs weather-producer --tail=50
```
Optional Connect sinks behaving:
```
   docker compose -f compose/docker-compose.yml --profile infra --profile schema logs connect 2>&1 | grep -i 'jdbc-sink'
```
```
docker compose -f compose/docker-compose.yml exec -T timescaledb psql -U vlc_dev -d vlc -c "SELECT max(ts) FROM air.hyper;"
docker compose -f compose/docker-compose.yml exec -T timescaledb psql -U vlc_dev -d vlc -c "SELECT max(ts) FROM weather.hyper;"
---
If `max(ts)` is recent, the pipeline has been happily doing its thing recently.

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

## CLI
The repo ships a `vlc` command-line interface (standard acidvuca layout:
`src/vlc/cli.py` + `[project.scripts]`, reusing shared tooling from
[acidbase](https://github.com/jurdabos/acidbase)). Set up with `uv sync`, then:
```bash
uv run vlc --help
# Canonical commit-and-push workflow (stages, commits with pre-commit retry, pushes to origin)
uv run vlc push -m "feat: describe the change"
uv run vlc push --dry-run   # preview without changing anything
```
Data commands read exclusively from the dbt-managed views/marts, so the CLI,
the Grafana dashboards, and the alert rules share a single source of truth:
```bash
uv run vlc status           # per-station freshness (public.data_freshness)
uv run vlc latest           # latest weather readings (weather.latest)
uv run vlc latest --air     # latest air quality readings (air.latest)
uv run vlc latest --all     # combined snapshot (public.station_snapshot)
uv run vlc stations         # station inventory (public.stations)
uv run vlc grafana check    # CI guardrail: no derived SQL logic in dashboards
```
All data commands accept `--csv` for piping. By default they reach
TimescaleDB through `docker compose exec timescaledb psql` (the DB publishes
no ports); set `VLC_DSN` (e.g. `postgresql://vlc_dev:REAL-PASSWORD@localhost:5432/vlc`)
to use a direct connection instead.

The SSOT contract: derived query logic (latest-per-station, staleness math,
snapshot joins) lives once, in `dbt/models/`; Grafana panels, alert rules,
and `src/vlc/queries.py` only SELECT from the resulting views. `vlc grafana
check` (run in CI) fails when a dashboard or alert re-derives such logic
against the raw hypertables. Freshness thresholds are dbt vars in
`dbt/dbt_project.yml` (`freshness_offline_max: "5 hours"` matches the
WeatherStationStale alert threshold).

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
├── backfill/           # Historical data loading scripts
├── compose/            # Docker Compose + nginx config
├── connect/            # Kafka Connect Dockerfile + sink configs
├── consumer/           # Python sink consumers (alt-sink profile)
├── db/init/            # TimescaleDB init scripts (extensions, schemas, views)
├── docs/               # Project documentation, SQL queries, design references
├── grafana/            # Grafana provisioning and dashboards
├── infra/              # Infrastructure setup for Azure deployment
├── monitoring/         # Prometheus, Alertmanager, JMX exporter configs
├── producer/           # air + weather Python producers
├── schemas/            # Avro schemas (.avsc) for air + weather
├── scripts/            # bootstrap, secrets, analysis utilities
├── src/vlc/            # vlc CLI package (click group; push from acidbase)
└── tests/              # pytest test suite
```

## Azure Deployment
1. Deploy VM via Bicep: `az deployment group create --resource-group vlc-rg --template-file infra/main.bicep ...`
2. SSH tunnel for UIs: `ssh -L 8080:localhost:8080 azureuser@<VM_IP>`
3. Copy `.env` to VM: `scp .env azureuser@<VM_IP>:/opt/vlc/`

## Monitoring
- Prometheus scrapes JMX exporters on Kafka, Connect, Schema Registry
- Alertmanager handles routing for critical alerts (offline partitions, failed tasks)
- Grafana dashboards in `grafana/provisioning/dashboards/`

## Schema Serialization
Producers use **Avro** serialization with Schema Registry. Schemas are defined in `schemas/*.avsc`:
- `schemas/air.avsc` — Air quality measurements
- `schemas/weather.avsc` — Weather station readings

Timestamps use Avro `timestamp-millis` logical type (epoch milliseconds).
