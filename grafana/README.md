# Grafana Configuration
Grafana dashboards and provisioning configuration for Valencia air quality and weather monitoring.

## Access
**URL:** http://localhost:8080/grafana/

Two layers of authentication:
1. Nginx HTTP Basic Auth (browser prompt)
2. Grafana login (admin / VLC_DEV_PASSWORD)

To update HTTP Basic Auth password:
```bash
cd /opt/vlc/compose
echo "admin:$(openssl passwd -apr1 YOUR_NEW_PASSWORD)" > .htpasswd
docker compose -f docker-compose.yml exec reverse-proxy nginx -s reload
```

## Directory Structure
```
grafana/
├── dashboards/
│   ├── air_quality.json       # Air quality dashboard with spatial queries
│   ├── weather.json           # Weather dashboard with meteorological metrics
│   └── system_metrics.json    # System metrics dashboard
└── provisioning/
    ├── datasources/
    │   ├── alertmanager.yml
    │   ├── timescaledb.yml    # TimescaleDB PostgreSQL datasource
    │   └── prometheus.yml
    └── dashboards/
        └── dashboards.yml     # Dashboard provider config
```

## Datasources
### TimescaleDB (PostgreSQL)
Grafana's native Postgres data source works with TimescaleDB out of the box.
- Database: `vlc`
- Schemas: `air.hyper` / `air.daily`, `weather.hyper` / `weather.daily`

### Prometheus
Metrics from Kafka, Connect, Schema Registry via JMX exporters.

### Alertmanager
Alert routing and notification status.

## Dashboards
### Valencia Air Quality
- Total row count in `air.hyper`
- Spatial query: stations within configurable radius
- Pollutant levels for all stations (last 7 days)

### Valencia Weather
- Total row count in `weather.hyper`
- Temperature trends across stations (last 24h)
- Latest weather readings by station
- Spatial query: stations within configurable radius
- Humidity and pressure charts (last 7 days)

### System Metrics
- Service status: Connect, Schema Registry, Prometheus
- Kafka Connect: Active connectors, task batch sizes, put batch times, record failures
- Schema Registry: Schema/subject counts, HTTP request rates and latencies (p99)

**Note:** Kafka broker metrics require JMX exporter on kafka service (planned for future phases).

## Starting Grafana
```bash
docker compose -f compose/docker-compose.yml --profile ui up -d grafana
```

## Notes
- Dashboards auto-refresh every 30 seconds
- Provisioned datasources and dashboards are loaded automatically on startup
- Dashboard changes in the UI are persisted (allowUiUpdates: true)
- Both air and weather use PostGIS geometry column (`geo`) for spatial queries

See `docs/sql_queries.md` for example SQL checks against TimescaleDB.
