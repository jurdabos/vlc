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
- Latest weather readings by station, including a color-coded `age` column
  (teal = fresh, light green = > 30 min, rust = > 5 h) so a single dead
  station is visible at a glance
- Spatial query: stations within configurable radius
- Humidity and pressure charts (last 7 days)

### System Metrics
- Service status: Connect, Schema Registry, Prometheus
- Kafka Connect: Active connectors, task batch sizes, put batch times, record failures
- Schema Registry: Schema/subject counts, HTTP request rates and latencies (p99)

**Note:** Kafka broker metrics require JMX exporter on kafka service (planned for future phases).

## Single source of truth
Derived query logic (latest-per-station, staleness math, snapshot joins,
station inventory) lives once, in the dbt models (`dbt/models/`), and is
materialized as views (`weather.latest`, `air.latest`, `public.station_snapshot`,
`public.data_freshness`, `public.stations`). Dashboard panels and alert rules
only SELECT from those views; plain projections/counts against the raw
hypertables (time-series panels) are fine. `uv run vlc grafana check` (also a
CI step) fails when a panel or alert re-derives view logic against
`air.hyper`/`weather.hyper`. After changing dbt models, apply them with
`docker compose -f compose/docker-compose.yml --profile infra --profile dbt run --rm dbt run --profiles-dir /dbt`.

## Alerting
`provisioning/alerting/vlc-staleness.yml` provisions the Grafana-managed rule
**WeatherStationStale** (folder `VLC`, evaluated every 5m): one alert instance
per `fiwareid` whose `public.data_freshness` staleness exceeds 5 hours (the
mart's `freshness_offline_max` dbt var — keep the rule threshold and the var
in sync). It catches single dead stations (e.g. `W05_VALENCIA_UPV_10m`, silent
since 2026-02-05) that the topic-level Prometheus alerts cannot see.

Grafana-managed alerts are forwarded to the existing Alertmanager
(`handleGrafanaManagedAlerts: true` in `provisioning/datasources/alertmanager.yml`)
and reuse its email routing. Alert provisioning is loaded at startup only, so
changes to the YAML require a Grafana restart.

One-time step on a fresh Grafana database: the datasource toggle is only
honored for orgs that have an ngalert admin configuration, which is stored in
Grafana's DB and cannot be file-provisioned. Create it once via the API
(replace REAL-ADMIN-USER/REAL-ADMIN-PASSWORD):
```bash
curl -u "REAL-ADMIN-USER:REAL-ADMIN-PASSWORD" \
  -H "Content-Type: application/json" \
  -X POST -d '{"alertmanagersChoice": "external"}' \
  http://localhost:8080/grafana/api/v1/ngalert/admin_config
```
`external` routes Grafana-managed alert notifications exclusively through the
external Alertmanager (Grafana's internal notifier has no SMTP configured).

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
