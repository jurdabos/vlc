# Grafana Configuration

Grafana dashboards and provisioning configuration for Valencia air quality monitoring.

## Directory Structure

```
grafana/
├── dashboards/
│   ├── air_quality.json       # Air quality dashboard with spatial queries
│   └── system_metrics.json    # System metrics dashboard
└── provisioning/
    ├── datasources/
    │   ├── timescaledb.yml    # TimescaleDB PostgreSQL datasource
    │   └── prometheus.yml     # Prometheus datasource
    └── dashboards/
        └── dashboards.yml     # Dashboard provider config
```

## Access

- URL: http://localhost:3000/grafana/
- Default credentials: admin/admin

## Datasources

### TimescaleDB (PostgreSQL)
- Database: `air`
- Schema: `air.hyper`
- Connection: `timescaledb:5432`
- User: postgres/postgres

### Prometheus
- Connection: `http://prometheus:9090`
- Metrics: Kafka, Connect, system metrics

## Dashboards

### Valencia Air Quality
Visualizes air quality data from the Valencia open data API:
- Total row count in air.hyper
- NO2 levels for Patraix station (last 24h)
- Latest 5 NO2 readings for Patraix
- Spatial query: stations within 1km of coordinates (39.47°N, -0.376°E)
- NO2 levels for all stations (last 7 days)

### System Metrics
System and infrastructure monitoring (Kafka, Connect, Prometheus).

## SQL Checks

You can run these queries directly in Grafana's Explore or psql:

```sql
-- Count arriving rows
SELECT count(*) FROM air.hyper;

-- Latest 5 readings for NO2 in Patraix
SELECT ts, nombre, no2
FROM air.hyper
WHERE nombre ILIKE 'Patraix'
ORDER BY ts DESC
LIMIT 5;

-- Spatial: stations within 1 km of 39.47N, -0.376E
SELECT nombre, direccion
FROM air.hyper
WHERE ST_DWithin(
  geom,
  ST_SetSRID(ST_MakePoint(-0.376, 39.470), 4326)::geography,
  1000
)
GROUP BY nombre, direccion
ORDER BY nombre;
```

## Starting Grafana

```bash
cd /opt/vlc/compose
docker compose --profile ui up -d grafana
```

## Notes

- Dashboards auto-refresh every 30 seconds
- Provisioned datasources and dashboards are loaded automatically on startup
- Dashboard changes in the UI are persisted (allowUiUpdates: true)
