# Grafana Configuration

Grafana dashboards and provisioning configuration for Valencia air quality and weather monitoring.

## Directory Structure

```
grafana/
├── dashboards/
│   ├── air_quality.json       # Air quality dashboard with spatial queries
│   ├── weather.json           # Weather dashboard with meteorological metrics
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
Grafana's native Postgres data source works with TimescaleDB out of the box.

- Host: `timescaledb:5432`
- Database: `vlc`
- User: `postgres` / `postgres`
- Schemas:
  - `air.hyper` / `air.daily` - Air quality data
  - `weather.hyper` / `weather.daily` - Weather data

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

### Valencia Weather
Visualizes meteorological data from Valencia weather stations:
- Total row count in weather.hyper
- Temperature trends across stations (last 24h)
- Latest weather readings by station
- Spatial query: stations within 1km of coordinates
- Humidity and pressure charts (last 7 days)

### System Metrics
System and infrastructure monitoring (Kafka, Connect, Prometheus).

## SQL Checks

You can run these queries directly in Grafana's Explore or psql.

### Air Quality Checks

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

### Weather Checks

```sql
-- Count arriving rows
SELECT count(*) FROM weather.hyper;

-- Latest 5 readings for temperature at a station
SELECT ts, fiwareid, temperature_c, humidity_pct, wind_speed_ms
FROM weather.hyper
ORDER BY ts DESC
LIMIT 5;

-- Current conditions by station
SELECT DISTINCT ON (fiwareid)
  fiwareid,
  ts,
  temperature_c,
  humidity_pct,
  pressure_hpa,
  wind_speed_ms,
  wind_dir_deg,
  precip_mm
FROM weather.hyper
ORDER BY fiwareid, ts DESC;

-- Spatial: weather stations within 1 km of 39.47N, -0.376E
SELECT DISTINCT fiwareid, lat, lon
FROM weather.hyper
WHERE ST_DWithin(
  geo,
  ST_SetSRID(ST_MakePoint(-0.376, 39.470), 4326)::geography,
  1000
);

-- Daily temperature averages (last 7 days)
SELECT bucket, fiwareid, temperature_avg, humidity_avg
FROM weather.daily
WHERE bucket > NOW() - INTERVAL '7 days'
ORDER BY bucket DESC, fiwareid;

-- Hourly temperature trend for last 24h
SELECT
  time_bucket('1 hour', ts) AS hour,
  AVG(temperature_c) AS avg_temp,
  AVG(humidity_pct) AS avg_humidity,
  AVG(pressure_hpa) AS avg_pressure
FROM weather.hyper
WHERE ts > NOW() - INTERVAL '24 hours'
GROUP BY hour
ORDER BY hour;
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
- Both air and weather use PostGIS geometry columns (`geom`/`geo`) for spatial queries
