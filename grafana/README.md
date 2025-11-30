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


## Datasources

### TimescaleDB (PostgreSQL)
Grafana's native Postgres data source works with TimescaleDB out of the box.

- Database: `vlc`
- Schemas:
  - `air.hyper` / `air.daily` - Air quality data
  - `weather.hyper` / `weather.daily` - Weather data

### Prometheus
- Metrics: Kafka, Connect, system metrics

## Dashboards

### Valencia Air Quality
Visualizes air quality data from the Valencia open data API:
- Total row count in air.hyper
- Spatial query: stations within 1km of baseline home coordinates (make this changeable in ph 3/4/5)
- Levels for all stations (last 7 days)

### Valencia Weather
Visualizes meteorological data from Valencia weather stations:
- Total row count in weather.hyper
- Temperature trends across stations (last 24h)
- Latest weather readings by station
- Spatial query: stations within 1km of coordinates
- Humidity and pressure charts (last 7 days)

### System Metrics
System and infrastructure monitoring:
- **Service Status** - Up/Down indicators for Connect, Schema Registry, Prometheus
- **Kafka Connect** - Active connectors, task batch sizes, put batch times, record failures
- **Schema Registry** - Schema/subject counts, HTTP request rates and latencies (p99)

**Note:** Kafka broker metrics require adding JMX exporter to the kafka service in docker-compose.yml (planned for future phases).

## SQL Checks
Run these queries directly in Grafana's Explore or docker exec timescaledb psql -U vlc_dev -d vlc -c "".

### Air Quality Checks

```sql
-- Count arriving rows
docker exec timescaledb psql -U vlc_dev -d vlc -c "
SELECT count(*) FROM air.hyper;
"

-- Latest 5 readings for NO2 at around Av. de Francia
docker exec timescaledb psql -U vlc_dev -d vlc -c "
SELECT ts, fiwareid, no2
FROM air.hyper
WHERE fiwareid LIKE '%FRANCIA%'
ORDER BY ts DESC
LIMIT 5;
"

-- Spatial: stations within 3 km of 39.494N, -0.403E
docker exec timescaledb psql -U vlc_dev -d vlc -c "
SELECT DISTINCT fiwareid
FROM air.hyper
WHERE ST_DWithin(
  geo,
  ST_SetSRID(ST_MakePoint(-0.403, 39.494), 4326)::geography,
  3000
)
ORDER BY fiwareid;
"

### Weather Checks
-- Count arriving rows
docker exec timescaledb psql -U vlc_dev -d vlc -c "
SELECT count(*) FROM weather.hyper;
"

-- Latest 5 readings for temperature at a station
docker exec timescaledb psql -U vlc_dev -d vlc -c "
SELECT ts, fiwareid, temperature_c, humidity_pct, wind_speed_ms
FROM weather.hyper
ORDER BY ts DESC
LIMIT 5;
"

-- Current conditions by station
docker exec timescaledb psql -U vlc_dev -d vlc -c "
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
"

-- Spatial: weather stations within 3 km of 39.494N, -0.403E
docker exec timescaledb psql -U vlc_dev -d vlc -c "
SELECT DISTINCT fiwareid, lat, lon
FROM weather.hyper
WHERE ST_DWithin(
  geo,
  ST_SetSRID(ST_MakePoint(-0.403, 39.494), 4326)::geography,
  3000
);
"

-- Daily temperature averages (last 7 days)
docker exec timescaledb psql -U vlc_dev -d vlc -c "
SELECT bucket_day, fiwareid, temp_avg_c, humidity_avg
FROM weather.daily
WHERE bucket_day > NOW() - INTERVAL '7 days'
ORDER BY bucket_day DESC, fiwareid;
"

-- Historical data for 1 Dec to 8 Dec
docker exec timescaledb psql -U vlc_dev -d vlc -c "
SELECT 
    EXTRACT(DAY FROM bucket_day)::int AS day_of_dec,
    fiwareid,
    ROUND(AVG(temp_avg_c)::numeric, 2) AS temp_avg_c,
    ROUND(AVG(humidity_avg)::numeric, 2) AS humidity_avg,
    ROUND(AVG(pressure_avg)::numeric, 2) AS pressure_avg,
    ROUND(AVG(precip_max_mm)::numeric, 2) AS precip_max_mm,
    ROUND(AVG(wind_avg_deg)::numeric, 2) AS wind_avg_deg,
    ROUND(AVG(wind_avg_ms)::numeric, 2) AS wind_avg_ms,
    COUNT(*) AS years_of_data
FROM weather.daily
WHERE EXTRACT(MONTH FROM bucket_day) = 12
  AND EXTRACT(DAY FROM bucket_day) BETWEEN 1 AND 8
GROUP BY day_of_dec, fiwareid
ORDER BY day_of_dec, fiwareid;
"

-- Hourly temperature trend for last 24h across stations
docker exec timescaledb psql -U vlc_dev -d vlc -c "
SELECT
  time_bucket('1 hour', ts) AS hour,
  AVG(temperature_c) AS avg_temp,
  AVG(humidity_pct) AS avg_humidity,
  AVG(pressure_hpa) AS avg_pressure
FROM weather.hyper
WHERE ts > NOW() - INTERVAL '24 hours'
GROUP BY hour
ORDER BY hour;
"

## Starting Grafana
```bash
cd /opt/vlc/compose
docker compose --profile ui up -d grafana
```

## Notes
- Dashboards auto-refresh every 30 seconds
- Provisioned datasources and dashboards are loaded automatically on startup
- Dashboard changes in the UI are persisted (allowUiUpdates: true)
- Both air and weather use PostGIS geometry column (`geo(m)`) for spatial queries
