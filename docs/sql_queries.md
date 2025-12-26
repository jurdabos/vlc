# SQL Queries for TimescaleDB
Example queries for exploring air quality and weather data in Grafana Explore or via CLI.

## CLI Access
```bash
docker compose -f compose/docker-compose.yml exec timescaledb psql -U vlc_dev -d vlc -c "<query>"
```

## Air Quality Queries
```sql
-- Count arriving rows
SELECT count(*) FROM air.hyper;

-- Latest 5 readings for NO2 at around Av. de Francia
SELECT ts, fiwareid, no2
FROM air.hyper
WHERE fiwareid LIKE '%FRANCIA%'
ORDER BY ts DESC
LIMIT 5;

-- Spatial: stations within 3 km of 39.494N, -0.403E
SELECT DISTINCT fiwareid
FROM air.hyper
WHERE ST_DWithin(
  geo,
  ST_SetSRID(ST_MakePoint(-0.403, 39.494), 4326)::geography,
  3000
)
ORDER BY fiwareid;
```

## Weather Queries
```sql
-- Count arriving rows
SELECT count(*) FROM weather.hyper;

-- Latest 5 readings for temperature at any station
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

-- Spatial: weather stations within 3250 m of 39.494N, -0.403E
SELECT DISTINCT fiwareid, lat, lon
FROM weather.hyper
WHERE ST_DWithin(
  geo,
  ST_SetSRID(ST_MakePoint(-0.403, 39.494), 4326)::geography,
  3250
);

-- Daily temperature averages (last 7 days)
SELECT bucket_day, fiwareid, temp_avg_c, humidity_avg
FROM weather.daily
WHERE bucket_day > NOW() - INTERVAL '7 days'
ORDER BY bucket_day DESC, fiwareid;

-- Hourly temperature trend for last 24h across stations
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
