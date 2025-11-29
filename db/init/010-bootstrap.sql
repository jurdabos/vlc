-- Schemas
CREATE SCHEMA IF NOT EXISTS air;
CREATE SCHEMA IF NOT EXISTS weather;

-- ======================
-- Air: hypertable
-- ======================
CREATE TABLE IF NOT EXISTS air.hyper (
  fiwareid            text        NOT NULL,
  ts                  timestamptz NOT NULL,
  no2                 double precision,
  o3                  double precision,
  so2                 double precision,
  co                  double precision,
  pm10                double precision,
  pm25                double precision,
  air_quality_summary text,
  lat                 double precision,
  lon                 double precision,
  geo                 geography(Point, 4326) GENERATED ALWAYS AS
                        (CASE WHEN lat IS NOT NULL AND lon IS NOT NULL
                              THEN ST_SetSRID(ST_MakePoint(lon,lat),4326)::geography
                              ELSE NULL END) STORED,
  PRIMARY KEY (fiwareid, ts)
);

SELECT create_hypertable('air.hyper','ts', if_not_exists => TRUE, chunk_time_interval => INTERVAL '7 days');

-- Assistive indexes
CREATE INDEX IF NOT EXISTS air_h_ts_idx  ON air.hyper (ts DESC);
CREATE INDEX IF NOT EXISTS air_h_geo_gix ON air.hyper USING GIST (geo);

-- Compression (explicit segment/order) & retention
ALTER TABLE air.hyper SET (
  timescaledb.compress = true,
  timescaledb.compress_segmentby = 'fiwareid',
  timescaledb.compress_orderby = 'ts DESC'
);
SELECT add_compression_policy('air.hyper', INTERVAL '7 days');
SELECT add_retention_policy('air.hyper',  INTERVAL '6000 days');

-- Continuous aggregates
CREATE MATERIALIZED VIEW IF NOT EXISTS air.daily
WITH (timescaledb.continuous) AS
SELECT
  fiwareid,
  time_bucket(INTERVAL '1 day', ts) AS bucket_day,
  avg(co)   AS co_avg,
  avg(no2)  AS no2_avg,
  avg(o3)   AS o3_avg,
  avg(pm10) AS pm10_avg,
  avg(pm25) AS pm25_avg,
  avg(so2)  AS so2_avg
FROM air.hyper
GROUP BY fiwareid, bucket_day;

SELECT add_continuous_aggregate_policy('air.daily',
  start_offset => INTERVAL '30 days',
  end_offset   => INTERVAL '1 day',
  schedule_interval => INTERVAL '1 hour');

CREATE MATERIALIZED VIEW IF NOT EXISTS air.weekly
WITH (timescaledb.continuous) AS
SELECT
  fiwareid,
  time_bucket(INTERVAL '7 days', ts) AS bucket_week,
  avg(co)   AS co_avg,
  avg(no2)  AS no2_avg,
  avg(o3)   AS o3_avg,
  avg(pm10) AS pm10_avg,
  avg(pm25) AS pm25_avg,
  avg(so2)  AS so2_avg
FROM air.hyper
GROUP BY fiwareid, bucket_week;

SELECT add_continuous_aggregate_policy('air.weekly',
  start_offset => INTERVAL '180 days',
  end_offset   => INTERVAL '1 day',
  schedule_interval => INTERVAL '4 hours');

-- =========================
-- Weather: hypertable
-- =========================
CREATE TABLE IF NOT EXISTS weather.hyper (
  fiwareid       text        NOT NULL,
  ts             timestamptz NOT NULL,
  wind_dir_deg   double precision,
  wind_speed_ms  double precision,
  temperature_c  double precision,
  humidity_pct   double precision,
  pressure_hpa   double precision,
  precip_mm      double precision,
  lat            double precision,
  lon            double precision,
  geo            geography(Point, 4326) GENERATED ALWAYS AS
                   (CASE WHEN lat IS NOT NULL AND lon IS NOT NULL
                         THEN ST_SetSRID(ST_MakePoint(lon,lat),4326)::geography
                         ELSE NULL END) STORED,
  PRIMARY KEY (fiwareid, ts)
);

SELECT create_hypertable('weather.hyper','ts', if_not_exists => TRUE, chunk_time_interval => INTERVAL '7 days');

-- Helpful indexes
CREATE INDEX IF NOT EXISTS weather_h_ts_idx  ON weather.hyper (ts DESC);
CREATE INDEX IF NOT EXISTS weather_h_geo_gix ON weather.hyper USING GIST (geo);

-- Compression & retention
ALTER TABLE weather.hyper SET (
  timescaledb.compress = true,
  timescaledb.compress_segmentby = 'fiwareid',
  timescaledb.compress_orderby = 'ts DESC'
);
SELECT add_compression_policy('weather.hyper', INTERVAL '7 days');
SELECT add_retention_policy('weather.hyper',  INTERVAL '365 days');

-- Continuous aggregates
-- Circular mean for wind direction: atan2(avg(sin), avg(cos)) converted to degrees
-- This correctly handles wrap-around (e.g., avg of 350° and 10° = 0°, not 180°)
CREATE MATERIALIZED VIEW IF NOT EXISTS weather.daily
WITH (timescaledb.continuous) AS
SELECT
  fiwareid,
  time_bucket(INTERVAL '1 day', ts) AS bucket_day,
  avg(temperature_c) AS temp_avg_c,
  avg(humidity_pct)  AS humidity_avg,
  avg(pressure_hpa)  AS pressure_avg,
  max(precip_mm)     AS precip_max_mm,
  -- Circular mean for wind direction
  degrees(atan2(
    avg(sin(radians(wind_dir_deg))),
    avg(cos(radians(wind_dir_deg)))
  )) AS wind_avg_deg,
  avg(wind_speed_ms) AS wind_avg_ms
FROM weather.hyper
GROUP BY fiwareid, bucket_day;

SELECT add_continuous_aggregate_policy('weather.daily',
  start_offset => INTERVAL '60 days',
  end_offset   => INTERVAL '1 day',
  schedule_interval => INTERVAL '1 hour');

-- Circular mean for wind direction in weekly aggregate
CREATE MATERIALIZED VIEW IF NOT EXISTS weather.weekly
WITH (timescaledb.continuous) AS
SELECT
  fiwareid,
  time_bucket(INTERVAL '7 days', ts) AS bucket_week,
  avg(temperature_c) AS temp_avg_c,
  avg(humidity_pct)  AS humidity_avg,
  avg(pressure_hpa)  AS pressure_avg,
  max(precip_mm)     AS precip_max_mm,
  -- Circular mean for wind direction
  degrees(atan2(
    avg(sin(radians(wind_dir_deg))),
    avg(cos(radians(wind_dir_deg)))
  )) AS wind_avg_deg,
  avg(wind_speed_ms) AS wind_avg_ms
FROM weather.hyper
GROUP BY fiwareid, bucket_week;

SELECT add_continuous_aggregate_policy('weather.weekly',
  start_offset => INTERVAL '180 days',
  end_offset   => INTERVAL '1 day',
  schedule_interval => INTERVAL '4 hours');
  
-- =========================
-- Connector role & privileges
-- =========================

-- 1) Create a dedicated login role for Kafka Connect
-- NOTE: change the password in connect/secrets/secrets.properties (`TS_PASSWORD`) to match
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'vlc_dev') THEN
    CREATE ROLE vlc LOGIN PASSWORD 'placeholder';
  END IF;
END
$$;

-- 2) Allow it to connect to the `vlc` database
GRANT CONNECT ON DATABASE vlc TO vlc_dev;

-- 3) Let it use the relevant schemas
GRANT USAGE ON SCHEMA air TO vlc_dev;
GRANT USAGE ON SCHEMA weather TO vlc_dev;

-- 4) Allow inserts/updates into the hypertables
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA air TO vlc_dev;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA weather TO vlc_dev;

-- 5) Ensure future tables in these schemas are also writable by vlc_dev
ALTER DEFAULT PRIVILEGES IN SCHEMA air
  GRANT SELECT, INSERT, UPDATE ON TABLES TO vlc_dev;

ALTER DEFAULT PRIVILEGES IN SCHEMA weather
  GRANT SELECT, INSERT, UPDATE ON TABLES TO vlc_dev;

