-- Backfill script for historical air quality data (2016-2022)
-- Creates a staging table, loads CSVs, transforms and inserts into air.hyper and weather.hyper
--
-- Usage:
--   1. Copy CSVs to container:
--      docker cp backfill/hourly_2016_2020.csv $(docker compose -f compose/docker-compose.yml ps -q timescaledb):/tmp/
--      docker cp backfill/hourly_2021_2022.csv $(docker compose -f compose/docker-compose.yml ps -q timescaledb):/tmp/
--   2. Run this script:
--      docker compose -f compose/docker-compose.yml exec -T timescaledb psql -U vlc_dev -d vlc -f /tmp/backfill.sql

-- Creating staging table
DROP TABLE IF EXISTS staging.hourly_raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE TABLE staging.hourly_raw (
    id integer,
    fecha text,
    dia_de_la_semana text,
    dia_del_mes integer,
    hora text,
    estacion text,
    pm1 text,
    pm2_5 text,
    pm10 text,
    no text,
    no2 text,
    nox text,
    o3 text,
    so2 text,
    co text,
    velocidad_del_viento text,
    direccion_del_viento text,
    nh3 text,
    c7h8 text,
    c6h6 text,
    ruido text,
    c8h10 text,
    temperatura text,
    humedad_relativa text,
    presion text,
    radiacion text,
    precipitacion text,
    velocidad_maxima_del_viento text,
    fecha_creacion text,
    fecha_baja text
);

-- Loading CSV data (2016-2020 and 2021-2022)
\copy staging.hourly_raw FROM '/tmp/hourly_2016_2020.csv' WITH (FORMAT csv, HEADER true);
\copy staging.hourly_raw FROM '/tmp/hourly_2021_2022.csv' WITH (FORMAT csv, HEADER true);

-- Inserting air quality data with station mapping
INSERT INTO air.hyper (fiwareid, ts, no2, o3, so2, co, pm10, pm25, lat, lon)
SELECT
    CASE estacion
        WHEN 'Avda. Francia' THEN 'A01_AVFRANCIA_60m'
        WHEN 'Bulevard Sud' THEN 'A02_BULEVARDSUD_60m'
        WHEN 'Moli del Sol' THEN 'A03_MOLISOL_60m'
        WHEN 'Pista Silla' THEN 'A04_PISTASILLA_60m'
        WHEN 'Politecnico' THEN 'A05_POLITECNIC_60m'
        WHEN 'Viveros' THEN 'A06_VIVERS_60m'
        WHEN 'Valencia Centro' THEN 'A07_VALENCIACENTRE_60m'
        WHEN 'Valencia Olivereta' THEN 'A10_OLIVERETA_60m'
    END as fiwareid,
    (fecha || 'T' || hora)::timestamptz as ts,
    NULLIF(no2, '')::double precision,
    NULLIF(o3, '')::double precision,
    NULLIF(so2, '')::double precision,
    NULLIF(co, '')::double precision,
    NULLIF(pm10, '')::double precision,
    NULLIF(pm2_5, '')::double precision,
    CASE estacion
        WHEN 'Avda. Francia' THEN 39.45782688751831
        WHEN 'Bulevard Sud' THEN 39.45039600550536
        WHEN 'Moli del Sol' THEN 39.48111211090413
        WHEN 'Pista Silla' THEN 39.45806095369672
        WHEN 'Politecnico' THEN 39.47964449692915
        WHEN 'Viveros' THEN 39.47964092480533
        WHEN 'Valencia Centro' THEN 39.470547670260125
        WHEN 'Valencia Olivereta' THEN 39.46924423509195
    END as lat,
    CASE estacion
        WHEN 'Avda. Francia' THEN -0.342986232422652
        WHEN 'Bulevard Sud' THEN -0.3963375643758562
        WHEN 'Moli del Sol' THEN -0.4088098969009376
        WHEN 'Pista Silla' THEN -0.37664393657915707
        WHEN 'Politecnico' THEN -0.33740066052186946
        WHEN 'Viveros' THEN -0.36964822314381013
        WHEN 'Valencia Centro' THEN -0.37639765165532396
        WHEN 'Valencia Olivereta' THEN -0.40592344552906795
    END as lon
FROM staging.hourly_raw
WHERE estacion IN ('Avda. Francia', 'Bulevard Sud', 'Moli del Sol', 'Pista Silla', 
                   'Politecnico', 'Viveros', 'Valencia Centro', 'Valencia Olivereta')
  AND fecha IS NOT NULL
  AND hora IS NOT NULL
ON CONFLICT (fiwareid, ts) DO NOTHING;

-- Inserting weather data with station mapping
INSERT INTO weather.hyper (fiwareid, ts, wind_dir_deg, wind_speed_ms, temperature_c, 
                           humidity_pct, pressure_hpa, precip_mm, lat, lon)
SELECT
    CASE estacion
        WHEN 'Avda. Francia' THEN 'W01_AVFRANCIA_10m'
        WHEN 'Nazaret Meteo' THEN 'W02_NAZARET_10m'
        WHEN 'Viveros' THEN 'W04_VALENCIADT_10m'
        WHEN 'Politecnico' THEN 'W05_VALENCIA_UPV_10m'
    END as fiwareid,
    (fecha || 'T' || hora)::timestamptz as ts,
    NULLIF(direccion_del_viento, '')::double precision,
    NULLIF(velocidad_del_viento, '')::double precision,
    NULLIF(temperatura, '')::double precision,
    NULLIF(humedad_relativa, '')::double precision,
    NULLIF(presion, '')::double precision,
    NULLIF(precipitacion, '')::double precision,
    CASE estacion
        WHEN 'Avda. Francia' THEN 39.45782688751831
        WHEN 'Nazaret Meteo' THEN 39.4485309997218
        WHEN 'Viveros' THEN 39.47964092480533
        WHEN 'Politecnico' THEN 39.47964449692915
    END as lat,
    CASE estacion
        WHEN 'Avda. Francia' THEN -0.342986232422652
        WHEN 'Nazaret Meteo' THEN -0.3332980005434063
        WHEN 'Viveros' THEN -0.36964822314381013
        WHEN 'Politecnico' THEN -0.33740066052186946
    END as lon
FROM staging.hourly_raw
WHERE estacion IN ('Avda. Francia', 'Nazaret Meteo', 'Viveros', 'Politecnico')
  AND fecha IS NOT NULL
  AND hora IS NOT NULL
  AND NULLIF(temperatura, '') IS NOT NULL
ON CONFLICT (fiwareid, ts) DO NOTHING;

-- Cleaning up staging
DROP TABLE staging.hourly_raw;
DROP SCHEMA staging;

-- Showing final counts
SELECT 'air.hyper' as table_name, COUNT(*) as row_count FROM air.hyper
UNION ALL
SELECT 'weather.hyper', COUNT(*) FROM weather.hyper;
