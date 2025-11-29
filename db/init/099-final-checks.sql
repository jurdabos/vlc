-- 099-final-checks.sql

-- Check TimescaleDB and PostGIS are installed
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
    RAISE EXCEPTION 'timescaledb extension is not installed in database %', current_database();
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'postgis') THEN
    RAISE EXCEPTION 'postgis extension is not installed in database %', current_database();
  END IF;
END;
$$;
-- Continuous aggregates wired up
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM timescaledb_information.hypertables
    WHERE hypertable_schema = 'air' AND hypertable_name = 'hyper'
  ) THEN
    RAISE EXCEPTION 'Hypertable air.hyper does not exist';
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM timescaledb_information.hypertables
    WHERE hypertable_schema = 'weather' AND hypertable_name = 'hyper'
  ) THEN
    RAISE EXCEPTION 'Hypertable weather.hyper does not exist';
  END IF;
END;
$$;
-- Policies (compression / retention / jobs)
-- Compression/retention jobs exist
SELECT job_id, application_name, hypertable_schema, hypertable_name
FROM timescaledb_information.jobs
WHERE application_name IN ('Compression Policy', 'Retention Policy')
  AND hypertable_schema IN ('air', 'weather');

