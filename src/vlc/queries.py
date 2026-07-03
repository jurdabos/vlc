"""Presentation-layer SQL for the vlc CLI data commands.

Single-source-of-truth contract: every statement below is a plain SELECT
from a dbt-managed view or mart (``dbt/models/``), never derived logic
(latest-per-station, staleness math, AQI classification) against the raw
hypertables. ``vlc grafana check`` enforces the same contract for the
Grafana dashboards and alert rules; code review enforces it here.
"""

STATUS = """\
SELECT schema_name, fiwareid, last_update, staleness, status
FROM public.data_freshness
ORDER BY schema_name, fiwareid;"""

LATEST_WEATHER = """\
SELECT fiwareid, ts, round(age_s) AS age_s, temperature_c, humidity_pct,
       pressure_hpa, wind_dir_deg, wind_speed_ms, precip_mm, lat, lon
FROM weather.latest
ORDER BY fiwareid;"""

LATEST_AIR = """\
SELECT fiwareid, ts, round(age_s) AS age_s, no2, o3, so2, co, pm10, pm25,
       air_quality_summary, lat, lon
FROM air.latest
ORDER BY fiwareid;"""

SNAPSHOT = """\
SELECT fiwareid, air_ts, weather_ts, no2, o3, pm10, pm25, air_quality_summary,
       temperature_c, humidity_pct, wind_speed_ms, precip_mm, lat, lon
FROM public.station_snapshot
ORDER BY fiwareid;"""

STATIONS = """\
SELECT *
FROM public.stations
ORDER BY 1;"""

RECORDS = """\
SELECT record_type, temperature_c, humidity_pct, pressure_hpa, wind_speed_ms,
       precip_mm, wind_dir_deg, fiwareid, ts
FROM public.weather_records;"""
