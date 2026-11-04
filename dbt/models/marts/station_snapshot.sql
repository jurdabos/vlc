{{
    config(
        schema='public',
        alias='station_snapshot'
    )
}}

-- Combined air + weather snapshot — one row per station.
-- Replaces the hand-maintained public.station_snapshot view from 020-views.sql.

select
    coalesce(a.fiwareid, w.fiwareid) as fiwareid,
    a.ts as air_ts,
    w.ts as weather_ts,
    a.no2,
    a.o3,
    a.so2,
    a.co,
    a.pm10,
    a.pm25,
    a.air_quality_summary,
    w.temperature_c,
    w.humidity_pct,
    w.pressure_hpa,
    w.wind_dir_deg,
    w.wind_speed_ms,
    w.precip_mm,
    coalesce(a.lat, w.lat) as lat,
    coalesce(a.lon, w.lon) as lon
from {{ ref('stg_air__latest') }} a
full outer join {{ ref('stg_weather__latest') }} w using (fiwareid)
