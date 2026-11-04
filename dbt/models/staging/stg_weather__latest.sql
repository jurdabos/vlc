{{
    config(
        schema='weather',
        alias='latest'
    )
}}

-- Most recent weather reading per station.
-- Replaces the hand-maintained weather.latest view from 020-views.sql.
-- age_s is computed at query time; it is the single source of truth for
-- per-station data age (consumed by the Grafana "Current Conditions by
-- Station" panel and the vlc CLI). Reads the validity layer, so impossible
-- measurements arrive as NULLs.

select distinct on (fiwareid)
    fiwareid,
    ts,
    extract(epoch from now() - ts) as age_s,
    temperature_c,
    humidity_pct,
    pressure_hpa,
    wind_dir_deg,
    wind_speed_ms,
    precip_mm,
    lat,
    lon,
    geo
from {{ ref('stg_weather__clean') }}
order by fiwareid, ts desc
