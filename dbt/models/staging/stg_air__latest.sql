{{
    config(
        schema='air',
        alias='latest'
    )
}}

-- Most recent air quality reading per station.
-- Replaces the hand-maintained air.latest view from 020-views.sql.
-- Reads the validity layer, so impossible readings arrive as NULLs.

select distinct on (fiwareid)
    fiwareid,
    ts,
    extract(epoch from now() - ts) as age_s,
    no2,
    o3,
    so2,
    co,
    pm10,
    pm25,
    air_quality_summary,
    lat,
    lon,
    geo
from {{ ref('stg_air__clean') }}
order by fiwareid, ts desc
