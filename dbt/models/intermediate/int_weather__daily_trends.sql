{{
    config(
        schema='weather',
        alias='daily_trends'
    )
}}

-- Day-over-day weather trends per station.
-- Replaces the hand-maintained weather.daily_trends view from 020-views.sql.

select
    fiwareid,
    bucket_day::date as day,
    temp_avg_c,
    humidity_avg,
    pressure_avg,
    precip_max_mm,
    wind_avg_deg,
    wind_avg_ms,
    lag(temp_avg_c) over (partition by fiwareid order by bucket_day) as temp_prev,
    temp_avg_c - lag(temp_avg_c) over (partition by fiwareid order by bucket_day) as temp_delta
from {{ source('weather', 'daily') }}
