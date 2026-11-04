{{
    config(
        schema='air',
        alias='daily_trends'
    )
}}

-- Day-over-day air quality trends per station.
-- Replaces the hand-maintained air.daily_trends view from 020-views.sql.

select
    fiwareid,
    bucket_day::date as day,
    co_avg,
    no2_avg,
    o3_avg,
    pm10_avg,
    pm25_avg,
    so2_avg,
    lag(pm25_avg) over (partition by fiwareid order by bucket_day) as pm25_prev,
    pm25_avg - lag(pm25_avg) over (partition by fiwareid order by bucket_day) as pm25_delta
from {{ source('air', 'daily') }}
