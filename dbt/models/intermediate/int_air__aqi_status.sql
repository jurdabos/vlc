{{
    config(
        schema='air',
        alias='aqi_status'
    )
}}

-- AQI categorization per station based on latest PM2.5 and PM10 readings.
-- Replaces the hand-maintained air.aqi_status view from 020-views.sql.

select
    fiwareid,
    ts,
    pm25,
    pm10,
    no2,
    o3,
    so2,
    co,
    case
        when pm25 <= 12 and pm10 <= 54 then 'Good'
        when pm25 <= 35.4 or pm10 <= 154 then 'Moderate'
        when pm25 <= 55.4 or pm10 <= 254 then 'Unhealthy for Sensitive Groups'
        when pm25 <= 150.4 or pm10 <= 354 then 'Unhealthy'
        when pm25 <= 250.4 or pm10 <= 424 then 'Very Unhealthy'
        else 'Hazardous'
    end as aqi_category,
    lat,
    lon
from {{ ref('stg_air__latest') }}
