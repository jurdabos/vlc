-- Failing rows: weather readings outside physical measurement bounds.
-- Derived from direct_sql_queries.txt check #5.

select *
from {{ source('weather', 'hyper') }}
where temperature_c < -40 or temperature_c > 60
   or humidity_pct < 0 or humidity_pct > 105
   or pressure_hpa < 800 or pressure_hpa > 1100
   or wind_speed_ms < 0 or wind_speed_ms > 100
   or wind_dir_deg < 0 or wind_dir_deg > 360
   or precip_mm < 0 or precip_mm > 500
