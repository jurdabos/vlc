-- Failing rows: air quality readings outside physical measurement bounds.
-- Derived from direct_sql_queries.txt check #5.

select *
from {{ source('air', 'hyper') }}
where no2 < 0 or no2 > 1000
   or o3 < 0 or o3 > 500
   or so2 < 0 or so2 > 2000
   or co < 0 or co > 100
   or pm10 < 0 or pm10 > 1000
   or pm25 < 0 or pm25 > 500
