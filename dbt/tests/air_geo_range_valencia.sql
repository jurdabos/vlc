-- Failing rows: air readings with coordinates outside the Valencia area.
-- Bounding box: lat 39.0–40.0, lon -1.0–0.5
-- Derived from direct_sql_queries.txt check #6.

select *
from {{ source('air', 'hyper') }}
where lat is not null and lon is not null
  and (lat < 39.0 or lat > 40.0 or lon < -1.0 or lon > 0.5)
