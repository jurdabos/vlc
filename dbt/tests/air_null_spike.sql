-- Fails if any air measurement column has more than 30% NULLs.
-- Derived from direct_sql_queries.txt check #9.

select
    'air.hyper' as table_name,
    count(*) as total_rows,
    round(100.0 * count(*) filter (where no2 is null) / nullif(count(*), 0), 2) as no2_null_pct,
    round(100.0 * count(*) filter (where o3 is null) / nullif(count(*), 0), 2) as o3_null_pct,
    round(100.0 * count(*) filter (where pm10 is null) / nullif(count(*), 0), 2) as pm10_null_pct,
    round(100.0 * count(*) filter (where pm25 is null) / nullif(count(*), 0), 2) as pm25_null_pct
from {{ source('air', 'hyper') }}
having
    100.0 * count(*) filter (where no2 is null) / nullif(count(*), 0) > 30
    or 100.0 * count(*) filter (where o3 is null) / nullif(count(*), 0) > 30
    or 100.0 * count(*) filter (where pm10 is null) / nullif(count(*), 0) > 30
    or 100.0 * count(*) filter (where pm25 is null) / nullif(count(*), 0) > 30
