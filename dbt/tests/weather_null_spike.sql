-- Fails if any weather measurement column has more than 30% NULLs.
-- Derived from direct_sql_queries.txt check #9.

select
    'weather.hyper' as table_name,
    count(*) as total_rows,
    round(100.0 * count(*) filter (where temperature_c is null) / nullif(count(*), 0), 2) as temp_null_pct,
    round(100.0 * count(*) filter (where humidity_pct is null) / nullif(count(*), 0), 2) as humidity_null_pct,
    round(100.0 * count(*) filter (where pressure_hpa is null) / nullif(count(*), 0), 2) as pressure_null_pct
from {{ source('weather', 'hyper') }}
having
    100.0 * count(*) filter (where temperature_c is null) / nullif(count(*), 0) > 30
    or 100.0 * count(*) filter (where humidity_pct is null) / nullif(count(*), 0) > 30
    or 100.0 * count(*) filter (where pressure_hpa is null) / nullif(count(*), 0) > 30
