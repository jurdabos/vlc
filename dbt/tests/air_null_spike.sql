-- Warns if any air measurement column has more than 30% NULLs.
-- Derived from direct_sql_queries.txt check #9.
-- Severity warn: this monitors the RAW layer, where high null shares are
-- partly structural (not every station carries every sensor — pm10/pm25
-- sit at ~36% nulls, co at ~61%). A hard failure here would block builds
-- of everything downstream of the air source for a condition nobody can
-- fix retroactively; the clean-layer tests remain hard contracts.

{{ config(severity='warn') }}

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
