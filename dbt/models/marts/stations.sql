{{
    config(
        schema='public',
        alias='stations'
    )
}}

-- Station inventory — first/last seen, record counts, avg coordinates.
-- Replaces the hand-maintained public.stations view from 020-views.sql.

select
    fiwareid,
    'air' as data_type,
    min(ts) as first_seen,
    max(ts) as last_seen,
    count(*) as record_count,
    avg(lat) as lat,
    avg(lon) as lon
from {{ source('air', 'hyper') }}
group by fiwareid

union all

select
    fiwareid,
    'weather' as data_type,
    min(ts) as first_seen,
    max(ts) as last_seen,
    count(*) as record_count,
    avg(lat) as lat,
    avg(lon) as lon
from {{ source('weather', 'hyper') }}
group by fiwareid
