{{
    config(
        schema='public',
        alias='data_freshness'
    )
}}

-- Data freshness monitoring — staleness status per station.
-- Replaces the hand-maintained public.data_freshness view from 020-views.sql.
-- Thresholds come from dbt vars (dbt_project.yml) so the classification is
-- the single source of truth shared by the Grafana WeatherStationStale alert
-- (fires on staleness > freshness_offline_max) and the vlc CLI `status`
-- command. staleness_s is provided so consumers never re-derive epoch math.

select
    'air' as schema_name,
    fiwareid,
    max(ts) as last_update,
    now() - max(ts) as staleness,
    extract(epoch from now() - max(ts)) as staleness_s,
    case
        when now() - max(ts) < interval '{{ var("freshness_fresh_max") }}' then 'Fresh'
        when now() - max(ts) < interval '{{ var("freshness_offline_max") }}' then 'Stale'
        else 'Offline'
    end as status
from {{ source('air', 'hyper') }}
group by fiwareid

union all

select
    'weather' as schema_name,
    fiwareid,
    max(ts) as last_update,
    now() - max(ts) as staleness,
    extract(epoch from now() - max(ts)) as staleness_s,
    case
        when now() - max(ts) < interval '{{ var("freshness_fresh_max") }}' then 'Fresh'
        when now() - max(ts) < interval '{{ var("freshness_offline_max") }}' then 'Stale'
        else 'Offline'
    end as status
from {{ source('weather', 'hyper') }}
group by fiwareid
