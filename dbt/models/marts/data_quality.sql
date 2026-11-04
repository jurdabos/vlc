{{
    config(
        schema='public',
        alias='data_quality'
    )
}}

-- Data-quality observability — how many readings the validity layer nulled,
-- per schema, station, month and flag ('metric:reason'). Raw violations are
-- an observable here, not a test failure: the raw hypertables keep the
-- glitches (immutable audit trail), the clean layer hides them, and this
-- mart shows where and how often sensors misbehave (`vlc quality`).
-- Only station-months with at least one flagged value appear.

with readings as (

    select 'weather' as schema_name, fiwareid, date_trunc('month', ts) as month, quality_flags
    from {{ ref('stg_weather__clean') }}

    union all

    select 'air' as schema_name, fiwareid, date_trunc('month', ts) as month, quality_flags
    from {{ ref('stg_air__clean') }}

),

totals as (

    select schema_name, fiwareid, month, count(*) as total_readings
    from readings
    group by schema_name, fiwareid, month

),

violations as (

    select schema_name, fiwareid, month, flag, count(*) as flagged_count
    from readings
    cross join lateral unnest(quality_flags) as flag
    group by schema_name, fiwareid, month, flag

)

select
    v.schema_name,
    v.fiwareid,
    v.month::date as month,
    v.flag,
    v.flagged_count,
    t.total_readings,
    round(100.0 * v.flagged_count / t.total_readings, 3) as flagged_pct
from violations v
join totals t using (schema_name, fiwareid, month)
