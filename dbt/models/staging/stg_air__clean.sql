{{
    config(
        schema='air',
        alias='clean'
    )
}}

-- Validity layer over air.hyper. The raw hypertable stays immutable; this
-- view NULLS (never drops) physically impossible pollutant readings per
-- column and records what was nulled in quality_flags ('metric:reason').
-- Bounds come from air_validity_bounds in dbt_project.yml. No spike rule
-- yet: pollutant concentrations can legitimately jump within an hour, so
-- only static bounds apply. Downstream models must read from here.

{% set bounds = var("air_validity_bounds") %}

select
    fiwareid,
    ts,
    {% for metric in bounds %}
    case when {{ out_of_bounds(metric, bounds[metric]) }} then null else {{ metric }} end as {{ metric }},
    {% endfor %}
    air_quality_summary,
    lat,
    lon,
    geo,
    array_remove(array[
        {% for metric in bounds %}
        case when {{ out_of_bounds(metric, bounds[metric]) }} then '{{ metric }}:out_of_bounds' end{% if not loop.last %},{% endif %}
        {% endfor %}
    ], null) as quality_flags
from {{ source('air', 'hyper') }}
