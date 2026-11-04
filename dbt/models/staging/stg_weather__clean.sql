{{
    config(
        schema='weather',
        alias='clean'
    )
}}

-- Validity layer over weather.hyper. The raw hypertable stays immutable;
-- this view NULLS (never drops) physically impossible measurements per
-- column and records what was nulled in quality_flags ('metric:reason').
-- Two detectors, both parametrized in dbt_project.yml:
--   * static bounds (weather_validity_bounds) — catches e.g. 107 % humidity
--     or 342 hPa pressure;
--   * spike rules (weather_spike_rules; temperature and pressure) — catch
--     single-reading glitches that sit inside the physical bounds, e.g.
--     the -7.8 degC reading at W02_NAZARET_10m on 2026-05-15 (19.6 degC
--     ten minutes earlier) or the 949 hPa excursion of 2026-05-19.
--     A reading is a spike when it deviates > max_delta from the previous
--     reading (within window_minutes) and the next reading either
--     disagrees with it too (V-shape) or is missing/outside the window.
--     Neighbours are compared on RAW values on purpose: a reading adjacent
--     to bounds-garbage still gets V-shape scrutiny.
-- Downstream models (latest, records, snapshot) must read from here.

{% set bounds = var("weather_validity_bounds") %}
{% set spikes = var("weather_spike_rules") %}

with neighbors as (

    select
        *,
        {% for metric in spikes %}
        lag({{ metric }}) over w as prev_{{ metric }},
        lead({{ metric }}) over w as next_{{ metric }},
        {% endfor %}
        ts - lag(ts) over w as prev_gap,
        lead(ts) over w - ts as next_gap
    from {{ source('weather', 'hyper') }}
    window w as (partition by fiwareid order by ts)

),

flagged as (

    select
        *,
        {% for metric in bounds %}
        {{ out_of_bounds(metric, bounds[metric]) }} as {{ metric }}_oob,
        {% endfor %}
        {% for metric, rule in spikes.items() %}
        (
            prev_{{ metric }} is not null
            and prev_gap <= interval '{{ rule["window_minutes"] }} minutes'
            and abs({{ metric }} - prev_{{ metric }}) > {{ rule["max_delta"] }}
            and (
                next_{{ metric }} is null
                or next_gap > interval '{{ rule["window_minutes"] }} minutes'
                or abs({{ metric }} - next_{{ metric }}) > {{ rule["max_delta"] }}
            )
        ) as {{ metric }}_spike{% if not loop.last %},{% endif %}
        {% endfor %}
    from neighbors

)

select
    fiwareid,
    ts,
    case when temperature_c_oob or temperature_c_spike then null else temperature_c end as temperature_c,
    case when humidity_pct_oob then null else humidity_pct end as humidity_pct,
    case when pressure_hpa_oob or pressure_hpa_spike then null else pressure_hpa end as pressure_hpa,
    case when wind_dir_deg_oob then null else wind_dir_deg end as wind_dir_deg,
    case when wind_speed_ms_oob then null else wind_speed_ms end as wind_speed_ms,
    case when precip_mm_oob then null else precip_mm end as precip_mm,
    lat,
    lon,
    geo,
    array_remove(array[
        case when temperature_c_oob then 'temperature_c:out_of_bounds' end,
        case when temperature_c_spike then 'temperature_c:spike' end,
        case when humidity_pct_oob then 'humidity_pct:out_of_bounds' end,
        case when pressure_hpa_oob then 'pressure_hpa:out_of_bounds' end,
        case when pressure_hpa_spike then 'pressure_hpa:spike' end,
        case when wind_dir_deg_oob then 'wind_dir_deg:out_of_bounds' end,
        case when wind_speed_ms_oob then 'wind_speed_ms:out_of_bounds' end,
        case when precip_mm_oob then 'precip_mm:out_of_bounds' end
    ], null) as quality_flags
from flagged
