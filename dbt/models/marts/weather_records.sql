{{
    config(
        schema='public',
        alias='weather_records'
    )
}}

-- Records of records — the row holding each meteorological extreme
-- (max/min of temperature, humidity, pressure, wind speed; max of
-- per-interval precipitation). Ties are resolved by the earliest
-- timestamp. Promoted from the ad-hoc block in db/direct_sql_queries.txt
-- so the CLI (`vlc records`) and any future dashboard panel read one
-- definition. Reads the validity layer, so sensor glitches (e.g. the
-- -7.8 degC spike of 2026-05-15) cannot masquerade as records.

{% set extremes = [
    ("max_temperature_c", "temperature_c", "desc"),
    ("min_temperature_c", "temperature_c", "asc"),
    ("max_humidity_pct", "humidity_pct", "desc"),
    ("min_humidity_pct", "humidity_pct", "asc"),
    ("max_pressure_hpa", "pressure_hpa", "desc"),
    ("min_pressure_hpa", "pressure_hpa", "asc"),
    ("max_wind_speed_ms", "wind_speed_ms", "desc"),
    ("min_wind_speed_ms", "wind_speed_ms", "asc"),
    ("max_precip_mm", "precip_mm", "desc"),
] %}

{% for record_type, column, direction in extremes %}
(
    select
        '{{ record_type }}' as record_type,
        temperature_c,
        humidity_pct,
        pressure_hpa,
        wind_speed_ms,
        precip_mm,
        wind_dir_deg,
        fiwareid,
        ts
    from {{ ref('stg_weather__clean') }}
    where {{ column }} is not null
    order by {{ column }} {{ direction }}, ts asc
    limit 1
)
{% if not loop.last %}
union all
{% endif %}
{% endfor %}
