-- Failing rows: measurements in the CLEAN layer outside the validity
-- bounds (weather_validity_bounds in dbt_project.yml). The clean layer's
-- contract is to null such values, so any row here is a bug in
-- stg_weather__clean. Raw-layer violations are expected and observable
-- via the data_quality mart instead.

{% set bounds = var("weather_validity_bounds") %}

select *
from {{ ref('stg_weather__clean') }}
where
{% for metric in bounds %}
    {{ out_of_bounds(metric, bounds[metric]) }}{% if not loop.last %} or{% endif %}
{% endfor %}
