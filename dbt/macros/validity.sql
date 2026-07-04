{% macro out_of_bounds(column, bounds) %}
{#- Renders a predicate that is true when *column* holds a physically
    impossible value according to *bounds* (a {min, max} mapping from the
    *_validity_bounds vars in dbt_project.yml). NULLs are not violations. -#}
({{ column }} is not null and ({{ column }} < {{ bounds["min"] }} or {{ column }} > {{ bounds["max"] }}))
{% endmacro %}
