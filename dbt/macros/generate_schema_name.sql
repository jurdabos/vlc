{#
    Overriding default generate_schema_name so that a model's configured
    `schema` value is used verbatim (e.g. schema='air' → creates in the
    `air` schema, NOT `target_schema_air`).

    This is required because Grafana dashboards query views by their
    schema-qualified names (air.latest, weather.latest, etc.).
#}

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
