{#
    Grants SELECT on every dbt-managed view to the vlc_dev role.
    Called as a post-hook in dbt_project.yml on-run-end.
#}

{% macro grant_select_to_vlc_dev() %}
    {% if execute %}
        {% for node in graph.nodes.values() if node.resource_type == 'model' %}
            {% set relation = adapter.get_relation(
                database=node.database,
                schema=node.schema,
                identifier=node.alias
            ) %}
            {% if relation %}
                {% do run_query("GRANT SELECT ON " ~ relation ~ " TO vlc_dev;") %}
            {% endif %}
        {% endfor %}
    {% endif %}
{% endmacro %}
