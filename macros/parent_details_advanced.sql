
{# 
1. Select all records for "top level" rows (there is no parent)
2. Join source table to table from step 1 on 2.parent_id = 1.child_id
3. Join source table to table from step 2 on 3.parent_id = 2.child_id
...
4. Stop when the resulting table does not contain any records, or max levels is reached
5. union all tables together
#}

{% macro create_table_and_get_size(table_name, sql) %}

    {# Create this table #}
    {% call statement('level_create', fetch_result=True) %}

        -- TODO : drop this here if it exists
        create table {{ schema }}.{{ table_name }} as (

            {{ sql }}

        );

    {% endcall %}

    {# Get the size of the table created above #}
    {% call statement('level_get_size', fetch_result=True) %}

        select count(*) as size from {{ schema }}.{{ table_name }};

    {% endcall %}


    {%- set result = load_result('level_get_size') -%}

    {% if result is not none %}
        {%- set num_rows = result.data[0][0] %}
        {{ log("NUM ROWS: " ~ num_rows, info=True) }}
    {% else %}
        {% set num_rows = 0 %}
    {% endif %}

    {{ return(num_rows) }}

{% endmacro %}

{% macro get_sql_for_level(level, entity_id_column_name, parent_id_column_name, detail_column_names) %}


{% endmacro %}

{% macro parent_details_advanced(table, entity_id_column_name, parent_id_column_name, max_levels = 10, detail_column_names = []) %}

    {% set level0_sql %}
      select
          {{entity_id_column_name}} child_id
          , {{parent_id_column_name}} parent_id
          {%- for col in detail_column_names %}
              , {{ col }}
          {%- endfor %}

      from {{table}}
      where {{ parent_id_column_name }} is null
    {% endset %}

    {% set num_rows = create_table_and_get_size("level0", level0_sql) %}

    {% for i in range(1, max_levels) %}

        {{ log("LOOP " ~ i ~ " NUM ROWS IS " ~ num_rows, info=True) }}
        {% if num_rows > 0 %}

            {% set sql = get_sql_for_level(i, entity_id_column_name, parent_id_column_name, detail_column_names) %}
            {% set num_rows = create_table_and_get_size('level' ~ i, sql) %}

        {% endif %}
    {% endfor %}

{% endmacro %}
