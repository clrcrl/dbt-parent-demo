{# The following macros generate sql used in the parent_detail macros. They can probably be cleaned up as they are fairly repetitious at the moment #}

{% macro get_sql_for_levels_as_ctes(table, entity_id_column_name, parent_id_column_name, level) %}
{# This macro generates the sql for the levels as ctes - it does not generate a complete sql statement, so needs to be run with a select statement after it #}
 
    with entities as (
      select
        {{entity_id_column_name}}
        , {{parent_id_column_name}}
      from {{table}}
    )
    {% for i in range(level+1) -%}
    , level{{i}} as (

        select
          {{i}} as level, 
          entities.{{entity_id_column_name}},
          entities.{{parent_id_column_name}},
          {% if loop.first %}
          entities.{{entity_id_column_name}} as top_{{parent_id_column_name}}
          {% else %}
          parent_entities.top_{{parent_id_column_name}}
          {% endif %}
        from entities
        {% if loop.first %}
        where entities.{{parent_id_column_name}} is null
        {% else %}
        inner join level{{i-1}} as parent_entities on entities.{{parent_id_column_name}} = parent_entities.{{entity_id_column_name}}
        {% endif %}
    )

    {% endfor %}

{% endmacro %}

{% macro get_sql_for_level_create_macro(table, entity_id_column_name, parent_id_column_name, level, target_table) %}
{# This macro generates the sql for a particular by referencing existing individual tables for each level. It is only used in the "create" macro, i.e. where a table for each level is created#}
    with entities as (
      select
        {{entity_id_column_name}}
        , {{parent_id_column_name}}
      from {{table}}
    )
    
    {% if level>0 %}
    , level{{level-1}} as (
      select
        *
      from  {{ schema }}.{{target_table}}{{level-1}}
    )
    {% endif %}

    select
      {{level}} as level, 
      entities.{{entity_id_column_name}},
      entities.{{parent_id_column_name}},
      {% if level==0 %}
      entities.{{entity_id_column_name}} as top_{{parent_id_column_name}}
      {% else %}
      parent_entities.top_{{parent_id_column_name}}
      {% endif %}
    from entities
    {% if level==0 %}
    where entities.{{parent_id_column_name}} is null
    {% else %}
    inner join level{{level-1}} as parent_entities on entities.{{parent_id_column_name}} = parent_entities.{{entity_id_column_name}}
    {% endif %}

{% endmacro %}


{% macro get_sql_for_level_insert_macro(table, entity_id_column_name, parent_id_column_name, level, target_table) %}
{# This macro generates the sql for a particular by referencing a table with all previous levels in it. It is only used in the "insert" macro, i.e. where a table for each level is inserted into a main table #}
    with entities as (
      select
        {{entity_id_column_name}}
        , {{parent_id_column_name}}
      from {{table}}
    )
    
    {% if level>0 %}
    , level{{level-1}} as (
      select
        *
      from  {{ schema }}.{{target_table}}
      where level = {{level-1}}
    )
    {% endif %}

    select
      {{level}} as level, 
      entities.{{entity_id_column_name}},
      entities.{{parent_id_column_name}},
      {% if level==0 %}
      entities.{{entity_id_column_name}} as top_{{parent_id_column_name}}
      {% else %}
      parent_entities.top_{{parent_id_column_name}}
      {% endif %}
    from entities
    {% if level==0 %}
    where entities.{{parent_id_column_name}} is null
    {% else %}
    inner join level{{level-1}} as parent_entities on entities.{{parent_id_column_name}} = parent_entities.{{entity_id_column_name}}
    {% endif %}

{% endmacro %}