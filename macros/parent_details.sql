{% macro parent_details(table, entity_id_column_name, parent_id_column_name, max_levels = 10) -%}
with entities as (
  select
  *
  from {{table}}
)

{% for i in range(max_levels) -%}

, level{{i}} as (

    select
      {{i}} as level, 
      entities.{{entity_id_column_name}},
      entities.{{parent_id_column_name}},
      {% if loop.first -%}
      entities.{{entity_id_column_name}} as top_{{parent_id_column_name}}
      {% else -%}
      parent_entities.top_{{parent_id_column_name}}
      {%- endif %}
    from entities
    {% if loop.first -%}
    where {{parent_id_column_name}} is null
    {% else -%}
    inner join level{{i-1}} as parent_entities on entities.{{parent_id_column_name}} = parent_entities.{{entity_id_column_name}}
    {% endif %}
)

{%- endfor %}

{% for i in range(max_levels) -%}
select *
from level{{i}}
{% if not loop.last %}
union all
{% endif %}
{% endfor %}

{%- endmacro %}