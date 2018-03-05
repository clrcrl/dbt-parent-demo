{% macro parent_details(table, entity_id_column_name, parent_id_column_name, max_levels = 10, detail_column_names = []) %}

with entities as (
  select
  *
  from {{table}}
)

{% for i in range(max_levels) -%}

, level_{{i}} as (

    select
      {{i}} as level, 
      entities."{{entity_id_column_name}}",
      entities."{{parent_id_column_name}}",
      {%- for col in detail_column_names %}
      entities."{{ col }}",
      {%- endfor %}
      {% if loop.first %}
      entities."{{entity_id_column_name}}" as top_parent_{{entity_id_column_name}}
      {% else %}
      parent_entities."top_{{parent_id_column_name}}"
      {% endif %}
    from entities
    {% if loop.first %}
    where {{parent_id_column_name}} is null
    {% else %}
    inner join level_{{i-1}} as parent_entities on entities.{{parent_id_column_name}} = parent_entities.{{entity_id_column_name}}
    {% endif %}
)


{% endfor %}


{% for i in range(max_levels) -%}
select *
from level_{{i}}
{% if not loop.last %}
union all
{% endif %}
{% endfor %}

order by top_{{parent_id_column_name}}, level

{% endmacro %}