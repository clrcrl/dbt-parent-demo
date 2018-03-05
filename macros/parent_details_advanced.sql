{% macro parent_details_advanced(table, entity_id_column_name, parent_id_column_name, max_levels = 10, detail_column_names = []) %}

-- for each level, find the relevant companies and insert them into results table
{% for i in range(max_levels) -%}

-- for the top parent (/non-children), select from underlying table, and simply create the results table from it
-- QUESTION: When I run this, it gets nested inside of another create statement. How do I get around this?

{% if loop.first %}
-- drop statement since this table might already exist
drop table if exists "analytics_claire"."companies_modelled__dbt_tmp" ;
create table "analytics_claire"."companies_modelled__dbt_tmp" as (
  WITH entities AS (
    SELECT
    *
    FROM {{table}}
  )
  SELECT
    {{i}} AS level
    , entities."{{entity_id_column_name}}"
    , entities."{{parent_id_column_name}}"
    {%- for col in detail_column_names %}
    , entities."{{ col }}"
    {%- endfor %}
    , entities."{{entity_id_column_name}}" AS top_parent_{{entity_id_column_name}}
  FROM entities
  WHERE {{parent_id_column_name}} IS NULL
);


-- for any children, use the existing table to build a temporary table with the next level of results
{% else %}
-- drop statement since this table might already exist
drop table "analytics_claire"."companies_modelled__single_level" ;
create table "analytics_claire"."companies_modelled__single_level" as (
  WITH entities AS (
    SELECT
    *
    FROM {{table}}
  )
  , level_{{i-1}} AS (
    SELECT
    *
    FROM "analytics_claire"."companies_modelled__dbt_tmp"
    WHERE level = {{i-0}}
  )
  
  SELECT
    {{i}} AS level
    , entities."{{entity_id_column_name}}"
    , entities."{{parent_id_column_name}}"
    {%- for col in detail_column_names %}
    , entities."{{ col }}"
    {%- endfor %}
    {% if loop.first %}
    , entities."{{entity_id_column_name}}" AS top_parent_{{entity_id_column_name}}
    {% else %}
    , parent_entities."top_{{parent_id_column_name}}"
    {% endif %}
  FROM entities
  INNER JOIN level_{{i-1}} AS parent_entities ON entities.{{parent_id_column_name}} = parent_entities.{{entity_id_column_name}}
  
)


{% endif %}

-- return the number of rows

{%- call statement('num_rows', fetch_result=True) -%}

    SELECT
    COUNT(*)
    FROM "analytics_claire"."companies_modelled__single_level"

{%- endcall -%}

{%- set num_rows = load_result('num_rows')['data'] | map(attribute=0)-%}
-- QUESTION: How do I return this as an integer instead of an array?
-- If there's no results - exit the loop
    {% if num_rows == 0 %}
    -- {# break #}  -- Need to import loop controls extension http://jinja.pocoo.org/docs/2.9/extensions/#loop-controls
    -- Otherwise insert the results into the results table
    {% else %}
    INSERT INTO "analytics_claire"."companies_modelled__dbt_tmp" (
      SELECT * FROM "analytics_claire"."companies_modelled__single_level"
    )
    
    {% endif %}

  
)


{% endfor %}


{% endmacro %}