{# The following are all macros that I developed in order to write my parent_details macros, however they are not specific to solving this problem, and could be used in other macros #}

{% macro get_results_size(sql) %}
{# This macro gets the size (number of records) of a result of a query. It could be useful in other (unrelated) macros #}
    {% call statement('get_results_size', fetch_result=True) %}
        with results as (
          
          {{sql}}
          
        )
        select count(*) as size from results;

    {% endcall %}


    {%- set result = load_result('get_results_size') -%}

    {% if result is not none %}
        {%- set num_rows = result.data[0][0] %}
    {% else %}
        {% set num_rows = 0 %}
    {% endif %}
    {{ return(num_rows) }}

{% endmacro %}


{% macro create_table_and_get_size(table_name, sql) %}
{# This macro creates a new table, and returns the size of that table #}
    {# Create this table #}
    {% call statement('create_table', fetch_result=True) %}
        drop table if exists  {{ schema }}.{{ table_name }} cascade;
        create table {{ schema }}.{{ table_name }} as (

            {{ sql }}

        );

    {% endcall %}
    
    {# Get the size of the table created above #}
    {% set sql %}
    select * from {{ schema }}.{{ table_name }}
    {% endset %}
    {{return ( get_results_size(sql))}}
{% endmacro %}

{% macro insert_into_table_and_get_num_new_records(table_name, sql) %}
{# This macro inserts a query into an existing table, and returns the number of records that were inserted #}
    {# Insert into existing table #}
    {% call statement('insert_into_table', fetch_result=True) %}
        insert into {{ schema }}.{{ table_name }} (

            {{ sql }}

        );

    {% endcall %}
    
    {# Get the size of the records we just inserted (no need to reset {{sql}})#}
    {{return ( get_results_size(sql))}}
{% endmacro %}

{% macro union_iterative_tables(table_prefix, iterations) %}
{# This macro unions tables in the format "{{table_prefix}}{{iteration}}", e.g. "level0". It could be useful in other (unrelated) macros #}
  {% for i in range(iterations) -%}
  select *
  from {{table_prefix}}{{i}}
  {% if not loop.last %}
  union all
  {% endif %}
  {% endfor %}
{% endmacro %}