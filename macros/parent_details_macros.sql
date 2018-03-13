{#
  OPTION 0 (simplest): Generate arbitrarily long SQL to be executed
  1. Generate sql for levels to an arbitrary depth as CTEs
  2. Union the CTEs together
  
  Pros:
  - Super simple (may actually be the best)
  
  Cons:
  - Risk of not iterating through all records if max_levels is not large enough
#}
{% macro parent_details(table, entity_id_column_name, parent_id_column_name, max_levels = 10) %}

{{get_sql_for_levels_as_ctes(table, entity_id_column_name, parent_id_column_name, max_levels)}}


{{union_iterative_tables('level', max_levels)}}

{% endmacro %}

{#
  OPTION 1: Create a table for each level
  0. Create table "level0" with all records for "top level" rows (there is no parent)
  1. Check how many records are in "level0" - if more than 1, continue
  2. Create table "level1" with all records that have a parent in "level0"
      i.e. inner join "level0" on "level1".parent_id = "level0".id
  3. Check how many records are in "level1" - if more than 1, continue
  2. Create table "level2" with all records that have a parent in "level1"
      i.e. inner join "level1" on "level2".parent_id = "level1".id
  3. Check how many records are in "level2" - if more than 1, continue
  ...
  4. Stop when the resulting table does not contain any records, or max levels is reached
  5. union all tables together
  
  Pros:
  - Fairly simple conceptually
  - Won't miss records (so long as max_levels is appropriately high)
  
  Cons:
  - End up with lots of tables (one for each level) in your target schema. Note that we cannot create temp tables in case this gets materialised as a view (the view will need to reference the underlying tables)
  
  Notes:
  - Jinja can't do `while` loops properly - have to hack it with an `if` function
  - Variable assignment within a loop doesn't update the variable outside the loop :(
#}

{% macro parent_details_create_level_tables(table, entity_id_column_name, parent_id_column_name, max_levels = 10, target_table = 'level') %}
    {% set global_vars = {'num_rows': 0, 'depth': 0} %}

    {% for i in range(max_levels) %}
        
        
        {% if loop.first or global_vars.num_rows  > 0 %}
            {% set sql = get_sql_for_level_create_macro(table, entity_id_column_name, parent_id_column_name, i, target_table) %}
            {% set num_rows = create_table_and_get_size(target_table ~ i, sql) %}
            {% if global_vars.update ( {'num_rows': num_rows, 'depth': i}) %} {% endif %}
              {% if global_vars.num_rows > 0 %}
              {{ log("LOOP " ~ i ~ " NUM ROWS IS " ~ global_vars.num_rows, info=True) }}
              {% else %}
              {{ log("No more children to loop through. Maximum depth is " ~ (global_vars.depth| int -1 ), info=True) }}
              {% endif %}
        {% endif %}
    {% endfor %}
    {{union_iterative_tables('{{schema}}.level', global_vars.depth)}}

{% endmacro %}



{#
  OPTION 2: Create a table and insert each level into it
  0. Create table with all records for "top level" rows (there is no parent)
  1. Check how many records are in "level0" - if more than 1, continue
  2. Run *query* for "level1" with all records that have a parent in "level0"
      i.e. inner join "level0" on "level1".parent_id = "level0".id
  3. Check how many records are in "level1" - if more than 1 *insert* records into table created in step 0 and continue
  2. Create table "level2" with all records that have a parent in "level1"
      i.e. inner join "level1" on "level2".parent_id = "level1".id
  3. Check how many records are in "level2" - if more than 1 *insert* records into table created in step 0 and continue
  ...
  4. Stop when the resulting query does not contain any records, or max levels is reached
  
  Pros:
  - Only one table left behind in schema
  - Won't miss records (so long as max_levels is appropriately high)
  
  Cons:
  - Insert statement can get tricky
  
  Notes:
  - as above
#}

{% macro parent_details_insert_into_table(table, entity_id_column_name, parent_id_column_name, max_levels = 10, target_table = 'levels') %}
    {% set global_vars = {'num_rows': 0, 'depth': 0} %}

    {% for i in range(max_levels) %}
        
        
        {% if loop.first or global_vars.num_rows  > 0 %}
          {% if loop.first %}
            {% set sql = get_sql_for_level_insert_macro(table, entity_id_column_name, parent_id_column_name, i, target_table) %}
            {% set num_rows = create_table_and_get_size(target_table, sql) %}

          {% else %}
            {% set sql = get_sql_for_level_insert_macro(table, entity_id_column_name, parent_id_column_name, i, target_table) %}

            {# get size #}
            {% set num_rows = insert_into_table_and_get_num_new_records(target_table, sql) %}
          
          {% endif %}
              {% if global_vars.update ( {'num_rows': num_rows, 'depth': i}) %} {% endif %}
              {{ log("LOOP " ~ i ~ " NUM ROWS IS " ~ global_vars.num_rows, info=True) }}
          
          
        {% endif %}
    {% endfor %}
    {{union_iterative_tables('{{schema}}.level', global_vars.depth)}}

{% endmacro %}

{#
  OPTION 4: Generate sql for n levels, until n+1 levels has no records
  0. Run the query from Option 0 for max_levels = 1
  1. Check number of records in most recent level (level = 0). If there is more than 1 record in level = 0, continue
  2. Run the query from Option 0 for max_levels = 2
  4. Check number of records in most recent level (level = 0). If there is more than 1 record in level = 1, continue
  ...
  4. Stop when the resulting query gets to a number of levels where there are no further records being added, or max levels is reached
  5. Return this sql as the sql to be generated
  
  Pros:
  - No tables need to be materialised
  - Won't miss records (so long as max_levels is appropriately high)
  
  Cons:
  - Running _a lot_ of queries
  
  Notes:
  - as above
#}

{% macro parent_details_generate_sql(table, entity_id_column_name, parent_id_column_name, max_levels = 10) %}
    {% set global_vars = {'num_rows': num_rows, 'depth': 0} %}

    {% for i in range(max_levels) %}
        
        
        {% if loop.first or global_vars.num_rows > 0 %}
            {% set sql %}
            {{get_sql_for_levels_as_ctes(table, entity_id_column_name, parent_id_column_name, i) }}
            select * from level{{i}}
 
            {% endset %}
            {% set num_rows = get_results_size(sql) %}
            {% if global_vars.update ( {'num_rows': num_rows, 'depth': i}) %} {% endif %}
            
            

        {% endif %}
    {% endfor %}
    
    {# Now actually call the function to generate the sql#}
    {{parent_details(table, entity_id_column_name, parent_id_column_name, global_vars.depth)}}
    
{% endmacro %}