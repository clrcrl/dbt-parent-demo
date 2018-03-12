/*
-- option 0
{{parent_details(
    table = "{{schema}}.male_family_tree",
    entity_id_column_name = "id",
    parent_id_column_name = "parent_id")}}


-- option 1
    
{{parent_details_create_level_tables(
    table = "{{schema}}.male_family_tree",
    entity_id_column_name = "id",
    parent_id_column_name = "parent_id",
    target_table = "family_tree_level")}}

    
-- option 2
{{parent_details_insert_into_table(
    table = "{{schema}}.male_family_tree",
    entity_id_column_name = "id",
    parent_id_column_name = "parent_id",
    target_table = "family_tree_levels")}}
*/  
-- option 3
{{parent_details_generate_sql(
    table = "{{schema}}.male_family_tree",
    entity_id_column_name = "id",
    parent_id_column_name = "parent_id")}}
/*
*/