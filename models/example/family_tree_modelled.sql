-- need to replace "analytics_claire" with target
/*
-- option 0
{{parent_details(
    table = "analytics_claire.male_family_tree",
    entity_id_column_name = "id",
    parent_id_column_name = "parent_id")}}


-- option 1
    
{{parent_details_create_level_tables(
    table = "analytics_claire.male_family_tree",
    entity_id_column_name = "id",
    parent_id_column_name = "parent_id",
    target_table = "family_tree_level")}}

    
-- option 2
{{parent_details_insert_into_table(
    table = "analytics_claire.male_family_tree",
    entity_id_column_name = "id",
    parent_id_column_name = "parent_id",
    target_table = "family_tree_levels")}}
*/  
-- option 3
{{parent_details_generate_sql(
    table = "analytics_claire.male_family_tree",
    entity_id_column_name = "id",
    parent_id_column_name = "parent_id")}}
/*
*/