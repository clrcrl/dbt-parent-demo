-- need to replace "analytics_claire" with target
-- come back and put some better examples in, such as calulating number of direct and total descendants, and rolled up revenue

{{
    parent_details_create_level_tables(table = "analytics_claire.companies",
                            entity_id_column_name = "company_id",
                            parent_id_column_name = "parent_company_id")}}

