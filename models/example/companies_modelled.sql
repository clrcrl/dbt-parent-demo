with companies as (
  select
  *
  from {{schema}}.companies
)

, companies_with_top_parent_id as (
{{parent_details(
      table = "{{schema}}.companies",
      entity_id_column_name = "company_id",
      parent_id_column_name = "parent_company_id",
)}}
)

, top_parent_company_revenues as (
  select
  companies_with_top_parent_id.top_parent_company_id
  , count(companies.company_id) as number_of_companies -- note this include the parent company itself
  , sum(companies.revenue) as total_revenue
  from companies
  left join companies_with_top_parent_id on companies_with_top_parent_id.company_id = companies.company_id
  group by 1
)

select
*
from top_parent_company_revenues