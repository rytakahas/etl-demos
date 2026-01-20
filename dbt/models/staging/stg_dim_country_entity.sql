{{ config(materialized='view') }}

select
  cast(country_entity_key as int64) as country_entity_key,
  upper(cast(country_code as string)) as country_code,
  cast(country_name as string) as country_name,
  cast(entity_code as string) as entity_code,
  cast(entity_name as string) as entity_name,
  cast(region as string) as region
from {{ source('raw', 'dim_country_entity') }}
where country_entity_key is not null
