{{ config(materialized='table') }}

select
  country_entity_key,
  country_code,
  country_name,
  entity_code,
  entity_name,
  region
from {{ ref('stg_dim_country_entity') }}
