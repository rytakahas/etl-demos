{{ config(materialized='table') }}

select
  vehicle_key,
  model_code,
  model_name,
  body_type,
  fuel_type,
  segment,
  msrp
from {{ ref('stg_dim_vehicle') }}
