{{ config(materialized='view') }}

select
  cast(vehicle_key as int64) as vehicle_key,
  cast(model_code as string) as model_code,
  cast(model_name as string) as model_name,
  cast(body_type as string) as body_type,
  upper(cast(fuel_type as string)) as fuel_type,
  cast(segment as string) as segment,
  cast(msrp as numeric) as msrp
from {{ source('raw', 'dim_vehicle') }}
where vehicle_key is not null
