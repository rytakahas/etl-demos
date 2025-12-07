{% macro stg_generic(source_name, table_name, column_map) %}
with src as (
  select * from {{ source(source_name, table_name) }}
),
renamed as (
  select
    {% for alias, expr in column_map.items() %}
      {{ expr }} as {{ alias }}{% if not loop.last %},{% endif %}
    {% endfor %}
  from src
)
select * from renamed
{% endmacro %}
