{{ config(
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_testdb_1",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('testdb_1', '_airbyte_raw_products') }}
select
    {{ json_extract_scalar('_airbyte_data', ['id'], ['id']) }} as id,
    {{ json_extract_scalar('_airbyte_data', ['make'], ['make']) }} as make,
    {{ json_extract_scalar('_airbyte_data', ['year'], ['year']) }} as {{ adapter.quote('year') }},
    {{ json_extract_scalar('_airbyte_data', ['model'], ['model']) }} as model,
    {{ json_extract_scalar('_airbyte_data', ['price'], ['price']) }} as price,
    {{ json_extract_scalar('_airbyte_data', ['created_at'], ['created_at']) }} as created_at,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('testdb_1', '_airbyte_raw_products') }} as table_alias
-- products
where 1 = 1

