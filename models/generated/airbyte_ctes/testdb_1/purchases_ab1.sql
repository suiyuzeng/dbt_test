{{ config(
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_testdb_1",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('testdb_1', '_airbyte_raw_purchases') }}
select
    {{ json_extract_scalar('_airbyte_data', ['id'], ['id']) }} as id,
    {{ json_extract_scalar('_airbyte_data', ['user_id'], ['user_id']) }} as user_id,
    {{ json_extract_scalar('_airbyte_data', ['product_id'], ['product_id']) }} as product_id,
    {{ json_extract_scalar('_airbyte_data', ['returned_at'], ['returned_at']) }} as returned_at,
    {{ json_extract_scalar('_airbyte_data', ['purchased_at'], ['purchased_at']) }} as purchased_at,
    {{ json_extract_scalar('_airbyte_data', ['added_to_cart_at'], ['added_to_cart_at']) }} as added_to_cart_at,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('testdb_1', '_airbyte_raw_purchases') }} as table_alias
-- purchases
where 1 = 1

