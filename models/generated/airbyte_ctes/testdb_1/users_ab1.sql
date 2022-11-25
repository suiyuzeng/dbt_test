{{ config(
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_testdb_1",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('testdb_1', '_airbyte_raw_users') }}
select
    {{ json_extract_scalar('_airbyte_data', ['id'], ['id']) }} as id,
    {{ json_extract_scalar('_airbyte_data', ['age'], ['age']) }} as age,
    {{ json_extract_scalar('_airbyte_data', ['name'], ['name']) }} as {{ adapter.quote('name') }},
    {{ json_extract_scalar('_airbyte_data', ['email'], ['email']) }} as email,
    {{ json_extract_scalar('_airbyte_data', ['title'], ['title']) }} as title,
    {{ json_extract_scalar('_airbyte_data', ['gender'], ['gender']) }} as gender,
    {{ json_extract_scalar('_airbyte_data', ['height'], ['height']) }} as height,
    {{ json_extract_scalar('_airbyte_data', ['weight'], ['weight']) }} as weight,
    {{ json_extract_scalar('_airbyte_data', ['language'], ['language']) }} as {{ adapter.quote('language') }},
    {{ json_extract_scalar('_airbyte_data', ['telephone'], ['telephone']) }} as telephone,
    {{ json_extract_scalar('_airbyte_data', ['blood_type'], ['blood_type']) }} as blood_type,
    {{ json_extract_scalar('_airbyte_data', ['created_at'], ['created_at']) }} as created_at,
    {{ json_extract_scalar('_airbyte_data', ['occupation'], ['occupation']) }} as occupation,
    {{ json_extract_scalar('_airbyte_data', ['updated_at'], ['updated_at']) }} as updated_at,
    {{ json_extract_scalar('_airbyte_data', ['nationality'], ['nationality']) }} as nationality,
    {{ json_extract_scalar('_airbyte_data', ['academic_degree'], ['academic_degree']) }} as academic_degree,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('testdb_1', '_airbyte_raw_users') }} as table_alias
-- users
where 1 = 1

