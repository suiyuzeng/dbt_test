{{ config(
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_testdb_1",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('products_ab1') }}
select
    cast(id as {{ dbt_utils.type_float() }}) as id,
    cast(make as {{ dbt_utils.type_string() }}(1024)) as make,
    cast({{ adapter.quote('year') }} as {{ dbt_utils.type_string() }}(1024)) as {{ adapter.quote('year') }},
    cast(model as {{ dbt_utils.type_string() }}(1024)) as model,
    cast(price as {{ dbt_utils.type_float() }}) as price,
    cast({{ empty_string_to_null('created_at') }} as {{ type_timestamp_with_timezone() }}) as created_at,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('products_ab1') }}
-- products
where 1 = 1

