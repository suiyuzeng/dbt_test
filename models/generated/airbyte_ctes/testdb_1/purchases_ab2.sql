{{ config(
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_testdb_1",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('purchases_ab1') }}
select
    cast(id as {{ dbt_utils.type_float() }}) as id,
    cast(user_id as {{ dbt_utils.type_float() }}) as user_id,
    cast(product_id as {{ dbt_utils.type_float() }}) as product_id,
    cast({{ empty_string_to_null('returned_at') }} as {{ type_timestamp_with_timezone() }}) as returned_at,
    cast({{ empty_string_to_null('purchased_at') }} as {{ type_timestamp_with_timezone() }}) as purchased_at,
    cast({{ empty_string_to_null('added_to_cart_at') }} as {{ type_timestamp_with_timezone() }}) as added_to_cart_at,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('purchases_ab1') }}
-- purchases
where 1 = 1

