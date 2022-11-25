{{ config(
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_testdb_1",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('users_ab1') }}
select
    cast(id as {{ dbt_utils.type_float() }}) as id,
    cast(age as {{ dbt_utils.type_bigint() }}) as age,
    cast({{ adapter.quote('name') }} as {{ dbt_utils.type_string() }}(1024)) as {{ adapter.quote('name') }},
    cast(email as {{ dbt_utils.type_string() }}(1024)) as email,
    cast(title as {{ dbt_utils.type_string() }}(1024)) as title,
    cast(gender as {{ dbt_utils.type_string() }}(1024)) as gender,
    cast(height as {{ dbt_utils.type_string() }}(1024)) as height,
    cast(weight as {{ dbt_utils.type_bigint() }}) as weight,
    cast({{ adapter.quote('language') }} as {{ dbt_utils.type_string() }}(1024)) as {{ adapter.quote('language') }},
    cast(telephone as {{ dbt_utils.type_string() }}(1024)) as telephone,
    cast(blood_type as {{ dbt_utils.type_string() }}(1024)) as blood_type,
    cast({{ empty_string_to_null('created_at') }} as {{ type_timestamp_with_timezone() }}) as created_at,
    cast(occupation as {{ dbt_utils.type_string() }}(1024)) as occupation,
    cast({{ empty_string_to_null('updated_at') }} as {{ type_timestamp_with_timezone() }}) as updated_at,
    cast(nationality as {{ dbt_utils.type_string() }}(1024)) as nationality,
    cast(academic_degree as {{ dbt_utils.type_string() }}(1024)) as academic_degree,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('users_ab1') }}
-- users
where 1 = 1

