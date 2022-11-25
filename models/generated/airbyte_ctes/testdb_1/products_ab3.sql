{{ config(
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_testdb_1",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('products_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        'id',
        'make',
        adapter.quote('year'),
        'model',
        'price',
        'created_at',
    ]) }} as _airbyte_products_hashid,
    tmp.*
from {{ ref('products_ab2') }} tmp
-- products
where 1 = 1

