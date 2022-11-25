{{ config(
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_testdb_1",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('purchases_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        'id',
        'user_id',
        'product_id',
        'returned_at',
        'purchased_at',
        'added_to_cart_at',
    ]) }} as _airbyte_purchases_hashid,
    tmp.*
from {{ ref('purchases_ab2') }} tmp
-- purchases
where 1 = 1

