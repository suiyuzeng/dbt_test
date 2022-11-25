{{ config(
    unique_key = '_airbyte_ab_id',
    schema = "testdb_1",
    post_hook = ["
                    {%
                        set scd_table_relation = adapter.get_relation(
                            database=this.database,
                            schema=this.schema,
                            identifier='purchases_scd'
                        )
                    %}
                    {%
                        if scd_table_relation is not none
                    %}
                    {%
                            do adapter.drop_relation(scd_table_relation)
                    %}
                    {% endif %}
                        "],
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('purchases_ab3') }}
select
    id,
    user_id,
    product_id,
    returned_at,
    purchased_at,
    added_to_cart_at,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_purchases_hashid
from {{ ref('purchases_ab3') }}
-- purchases from {{ source('testdb_1', '_airbyte_raw_purchases') }}
where 1 = 1

