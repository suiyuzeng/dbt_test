{{ config(
    unique_key = '_airbyte_ab_id',
    schema = "testdb_1",
    post_hook = ["
                    {%
                        set scd_table_relation = adapter.get_relation(
                            database=this.database,
                            schema=this.schema,
                            identifier='users_scd'
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
-- depends_on: {{ ref('users_ab3') }}
select
    id,
    age,
    {{ adapter.quote('name') }},
    email,
    title,
    gender,
    height,
    weight,
    {{ adapter.quote('language') }},
    telephone,
    blood_type,
    created_at,
    occupation,
    updated_at,
    nationality,
    academic_degree
from {{ ref('users_ab3') }}
-- users from {{ source('testdb_1', '_airbyte_raw_users') }}
where 1 = 1

