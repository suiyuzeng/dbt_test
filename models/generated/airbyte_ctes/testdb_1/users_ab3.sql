{{ config(
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_testdb_1",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('users_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        'id',
        'age',
        adapter.quote('name'),
        'email',
        'title',
        'gender',
        'height',
        'weight',
        adapter.quote('language'),
        'telephone',
        'blood_type',
        'created_at',
        'occupation',
        'updated_at',
        'nationality',
        'academic_degree',
    ]) }} as _airbyte_users_hashid,
    tmp.*
from {{ ref('users_ab2') }} tmp
-- users
where 1 = 1

