with s_bz_accounts as (
    select * from {{ source('firefly_raw', 'bz_accounts') }}
),
s_bz_account_types as (
    select * from {{ source('firefly_raw', 'bz_account_types') }}
),
renamed as (
    SELECT
    a.id AS account_id,
    a.name AS account_name,
    at.type AS account_type,
    a.active AS is_active,
    a.created_at,
    a.updated_at
FROM s_bz_accounts a
 LEFT JOIN s_bz_account_types at on 
   a.account_type_id = at.id
WHERE deleted_at IS NULL
)
select * from renamed