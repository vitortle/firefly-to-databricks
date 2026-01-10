with source as (
    select * from {{ source('firefly_raw', 'bz_accounts') }}
),
renamed as (
    SELECT
    a.id AS account_id,
    a.name AS account_name,
    at.type AS account_type,
    a.active AS is_active,
    a.created_at,
    a.updated_at
FROM firefly.raw.bz_accounts a
 LEFT JOIN firefly.raw.bz_account_types at on 
   a.account_type_id = at.id
WHERE deleted_at IS NULL
)
select * from renamed