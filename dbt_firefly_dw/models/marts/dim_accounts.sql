with final as (
    select * from {{ ref("stg_accounts") }}
)
select * from final