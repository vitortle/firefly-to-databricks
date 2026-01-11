with final as (
    select * from {{ ref("stg_transaction_types") }}
)
select * from final