with final as (
    select * from {{ ref("stg_categories") }}
)
select * from final