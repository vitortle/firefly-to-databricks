with transactions as (
    select * from {{ ref('stg_transactions') }}
),
final as (
    select 
        journal_id, 
        created_at, 
        updated_at, 
        transaction_type_id, 
        category_id, 
        description, 
        transaction_journal_datetime, 
        transaction_journal_date, 
        transaction_id, 
        account_id,
    	transaction_group_id,
		url,
        amount 
    from transactions
)
select * from final