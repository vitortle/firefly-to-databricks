with transaction_journals as (
    select * from {{ source('firefly_raw', 'bz_transaction_journals') }}
),
transactions_ as (
    select * from {{ source('firefly_raw', 'bz_transactions') }}
),
transaction_types as (
    select * from {{ source('firefly_raw', 'bz_transaction_types') }}
),
category_transaction_journal as (
    select * from {{ source('firefly_raw', 'bz_category_transaction_journal') }}
),
renamed as (
SELECT
	tj.id journal_id,
	tj.created_at,
	tj.updated_at,
	tj.transaction_type_id,
	catj.category_id AS category_id,
	tj.description,
	tj.date as transaction_journal_datetime,  
	cast(tj.date as date) as transaction_journal_date,
	t.id as transaction_id,
	t.account_id,
	tj.transaction_group_id,
	concat("https://firefly.insidedatalab.com/transactions/show/", tj.transaction_group_id) url,
	t.amount
FROM
	transaction_journals tj
left join transactions_ t on
	t.transaction_journal_id = tj.id
left join transaction_types typ on 
	typ.id = tj.transaction_type_id
left join category_transaction_journal catj on 
	catj.transaction_journal_id = tj.id
WHERE 
	deleted_at IS NULL
order by
	date asc
)
select * from renamed