with fact as ( select * from {{ ref('fct_transactions') }} ),
dim_account as ( select * from {{ ref('dim_accounts') }} ),
dim_transaction_type as ( select * from {{ ref('dim_transaction_types') }} ),
dim_categories as ( select * from {{ ref('dim_categories') }} )
SELECT
	t.journal_id
	, t.description 
	, t.transaction_journal_date AS date
	, t.transaction_id
	, date_format(t.transaction_journal_date, 'yyyyMM') AS year_month_date
	, a.account_name 
	, a.account_type 
	, tt.transaction_type_name 
	, c.category_name 
	, t.amount * -1 as amount
FROM
	fact t
LEFT JOIN dim_account a ON 
	t.account_id = a.account_id
LEFT JOIN dim_transaction_type tt ON 
	tt.transaction_types_id = t.transaction_type_id 
LEFT JOIN dim_categories c ON 
	c.categories_id = t.category_id 
WHERE 
	tt.transaction_type_name = 'Withdrawal'
	AND a.account_type = 'Asset account'
ORDER BY t.transaction_journal_date DESC