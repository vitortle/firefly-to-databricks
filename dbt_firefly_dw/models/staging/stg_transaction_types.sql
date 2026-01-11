with source as (
    select * from {{ source('firefly_raw', 'bz_transaction_types') }}
),
renamed as (
SELECT
	id AS transaction_types_id,
	created_at,
	updated_at,
	deleted_at,
	`type` AS transaction_type_name
FROM
	source
where 
	deleted_at IS NOT NULL
)
select * from renamed

