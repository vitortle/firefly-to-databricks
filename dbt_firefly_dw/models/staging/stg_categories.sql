with source as (
    select * from {{ source('firefly_raw', 'bz_categories') }}
),
renamed as (
    SELECT
	id AS categories_id,
	created_at,
	updated_at,
	deleted_at,
	user_id,
	name AS category_name
FROM
	source
-- WHERE deleted_at IS null
)
select * from renamed