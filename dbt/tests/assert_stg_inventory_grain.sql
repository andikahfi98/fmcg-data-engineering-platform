select
    month_start,
    distributor_id,
    product_id,
    count(*) as row_count

from {{ ref('stg_inventory') }}

group by
    month_start,
    distributor_id,
    product_id

having count(*) > 1