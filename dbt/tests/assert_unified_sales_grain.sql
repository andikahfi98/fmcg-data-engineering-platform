select
    invoice_id,
    product_id,
    count(*) as row_count

from {{ ref('stg_sales_unified') }}

group by
    invoice_id,
    product_id

having count(*) > 1