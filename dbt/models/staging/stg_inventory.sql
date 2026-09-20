with source as (

    select *
    from {{ source('raw', 'current_inventory') }}

),

cleaned as (

    select
        nullif(trim(month_start), '')::timestamp::date as month_start,
        nullif(trim(distributor_id), '') as distributor_id,
        nullif(trim(product_id), '') as product_id,

        nullif(trim(opening_stock), '')::numeric::integer as opening_stock,
        nullif(trim(stock_received), '')::numeric::integer as stock_received,
        nullif(trim(units_sold), '')::numeric::integer as units_sold,
        nullif(trim(closing_stock), '')::numeric::integer as closing_stock,

        nullif(trim(stock_value), '')::numeric(18,2) as stock_value,

        raw_record_id,
        source_file,
        source_sheet,
        source_row_number,
        pipeline_run_id,
        ingested_at

    from source

)

select *
from cleaned