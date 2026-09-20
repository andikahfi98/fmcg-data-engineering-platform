with source as (

    select *
    from {{ source('raw', 'current_sales') }}

),

cleaned as (

    select

        -- business identifiers
        nullif(trim(distributor_id), '') as distributor_id,
        nullif(trim(salesperson_id), '') as salesperson_id,
        nullif(trim(outlet_id), '') as outlet_id,
        nullif(trim(product_id), '') as product_id,
        nullif(trim(invoice_id), '') as invoice_id,

        -- date
        nullif(trim(date), '')::date as sales_date,

        -- quantities
        nullif(trim(quantity), '')::integer as quantity,

        -- financial metrics
        nullif(trim(unit_price), '')::numeric(18,2) as unit_price,
        nullif(trim(discount_pct), '')::numeric(10,6) as discount_pct,
        nullif(trim(discount_amount), '')::numeric(18,2) as discount_amount,
        nullif(trim(gross_sales), '')::numeric(18,2) as gross_sales,
        nullif(trim(net_sales), '')::numeric(18,2) as net_sales,
        nullif(trim(unit_cogs), '')::numeric(18,2) as unit_cogs,
        nullif(trim(total_cogs), '')::numeric(18,2) as total_cogs,

        -- lineage
        raw_record_id,
        source_file,
        source_row_number,
        pipeline_run_id,
        ingested_at

    from source

)

select *
from cleaned