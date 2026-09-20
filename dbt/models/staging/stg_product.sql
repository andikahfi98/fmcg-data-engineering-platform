with source as (

    select *
    from {{ source('raw', 'current_product') }}

),

cleaned as (

    select
        nullif(trim(product_id), '') as product_id,
        nullif(trim(category), '') as category,
        nullif(trim(brand), '') as brand,
        nullif(trim(product_name), '') as product_name,
        nullif(trim(variant), '') as variant,
        nullif(trim(pack_size), '') as pack_size,

        nullif(trim(unit_price), '')::numeric(18,2) as unit_price,
        nullif(trim(unit_cogs), '')::numeric(18,2) as unit_cogs,

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