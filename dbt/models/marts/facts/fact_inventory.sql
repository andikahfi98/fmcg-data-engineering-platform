with inventory as (

    select *
    from {{ ref('stg_inventory') }}

)

select

    -- surrogate fact key
    md5(
        'inventory|'
        || inventory.month_start::text
        || '|'
        || inventory.distributor_id
        || '|'
        || inventory.product_id
    ) as inventory_key,

    -- dimension keys
    date_dim.date_key,
    distributor.distributor_key,
    product.product_key,

    -- business identifiers
    inventory.month_start,
    inventory.distributor_id,
    inventory.product_id,

    -- measures
    inventory.opening_stock,
    inventory.stock_received,
    inventory.units_sold,
    inventory.closing_stock,
    inventory.stock_value,

    (
        inventory.opening_stock
        + inventory.stock_received
        - inventory.units_sold
    ) as calculated_closing_stock,

    -- lineage
    inventory.raw_record_id,
    inventory.source_file,
    inventory.source_sheet,
    inventory.source_row_number,
    inventory.pipeline_run_id,
    inventory.ingested_at

from inventory

left join {{ ref('dim_date') }} date_dim
    on inventory.month_start = date_dim.date_day

left join {{ ref('dim_distributor') }} distributor
    on inventory.distributor_id = distributor.distributor_id

left join {{ ref('dim_product') }} product
    on inventory.product_id = product.product_id