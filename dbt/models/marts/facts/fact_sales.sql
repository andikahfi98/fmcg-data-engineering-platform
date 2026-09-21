with sales as (

    select *
    from {{ ref('stg_sales_unified') }}

),

final as (

    select

        -- ===================================================
        -- FACT SURROGATE KEY
        -- ===================================================

        md5(
            'sales|'
            || sales.invoice_id
            || '|'
            || sales.product_id
        ) as sales_key,


        -- ===================================================
        -- DIMENSION KEYS
        -- ===================================================

        date_dim.date_key,

        product.product_key,

        distributor.distributor_key,

        salesperson.salesperson_key,

        outlet.outlet_key,


        -- ===================================================
        -- DEGENERATE / BUSINESS IDENTIFIERS
        -- ===================================================

        sales.invoice_id,
        sales.product_id,
        sales.distributor_id,
        sales.salesperson_id,
        sales.outlet_id,

        sales.sales_date,


        -- ===================================================
        -- MEASURES
        -- ===================================================

        sales.quantity,

        sales.unit_price,

        sales.discount_pct,

        sales.discount_amount,

        sales.gross_sales,

        sales.net_sales,

        sales.unit_cogs,

        sales.total_cogs,

        sales.net_sales
            - sales.total_cogs
            as gross_profit,


        case
            when sales.net_sales <> 0
            then (
                sales.net_sales
                - sales.total_cogs
            ) / sales.net_sales
        end as gross_margin_pct,


        -- ===================================================
        -- LINEAGE
        -- ===================================================

        sales.record_source,
        sales.source_record_id,
        sales.source_file,
        sales.source_event_timestamp,
        sales.ingested_at

    from sales

    left join {{ ref('dim_date') }} date_dim
        on sales.sales_date
        = date_dim.date_day

    left join {{ ref('dim_product') }} product
        on sales.product_id
        = product.product_id

    left join {{ ref('dim_distributor') }} distributor
        on sales.distributor_id
        = distributor.distributor_id

    left join {{ ref('dim_salesperson') }} salesperson
        on sales.salesperson_id
        = salesperson.salesperson_id

    left join {{ ref('dim_outlet') }} outlet
        on sales.outlet_id
        = outlet.outlet_id

)

select *
from final