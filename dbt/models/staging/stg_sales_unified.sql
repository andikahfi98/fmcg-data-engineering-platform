with batch_sales as (

    select
        sales_date,
        distributor_id,
        salesperson_id,
        outlet_id,
        product_id,
        invoice_id,

        quantity,
        unit_price,
        discount_pct,
        discount_amount,
        gross_sales,
        net_sales,
        unit_cogs,
        total_cogs,

        'batch' as record_source,

        raw_record_id::text
            as source_record_id,

        source_file,

        null::timestamptz
            as source_event_timestamp,

        ingested_at,

        1 as source_priority

    from {{ ref('stg_sales') }}

),

streaming_sales as (

    select
        sales_date,
        distributor_id,
        salesperson_id,
        outlet_id,
        product_id,
        invoice_id,

        quantity,
        unit_price,
        discount_pct,
        discount_amount,
        gross_sales,
        net_sales,
        unit_cogs,
        total_cogs,

        'streaming' as record_source,

        event_id
            as source_record_id,

        source_file,

        event_timestamp
            as source_event_timestamp,

        ingested_at,

        2 as source_priority

    from {{ ref('stg_sales_streaming') }}

),

combined as (

    select *
    from batch_sales

    union all

    select *
    from streaming_sales

),

ranked as (

    select
        *,

        row_number() over (
            partition by
                invoice_id,
                product_id

            order by
                source_priority desc,
                source_event_timestamp desc nulls last,
                ingested_at desc
        ) as row_num

    from combined

)

select
    sales_date,
    distributor_id,
    salesperson_id,
    outlet_id,
    product_id,
    invoice_id,

    quantity,
    unit_price,
    discount_pct,
    discount_amount,
    gross_sales,
    net_sales,
    unit_cogs,
    total_cogs,

    record_source,
    source_record_id,
    source_file,
    source_event_timestamp,
    ingested_at

from ranked

where row_num = 1