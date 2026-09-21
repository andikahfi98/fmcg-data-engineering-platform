with source as (

    select *
    from {{ source('raw', 'sales_streaming_events') }}

),

transformed as (

    select
        streaming_record_id,

        event_id,
        event_type,
        event_timestamp,

        source,
        source_file,

        payload -> 'transaction' ->> 'date'
            as sales_date_raw,

        payload -> 'transaction' ->> 'distributor_id'
            as distributor_id,

        payload -> 'transaction' ->> 'salesperson_id'
            as salesperson_id,

        payload -> 'transaction' ->> 'outlet_id'
            as outlet_id,

        payload -> 'transaction' ->> 'product_id'
            as product_id,

        payload -> 'transaction' ->> 'invoice_id'
            as invoice_id,

        payload -> 'transaction' ->> 'quantity'
            as quantity_raw,

        payload -> 'transaction' ->> 'unit_price'
            as unit_price_raw,

        payload -> 'transaction' ->> 'discount_pct'
            as discount_pct_raw,

        payload -> 'transaction' ->> 'discount_amount'
            as discount_amount_raw,

        payload -> 'transaction' ->> 'gross_sales'
            as gross_sales_raw,

        payload -> 'transaction' ->> 'net_sales'
            as net_sales_raw,

        payload -> 'transaction' ->> 'unit_cogs'
            as unit_cogs_raw,

        payload -> 'transaction' ->> 'total_cogs'
            as total_cogs_raw,

        kafka_topic,
        kafka_partition,
        kafka_offset,

        ingested_at

    from source

),

typed as (

    select
        streaming_record_id,

        event_id,
        event_type,
        event_timestamp,

        source,
        source_file,

        sales_date_raw::date
            as sales_date,

        trim(distributor_id)
            as distributor_id,

        trim(salesperson_id)
            as salesperson_id,

        trim(outlet_id)
            as outlet_id,

        trim(product_id)
            as product_id,

        trim(invoice_id)
            as invoice_id,

        quantity_raw::integer
            as quantity,

        unit_price_raw::numeric(18, 2)
            as unit_price,

        discount_pct_raw::numeric(10, 6)
            as discount_pct,

        discount_amount_raw::numeric(18, 2)
            as discount_amount,

        gross_sales_raw::numeric(18, 2)
            as gross_sales,

        net_sales_raw::numeric(18, 2)
            as net_sales,

        unit_cogs_raw::numeric(18, 2)
            as unit_cogs,

        total_cogs_raw::numeric(18, 2)
            as total_cogs,

        kafka_topic,
        kafka_partition,
        kafka_offset,

        ingested_at

    from transformed

)

select *
from typed