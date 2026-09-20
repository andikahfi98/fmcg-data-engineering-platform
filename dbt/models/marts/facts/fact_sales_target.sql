with target as (

    select *
    from {{ ref('stg_target') }}

)

select

    md5(
        'sales_target|'
        || target.month_start::text
        || '|'
        || target.salesperson_id
    ) as sales_target_key,

    -- dimension keys
    date_dim.date_key,
    salesperson.salesperson_key,

    -- business identifiers
    target.month_start,
    target.salesperson_id,

    -- measures
    target.target_sales,
    target.target_active_outlets,

    -- lineage
    target.raw_record_id,
    target.source_file,
    target.source_sheet,
    target.source_row_number,
    target.pipeline_run_id,
    target.ingested_at

from target

left join {{ ref('dim_date') }} date_dim
    on target.month_start = date_dim.date_day

left join {{ ref('dim_salesperson') }} salesperson
    on target.salesperson_id = salesperson.salesperson_id