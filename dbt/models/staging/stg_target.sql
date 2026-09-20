with source as (

    select *
    from {{ source('raw', 'current_target') }}

),

cleaned as (

    select
        nullif(trim(month_start), '')::timestamp::date as month_start,
        nullif(trim(salesperson_id), '') as salesperson_id,

        nullif(trim(target_sales), '')::numeric(18,2) as target_sales,

        nullif(trim(target_active_outlets), '')::numeric::integer
            as target_active_outlets,

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