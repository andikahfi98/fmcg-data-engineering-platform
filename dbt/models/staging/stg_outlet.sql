with source as (

    select *
    from {{ source('raw', 'current_outlet') }}

),

cleaned as (

    select
        nullif(trim(outlet_id), '') as outlet_id,
        nullif(trim(outlet_name), '') as outlet_name,
        nullif(trim(channel), '') as channel,
        nullif(trim(outlet_tier), '') as outlet_tier,
        nullif(trim(distributor_id), '') as distributor_id,
        nullif(trim(salesperson_id), '') as salesperson_id,
        nullif(trim(city), '') as city,
        nullif(trim(province), '') as province,
        nullif(trim(region), '') as region,

        nullif(trim(open_year), '')::numeric::integer as open_year,

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