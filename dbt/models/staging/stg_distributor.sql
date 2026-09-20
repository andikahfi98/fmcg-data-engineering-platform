with source as (

    select *
    from {{ source('raw', 'current_distributor') }}

),

cleaned as (

    select
        nullif(trim(distributor_id), '') as distributor_id,
        nullif(trim(distributor_name), '') as distributor_name,
        nullif(trim(region), '') as region,
        nullif(trim(province), '') as province,
        nullif(trim(city), '') as city,
        nullif(trim(distributor_type), '') as distributor_type,

        nullif(trim(start_year), '')::numeric::integer as start_year,

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