with source as (

    select *
    from {{ source('raw', 'current_salesperson') }}

),

cleaned as (

    select
        nullif(trim(salesperson_id), '') as salesperson_id,
        nullif(trim(salesperson_name), '') as salesperson_name,
        nullif(trim(supervisor), '') as supervisor,
        nullif(trim(distributor_id), '') as distributor_id,
        nullif(trim(territory), '') as territory,

        nullif(trim(join_year), '')::numeric::integer as join_year,

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