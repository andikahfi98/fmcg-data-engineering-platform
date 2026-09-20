with staging as (

    select count(*) as row_count
    from {{ ref('stg_sales') }}

),

warehouse as (

    select count(*) as row_count
    from {{ ref('fact_sales') }}

)

select
    staging.row_count as staging_rows,
    warehouse.row_count as warehouse_rows

from staging
cross join warehouse

where
    staging.row_count
    <> warehouse.row_count