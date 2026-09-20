with date_bounds as (

    select
        min(min_date) as min_date,
        max(max_date) as max_date

    from (

        select
            min(sales_date) as min_date,
            max(sales_date) as max_date
        from {{ ref('stg_sales') }}

        union all

        select
            min(month_start),
            max(month_start)
        from {{ ref('stg_target') }}

        union all

        select
            min(month_start),
            max(month_start)
        from {{ ref('stg_inventory') }}

    ) dates

),

date_spine as (

    select
        generate_series(
            min_date,
            max_date,
            interval '1 day'
        )::date as date_day

    from date_bounds

)

select

    to_char(
        date_day,
        'YYYYMMDD'
    )::integer as date_key,

    date_day,

    extract(day from date_day)::integer
        as day_of_month,

    extract(month from date_day)::integer
        as month_number,

    trim(to_char(date_day, 'Month'))
        as month_name,

    extract(quarter from date_day)::integer
        as quarter_number,

    extract(year from date_day)::integer
        as year,

    date_trunc(
        'month',
        date_day
    )::date as month_start,

    to_char(
        date_day,
        'YYYY-MM'
    ) as year_month,

    extract(isodow from date_day)::integer
        as day_of_week_number,

    trim(to_char(date_day, 'Day'))
        as day_name,

    (
        date_day =
        date_trunc('month', date_day)::date
    ) as is_month_start

from date_spine