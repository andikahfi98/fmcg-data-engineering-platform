with salesperson as (

    select *
    from {{ ref('stg_salesperson') }}

)

select

    md5(
        'salesperson|' || salesperson_id
    ) as salesperson_key,

    salesperson_id,
    salesperson_name,
    supervisor,
    distributor_id,
    territory,
    join_year

from salesperson