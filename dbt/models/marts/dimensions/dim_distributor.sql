with distributor as (

    select *
    from {{ ref('stg_distributor') }}

)

select

    md5(
        'distributor|' || distributor_id
    ) as distributor_key,

    distributor_id,
    distributor_name,
    distributor_type,
    city,
    province,
    region,
    start_year

from distributor