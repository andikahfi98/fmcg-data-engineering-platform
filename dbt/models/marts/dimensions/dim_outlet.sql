with outlet as (

    select *
    from {{ ref('stg_outlet') }}

)

select

    md5(
        'outlet|' || outlet_id
    ) as outlet_key,

    outlet_id,
    outlet_name,
    channel,
    outlet_tier,
    distributor_id,
    salesperson_id,
    city,
    province,
    region,
    open_year

from outlet