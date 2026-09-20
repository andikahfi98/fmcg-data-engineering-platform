with product as (

    select *
    from {{ ref('stg_product') }}

)

select

    md5(
        'product|' || product_id
    ) as product_key,

    product_id,
    category,
    brand,
    product_name,
    variant,
    pack_size,
    unit_price,
    unit_cogs

from product