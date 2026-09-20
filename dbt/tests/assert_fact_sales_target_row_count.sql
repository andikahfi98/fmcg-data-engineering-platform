select 1
where
    (
        select count(*)
        from {{ ref('stg_target') }}
    )
    <>
    (
        select count(*)
        from {{ ref('fact_sales_target') }}
    )