select 1
where
    (
        select count(*)
        from {{ ref('stg_inventory') }}
    )
    <>
    (
        select count(*)
        from {{ ref('fact_inventory') }}
    )