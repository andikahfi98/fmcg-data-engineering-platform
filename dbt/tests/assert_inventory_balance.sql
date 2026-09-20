select
    inventory_key,
    opening_stock,
    stock_received,
    units_sold,
    closing_stock,
    calculated_closing_stock

from {{ ref('fact_inventory') }}

where closing_stock <> calculated_closing_stock