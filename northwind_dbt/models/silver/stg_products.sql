WITH source AS (
    select * from bronze.products
),
cats  AS (
    select * from bronze.categories
),
sups  AS (
    select * from bronze.suppliers
),
ajustado AS (
    select
        f.product_id::integer as product_id,
        f.product_name::varchar as product_name,
        f.supplier_id::integer as supplier_id,
        f.category_id::integer as category_id,
        c.category_name::varchar as category_name,
        s.company_name::varchar as supplier_name,
        f.quantity_per_unit::varchar as quantity_per_unit,
        f.unit_price::numeric(10, 2) as unit_price, -- limitando e arredondando os números
        f.units_in_stock::integer as units_in_stock,
        f.units_on_order::integer as units_on_order,
        f.reorder_level::integer as reorder_level,
        CASE
            WHEN f.discontinued::integer = 1 THEN true
            ELSE false
        END as is_discontinued, -- criando um booleano para produtos descontinuados
        CASE
            WHEN f.units_in_stock::integer = 0 THEN 'Out of Stock'
            WHEN f.units_in_stock::integer <= f.reorder_level::integer THEN 'Critical Stock'
            WHEN f.units_in_stock::integer <= f.reorder_level::integer * 2 THEN 'Low Stock' -- Esse valor pode ser ajustável dependendo do negócio
            ELSE 'In Stock'
        END as stock_status -- criando um status de estoque dos produtos
    FROM
        source f
    LEFT JOIN
        cats c on f.category_id::integer = c.category_id::integer
    LEFT JOIN
        sups s on f.supplier_id::integer = s.supplier_id::integer
    WHERE
        f.product_id is not null
)
SELECT * FROM ajustado