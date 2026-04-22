with source as (select * from bronze.products),
cats  as (select * from bronze.categories),
sups  as (select * from bronze.suppliers),

cleaned as (
    select
        p.product_id::integer                       as product_id,
        p.product_name::varchar                     as product_name,
        p.supplier_id::integer                      as supplier_id,
        p.category_id::integer                      as category_id,
        c.category_name::varchar                    as category_name,
        s.company_name::varchar                     as supplier_name,
        p.quantity_per_unit::varchar                as quantity_per_unit,
        p.unit_price::numeric(10,2)                 as unit_price,
        p.units_in_stock::integer                   as units_in_stock,
        p.units_on_order::integer                   as units_on_order,
        p.reorder_level::integer                    as reorder_level,
        p.discontinued::integer                     as discontinued,

        case when p.discontinued::integer = 1
             then true else false end               as is_discontinued,

        case
            when p.units_in_stock::integer = 0
                then 'Sem Estoque'
            when p.units_in_stock::integer <= p.reorder_level::integer
                then 'Estoque Crítico'
            when p.units_in_stock::integer <= p.reorder_level::integer * 2
                then 'Estoque Baixo'
            else 'Estoque OK'
        end                                         as stock_status

    from source p
    left join cats c on p.category_id::integer = c.category_id::integer
    left join sups s on p.supplier_id::integer = s.supplier_id::integer
    where p.product_id is not null
)

select * from cleaned