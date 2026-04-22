with orders      as (select * from {{ ref('stg_orders') }}),
     details     as (select * from {{ ref('stg_order_details') }}),
     customers   as (select * from {{ ref('stg_customers') }}),
     products    as (select * from {{ ref('stg_products') }}),
     employees   as (select * from {{ ref('stg_employees') }}),
     shippers    as (select * from {{ ref('stg_shippers') }})

select
    -- Chaves
    d.order_id,
    d.product_id,
    o.customer_id,
    o.employee_id,
    o.shipper_id,

    -- Tempo
    o.order_date,
    o.shipped_date,
    o.required_date,
    o.order_year,
    o.order_month,
    o.order_year_month,

    -- Cliente
    c.company_name          as customer_name,
    c.city                  as customer_city,
    c.country               as customer_country,
    c.continent             as customer_continent,

    -- Produto
    p.product_name,
    p.category_name,
    p.supplier_name,
    p.stock_status,
    p.is_discontinued,

    -- Vendedor
    e.full_name             as employee_name,
    e.title                 as employee_title,

    -- Transportadora
    s.shipper_name,

    -- Métricas
    d.quantity,
    d.unit_price,
    d.discount,
    d.discount_amount,
    d.gross_revenue,
    d.net_revenue,
    d.discount_tier,
    o.freight,

    -- Operacional
    o.delivery_status,
    o.is_late,
    o.days_to_ship,
    o.days_to_deadline

from details d
inner join orders    o on d.order_id   = o.order_id
left  join customers c on o.customer_id = c.customer_id
left  join products  p on d.product_id  = p.product_id
left  join employees e on o.employee_id = e.employee_id
left  join shippers  s on o.shipper_id  = s.shipper_id