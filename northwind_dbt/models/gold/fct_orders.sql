WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),
order_details as (
    SELECT * FROM {{ ref('stg_order_details') }}
),
customers as (
    SELECT * FROM {{ ref('stg_customers') }}
),
products as (
    SELECT * FROM {{ ref('stg_products') }}
),
employees as (
    SELECT * FROM {{ ref('stg_employees') }}
),
shippers as (
    SELECT * FROM {{ ref('stg_shippers') }}
)
SELECT
    d.order_id,
    d.product_id,
    o.customer_id,
    o.employee_id,
    o.shipper_id,
    o.order_date,
    o.shipped_date,
    o.required_date,
    c.company_name as customer_name,
    c.city as customer_city,
    c.country as customer_country,
    p.product_name,
    p.category_name,
    p.supplier_name,
    p.stock_status,
    p.is_discontinued,
    e.full_name as employee_name,
    e.title as employee_title,
    s.shipper_name,
    d.quantity,
    d.unit_price,
    d.discount,
    d.discount_tier,
    d.discount_amount,
    d.gross_revenue,
    d.net_revenue,
    o.freight,
    o.delivery_status,
    o.is_late,
    o.days_to_ship,
    o.days_to_deadline
FROM
    order_details d
INNER JOIN
    orders o on d.order_id   = o.order_id
LEFT JOIN
    customers c on o.customer_id = c.customer_id
LEFT JOIN
    products  p on d.product_id  = p.product_id
LEFT JOIN
    employees e on o.employee_id = e.employee_id
LEFT JOIN
    shippers  s on o.shipper_id  = s.shipper_id