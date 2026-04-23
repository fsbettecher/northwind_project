WITH source AS (
    SELECT * FROM bronze.orders
),
ajustado AS (
    SELECT
        order_id::integer as order_id,
        customer_id::varchar(10) as customer_id, -- limitando valores para 10 caracteres
        employee_id::integer as employee_id,
        order_date::date as order_date,
        required_date::date as required_date,
        shipped_date::date as shipped_date,
        ship_via::integer as shipper_id,
        freight::numeric(10, 2) as freight, -- limitando e arredondando os números
        ship_name::varchar as ship_name,
        ship_address::varchar as ship_address,
        ship_city::varchar as ship_city,
        coalesce(ship_region::varchar, 'N/A') as ship_region, -- removendo nulls
        ship_postal_code::varchar as ship_postal_code,
        ship_country::varchar as ship_country,
        (shipped_date::date - order_date::date) as days_to_ship,
        (required_date::date - order_date::date) as days_to_deadline,
        CASE
            WHEN shipped_date is null THEN 'Not Shipped'
            WHEN shipped_date::date > required_date::date THEN 'Late'
            ELSE 'On Time'
        END as delivery_status, -- criando um status de entrega
        CASE
            WHEN shipped_date::date > required_date::date THEN true
            ELSE false
        END as is_late -- criando um booleano de atraso
    FROM
        source
    WHERE
        order_id is not null
)
SELECT * FROM ajustado