WITH source AS (
    SELECT * FROM bronze.orders
),
ajustado AS (
    SELECT
        order_id::integer as order_id,
        customer_id::varchar(10) as customer_id,
        employee_id::integer as employee_id,
        order_date::date as order_date,
        required_date::date as required_date,
        shipped_date::date as shipped_date,
        ship_via::integer as shipper_id,
        freight::numeric(10,2) as freight,
        ship_name::varchar as ship_name,
        ship_address::varchar as ship_address,
        ship_city::varchar as ship_city,
        coalesce(ship_region::varchar, 'N/A') as ship_region,
        ship_postal_code::varchar as ship_postal_code,
        ship_country::varchar as ship_country,
        extract(year  from order_date::date)::int as order_year,
        extract(month from order_date::date)::int as order_month,
        to_char(order_date::date, 'YYYY-MM') as order_year_month,
        (shipped_date::date - order_date::date) as days_to_ship,
        (required_date::date - order_date::date) as days_to_deadline,
        CASE
            WHEN shipped_date is null THEN 'Não Enviado'
            WHEN shipped_date::date > required_date::date THEN 'Atrasado'
            ELSE 'No Prazo'
        END as delivery_status,
        CASE
            WHEN shipped_date::date > required_date::date THEN 1
            ELSE 0
        END as is_late
    FROM
        source
    WHERE
        order_id is not null
)
SELECT * FROM ajustado