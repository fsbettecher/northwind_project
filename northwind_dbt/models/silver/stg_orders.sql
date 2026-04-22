with source as (
    select * from bronze.orders
),

cleaned as (
    select
        order_id::integer                           as order_id,
        customer_id::varchar(10)                    as customer_id,
        employee_id::integer                        as employee_id,
        order_date::date                            as order_date,
        required_date::date                         as required_date,
        shipped_date::date                          as shipped_date,
        ship_via::integer                           as shipper_id,
        freight::numeric(10,2)                      as freight,
        ship_name::varchar                          as ship_name,
        ship_address::varchar                       as ship_address,
        ship_city::varchar                          as ship_city,
        coalesce(ship_region::varchar, 'N/A')       as ship_region,
        ship_postal_code::varchar                   as ship_postal_code,
        ship_country::varchar                       as ship_country,

        extract(year  from order_date::date)::int   as order_year,
        extract(month from order_date::date)::int   as order_month,
        to_char(order_date::date, 'YYYY-MM')        as order_year_month,

        (shipped_date::date - order_date::date)     as days_to_ship,
        (required_date::date - order_date::date)    as days_to_deadline,

        case
            when shipped_date is null then 'Não Enviado'
            when shipped_date::date > required_date::date then 'Atrasado'
            else 'No Prazo'
        end                                         as delivery_status,

        case
            when shipped_date::date > required_date::date then 1
            else 0
        end                                         as is_late

    from source
    where order_id is not null
)

select * from cleaned