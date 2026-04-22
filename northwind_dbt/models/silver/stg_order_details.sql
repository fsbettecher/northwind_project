with source as (
    select * from bronze.order_details
),

cleaned as (
    select
        order_id::integer                           as order_id,
        product_id::integer                         as product_id,
        unit_price::numeric(10,2)                   as unit_price,
        quantity::integer                           as quantity,
        discount::numeric(5,4)                      as discount,

        (unit_price::numeric * quantity::integer)::numeric(12,2)
                                                    as gross_revenue,

        (unit_price::numeric * quantity::integer
            * (1 - discount::numeric))::numeric(12,2)
                                                    as net_revenue,

        (unit_price::numeric * quantity::integer * discount::numeric)::numeric(12,2)
                                                    as discount_amount,

        case
            when discount::numeric = 0    then 'Sem Desconto'
            when discount::numeric < 0.10 then 'Desconto Baixo (<10%)'
            when discount::numeric < 0.20 then 'Desconto Médio (10-20%)'
            else                               'Desconto Alto (>20%)'
        end                                         as discount_tier

    from source
    where order_id is not null
      and product_id is not null
)

select * from cleaned