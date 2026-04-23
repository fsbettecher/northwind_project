WITH source AS (
    SELECT * FROM bronze.order_details
),
ajustado AS (
    SELECT
        order_id::integer as order_id,
        product_id::integer as product_id,
        unit_price::numeric(10, 2) as unit_price, -- limitando e arredondando os números
        quantity::integer as quantity,
        discount::numeric(5, 4) as discount, --limitando e arredondando os números
        (unit_price::numeric * quantity::integer)::numeric(12, 2) as gross_revenue, -- calculando a renda bruta
        (unit_price::numeric * quantity::integer * (1 - discount::numeric))::numeric(12, 2) as net_revenue, -- calculando a renda líquida
        (unit_price::numeric * quantity::integer * discount::numeric)::numeric(12, 2) as discount_amount, -- calculando o desconto em valor
        CASE
            WHEN discount::numeric = 0 THEN 'No Discount'
            WHEN discount::numeric < 0.10 THEN 'Low Discount (<10%)'
            WHEN discount::numeric < 0.20 THEN 'Medium Discount (10-20%)'
            ELSE 'High Discount (>20%)'
        END as discount_tier -- definindo os níveis de desconto. Pode ser ajustado dependendo do negócio
    FROM
        source
    WHERE
        order_id is not null
        AND product_id is not null
)
SELECT * FROM ajustado