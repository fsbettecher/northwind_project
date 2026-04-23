WITH source AS (
    SELECT * FROM bronze.order_details
),
ajustado AS (
    SELECT
        order_id::integer as order_id,
        product_id::integer as product_id,
        unit_price::numeric(10,2) as unit_price, -- limitando e arredondando os números
        quantity::integer as quantity,
        discount::numeric(5,4) as discount, --limitando e arredondando os números
        (unit_price::numeric * quantity::integer)::numeric(12,2) as gross_revenue, -- calculando a renda bruta
        (unit_price::numeric * quantity::integer * (1 - discount::numeric))::numeric(12,2) as net_revenue, -- calculando a renda líquida
        (unit_price::numeric * quantity::integer * discount::numeric)::numeric(12,2) as discount_amount, -- calculando o desconto
        CASE
            WHEN discount::numeric = 0 THEN 'Sem Desconto'
            WHEN discount::numeric < 0.10 THEN 'Desconto Baixo (<10%)'
            WHEN discount::numeric < 0.20 THEN 'Desconto Médio (10-20%)'
            ELSE 'Desconto Alto (>20%)'
        END as discount_tier -- definindo os níveis de desconto. Pode ser ajustado dependendo do negócio
    FROM
        source
    WHERE
        order_id is not null
        AND product_id is not null
)
SELECT * FROM ajustado