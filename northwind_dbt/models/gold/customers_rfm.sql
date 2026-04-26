WITH fake_current_date AS (
    SELECT
        MAX(order_date) + interval '1 day' as ref_date -- simulando um marco de data, uma vez que os dados finalizaram em 1998
    FROM
        {{ ref('stg_orders') }}
),
rfm_base as (
    SELECT
        o.customer_id,
        c.company_name,
        c.contact_name as customer_name,
        c.contact_title,
        c.country,
        (SELECT ref_date FROM fake_current_date)::date - max(o.order_date) as recency, -- total de dias entre a útima compra e a data de referência
        COUNT(DISTINCT o.order_id) as frequency, -- frequência de pedidos por cliente
        ROUND(SUM(d.net_revenue)::numeric, 2) as monetary -- total de receita líquida gerada por cliente
    FROM
        {{ ref('stg_orders') }} o
    INNER JOIN
        {{ ref('stg_order_details') }} d on o.order_id = d.order_id
    INNER JOIN
        {{ ref('stg_customers') }} c on o.customer_id = c.customer_id
    GROUP BY
        o.customer_id, c.company_name, c.contact_name, c.contact_title, c.country
),
rfm_scores as (
    SELECT
        *,
        CASE
            WHEN recency <= 30  THEN 4
            WHEN recency <= 60  THEN 3
            WHEN recency <= 120 THEN 2
            ELSE 1
        END as recency_score, -- scores de recência com base em intervalos de dias. Pode variar conforme o negócio
        NTILE(4) OVER (ORDER BY frequency asc) as frequency_score, -- Dividindo os clientes em 4 grupos iguais. Pode variar conforme o negócio
        NTILE(4) OVER (ORDER BY monetary asc) as monetary_score -- Dividindo os clientes em 4 grupos iguais. Pode variar conforme o negócio
    FROM rfm_base
),
rfm_status as (
    SELECT
        *,
        (recency_score + frequency_score + monetary_score) as rfm_total, -- máximo de 12 pontos, mínimo de 3 pontos
        CASE
            WHEN recency_score + frequency_score + monetary_score >= 10 THEN 'Loyal Customer'
            WHEN recency_score + frequency_score + monetary_score >= 7  THEN 'Regular Customer'
            WHEN recency_score + frequency_score + monetary_score >= 4  THEN 'Potential Customer'
            ELSE 'At Risk'
        END as rfm_status,
        CASE
            WHEN recency_score + frequency_score + monetary_score >= 10 THEN 4
            WHEN recency_score + frequency_score + monetary_score >= 7  THEN 3
            WHEN recency_score + frequency_score + monetary_score >= 4  THEN 2
            ELSE 1
        END as status_priority
    FROM
        rfm_scores
)
SELECT * FROM rfm_status
ORDER BY
    monetary DESC