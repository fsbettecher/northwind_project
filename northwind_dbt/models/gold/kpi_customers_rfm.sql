WITH snapshot_date AS (
    SELECT
        MAX(order_date) + interval '1 day' as ref_date
    FROM
        {{ ref('stg_orders') }}
),
rfm_raw as (
    SELECT
        o.customer_id,
        c.company_name as customer_name,
        c.country,
        (SELECT ref_date FROM snapshot_date)::date - max(o.order_date) as recency_days,
        COUNT(DISTINCT o.order_id) as frequency,
        ROUND(SUM(d.net_revenue)::numeric, 2) as monetary,
        MIN(o.order_date) as first_order_date,
        MAX(o.order_date) as last_order_date,
        ROUND(SUM(d.net_revenue) / COUNT(DISTINCT o.order_id), 2) as avg_order_value
    FROM
        {{ ref('stg_orders') }} o
    INNER JOIN
        {{ ref('stg_order_details') }} d on o.order_id = d.order_id
    INNER JOIN
        {{ ref('stg_customers') }} c on o.customer_id = c.customer_id
    GROUP BY
        o.customer_id, c.company_name, c.country
),
rfm_scored as (
    SELECT
        *,
        CASE
            WHEN recency_days <= 30  THEN 4
            WHEN recency_days <= 60  THEN 3
            WHEN recency_days <= 120 THEN 2
            ELSE 1
        END as r_score,
        NTILE(4) OVER (ORDER BY frequency asc) as f_score,
        NTILE(4) OVER (ORDER BY monetary asc) as m_score
    from rfm_raw
),
rfm_segment as (
    SELECT
        *,
        (r_score + f_score + m_score) as rfm_total,
        CASE
            WHEN r_score + f_score + m_score >= 10 THEN 'Campeão'
            WHEN r_score + f_score + m_score >= 8  THEN 'Fiel'
            WHEN r_score + f_score + m_score >= 5  THEN 'Potencial'
            ELSE 'Em Risco'
        END as rfm_segment,
        CASE
            WHEN r_score + f_score + m_score >= 10 THEN 4
            WHEN r_score + f_score + m_score >= 8  THEN 3
            WHEN r_score + f_score + m_score >= 5  THEN 2
            ELSE 1
        END as segment_priority
    FROM
        rfm_scored
)
SELECT * FROM rfm_segment
ORDER BY
    monetary DESC