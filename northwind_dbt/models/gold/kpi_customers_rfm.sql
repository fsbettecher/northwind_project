with snapshot_date as (
    select max(order_date) + interval '1 day' as ref_date
    from {{ ref('stg_orders') }}
),

rfm_raw as (
    select
        o.customer_id,
        c.company_name                                  as customer_name,
        c.country,
        c.continent,

        (select ref_date from snapshot_date)::date
            - max(o.order_date)                         as recency_days,

        count(distinct o.order_id)                      as frequency,

        round(sum(d.net_revenue)::numeric, 2)           as monetary,

        min(o.order_date)                               as first_order_date,
        max(o.order_date)                               as last_order_date,

        round(sum(d.net_revenue) / count(distinct o.order_id), 2)
                                                        as avg_order_value

    from {{ ref('stg_orders') }} o
    inner join {{ ref('stg_order_details') }} d on o.order_id = d.order_id
    inner join {{ ref('stg_customers') }} c on o.customer_id = c.customer_id
    group by 1, 2, 3, 4
),

rfm_scored as (
    select
        *,
        case
            when recency_days <= 30  then 4
            when recency_days <= 60  then 3
            when recency_days <= 120 then 2
            else 1
        end                                             as r_score,

        ntile(4) over (order by frequency asc)          as f_score,
        ntile(4) over (order by monetary asc)           as m_score

    from rfm_raw
),

rfm_segment as (
    select
        *,
        (r_score + f_score + m_score)                   as rfm_total,

        case
            when r_score + f_score + m_score >= 10 then 'Campeão'
            when r_score + f_score + m_score >= 8  then 'Fiel'
            when r_score + f_score + m_score >= 5  then 'Potencial'
            else                                        'Em Risco'
        end                                             as rfm_segment,

        case
            when r_score + f_score + m_score >= 10 then 4
            when r_score + f_score + m_score >= 8  then 3
            when r_score + f_score + m_score >= 5  then 2
            else                                        1
        end                                             as segment_priority

    from rfm_scored
)

select * from rfm_segment
order by monetary desc