select
    customer_country                                as country,
    customer_continent                              as continent,

    count(distinct order_id)                        as total_orders,
    count(distinct customer_id)                     as unique_customers,
    round(sum(net_revenue)::numeric, 2)             as total_net_revenue,
    round(
        sum(net_revenue) / count(distinct order_id)
    , 2)                                            as avg_ticket,
    round(
        sum(net_revenue) / sum(sum(net_revenue)) over () * 100
    , 2)                                            as revenue_share_pct,
    rank() over (order by sum(net_revenue) desc)    as revenue_rank

from {{ ref('fct_orders') }}
where customer_country is not null
group by 1, 2
order by total_net_revenue desc