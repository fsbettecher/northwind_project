with base as (
    select
        order_year_month,
        order_year,
        order_month,
        count(distinct order_id)            as total_orders,
        count(distinct customer_id)         as active_customers,
        sum(net_revenue)                    as net_revenue,
        sum(gross_revenue)                  as gross_revenue,
        sum(discount_amount)                as total_discounts,
        sum(net_revenue) / nullif(count(distinct order_id), 0)
                                            as avg_ticket,
        sum(freight)                        as total_freight
    from {{ ref('fct_orders') }}
    group by 1, 2, 3
),

with_lag as (
    select
        *,
        lag(net_revenue) over (order by order_year_month) as prev_month_revenue,
        lag(total_orders) over (order by order_year_month) as prev_month_orders
    from base
)

select
    *,
    round(
        (net_revenue - prev_month_revenue)
        / nullif(prev_month_revenue, 0) * 100, 2
    )                                       as revenue_mom_pct,

    round(
        (total_orders - prev_month_orders)::numeric
        / nullif(prev_month_orders, 0) * 100, 2
    )                                       as orders_mom_pct

from with_lag
order by order_year_month