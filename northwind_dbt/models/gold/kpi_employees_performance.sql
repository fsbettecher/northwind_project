with base as (
    select
        employee_id,
        employee_name,
        employee_title,

        count(distinct order_id)                    as total_orders,
        count(distinct customer_id)                 as unique_customers,
        round(sum(net_revenue)::numeric, 2)         as total_net_revenue,
        round(avg(net_revenue / quantity)::numeric, 2)
                                                    as avg_unit_price_sold,
        round(
            sum(net_revenue) / count(distinct order_id)
        , 2)                                        as avg_ticket,
        round(avg(discount)::numeric * 100, 2)      as avg_discount_pct,
        round(avg(days_to_ship)::numeric, 1)        as avg_days_to_ship,
        sum(is_late)                                as late_orders,
        round(
            sum(is_late)::numeric / count(distinct order_id) * 100
        , 1)                                        as late_order_pct

    from {{ ref('fct_orders') }}
    where employee_id is not null
    group by 1, 2, 3
),

ranked as (
    select
        *,
        rank() over (order by total_net_revenue desc)
                                                    as revenue_rank,
        round(
            total_net_revenue / sum(total_net_revenue) over () * 100
        , 2)                                        as revenue_share_pct

    from base
)

select * from ranked
order by revenue_rank