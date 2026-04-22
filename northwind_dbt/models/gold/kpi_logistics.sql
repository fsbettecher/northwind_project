select
    shipper_id,
    shipper_name,

    count(distinct order_id)                        as total_orders,
    round(sum(net_revenue)::numeric, 2)             as total_revenue_shipped,
    round(sum(freight)::numeric, 2)                 as total_freight,
    round(avg(freight)::numeric, 2)                 as avg_freight_per_order,
    round(avg(days_to_ship)::numeric, 2)            as avg_days_to_ship,
    min(days_to_ship)                               as min_days_to_ship,
    max(days_to_ship)                               as max_days_to_ship,
    sum(is_late)                                    as late_orders,
    count(distinct order_id) - sum(is_late)         as on_time_orders,
    round(
        (count(distinct order_id) - sum(is_late))::numeric
        / count(distinct order_id) * 100
    , 1)                                            as on_time_pct,
    round(
        sum(is_late)::numeric
        / count(distinct order_id) * 100
    , 1)                                            as late_pct

from {{ ref('fct_orders') }}
where shipper_id is not null
group by 1, 2
order by total_orders desc