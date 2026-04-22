with product_sales as (
    select
        product_id,
        product_name,
        category_name,
        supplier_name,
        stock_status,
        is_discontinued,

        count(distinct order_id)                    as total_orders,
        sum(quantity)                               as total_quantity_sold,
        round(sum(net_revenue)::numeric, 2)         as total_net_revenue,
        round(avg(unit_price)::numeric, 2)          as avg_unit_price,
        round(avg(discount)::numeric * 100, 2)      as avg_discount_pct,
        count(distinct customer_id)                 as unique_customers

    from {{ ref('fct_orders') }}
    group by 1, 2, 3, 4, 5, 6
),

with_share as (
    select
        *,
        round(
            total_net_revenue
            / sum(total_net_revenue) over () * 100
        , 2)                                        as revenue_share_pct,

        rank() over (order by total_net_revenue desc)
                                                    as revenue_rank,

        rank() over (
            partition by category_name
            order by total_net_revenue desc
        )                                           as category_rank,

        case
            when sum(total_net_revenue) over (
                order by total_net_revenue desc
                rows between unbounded preceding and current row
            ) / sum(total_net_revenue) over () <= 0.80
            then 'A'
            when sum(total_net_revenue) over (
                order by total_net_revenue desc
                rows between unbounded preceding and current row
            ) / sum(total_net_revenue) over () <= 0.95
            then 'B'
            else 'C'
        end                                         as abc_class

    from product_sales
)

select * from with_share
order by revenue_rank