with orders as (

    select * from {{ ref('stg_orders') }}

),

payments as (

    select * from {{ ref('stg_stripe__payments')}}
)

select
    orders.order_id,
    customer_id,
    sum(case when payments.status = "success" then amount end) as amount
from orders
inner join payments using (order_id)
group by 1,2