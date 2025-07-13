{{
    config(
        materialized='incremental',
        unique_key = 'order_id',
        partition_by = {
            "field": 'order_date'
            "datatype": "date"
            "granularity": "day"
        },
        incremental_strategy = 'insert_overwrite',
        on_schema_change = 'sync_all_columns'
    )
}}

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
{% if is_incremental() %}
    where order_date > (select max(order_date) from {{ this }}) 
{% endif %}
group by 1,2