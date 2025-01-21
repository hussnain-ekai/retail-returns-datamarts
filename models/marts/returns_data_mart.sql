
{{
  config(
    materialized = 'table',
    schema = 'data_mart',
    tags = ['returns', 'refunds'],
    post_hook = [],
    pre_hook = []
  )
}}

with returns as (
    select * from {{ ref('returns') }}
),
financial_transactions as (
    select * from {{ ref('financial_transactions') }}
),
orders as (
    select * from {{ ref('orders') }}
)

select
    ret.RETURN_ID,
    ord.SALES_CHANNEL,
    COUNT(ret.RETURN_ID) AS total_returns,
    SUM(ft.AMOUNT) AS total_refund_amount,
    AVG(ft.AMOUNT) AS average_refund_amount_per_return,
    SUM(ft.AMOUNT) AS refunds_by_sales_channel,
    AVG(DATEDIFF(day, ret.RETURN_INITIATED_DATE, ret.REFUND_ISSUED_DATE)) AS average_time_to_issue_refund
from returns ret
join financial_transactions ft on ret.RETURN_ID = ft.RETURN_ID
join orders ord on ret.ORDER_ID = ord.ORDER_ID
group by ret.RETURN_ID, ord.SALES_CHANNEL
