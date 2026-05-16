{{
  config(
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['order_date', 'dim_customer_sk', 'dim_product_sk', 'channel'],
    partition_by=['order_date'],
    on_schema_change='append_new_columns'
  )
}}

with sales as (
    select
        order_date,
        dim_customer_sk,
        dim_product_sk,
        store_id,
        channel,
        currency,
        sum(qty)         as units,
        sum(line_amount) as gross_revenue,
        count(distinct order_id) as orders
    from {{ source('silver_retail', 'fact_sales') }}
    {% if is_incremental() %}
        where order_date >= (select coalesce(max(order_date), '1900-01-01') from {{ this }})
    {% endif %}
    group by 1, 2, 3, 4, 5, 6
),

with_dims as (
    select
        s.*,
        c.customer_segment,
        c.country,
        p.category,
        p.brand
    from sales s
    left join {{ source('silver_retail', 'dim_customer') }} c
      on s.dim_customer_sk = c.dim_customer_sk
    left join {{ source('silver_retail', 'dim_product') }} p
      on s.dim_product_sk = p.dim_product_sk
)

select * from with_dims
