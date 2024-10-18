with
    sales_reason as (
        select
            sales_reason_pk
            , sales_reason_name
            , sales_reason_type
        from {{ ref('stg_sales_reason') }}
    )
    
    , sales_order_reason as (
        select 
            sales_order_fk
            , sales_reason_fk
        from {{ ref('sales_order_header_sales_reason') }}
    )

    , sales_reason_detail as (
        select 
            sales_order_reason.sales_order_fk
            , sales_reason.sales_reason_name
            , sales_reason.sales_reason_type
        from sales_order_reason left join sales_reason on 
            sales_order_reason.sales_reason_fk = sales_reason.sales_reason_pk
    )

select *
from sales_reason_detail
