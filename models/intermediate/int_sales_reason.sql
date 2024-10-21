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

    , agg_sales_reason_detail as (
        select 
            sales_order_fk
            , listagg(sales_reason_name, ', ') within group (order by sales_reason_name) as sales_reasons
            , listagg(sales_reason_type, ', ') within group (order by sales_reason_name) as sales_reasons_type
        from sales_reason_detail
        group by sales_order_fk
    )

select  *
from agg_sales_reason_detail
