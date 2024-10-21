with 
    sales_header_reason as (
        select
            cast({{ dbt_utils.generate_surrogate_key(
                ['salesorderid', 'salesreasonid']
            ) }} as string) as sales_order_sk
            , cast(salesorderid as int) as sales_order_fk
            , cast(salesreasonid as int) sales_reason_fk
        from {{ source('adv_works', 'salesorderheadersalesreason') }}
    )  

select *
from sales_header_reason
