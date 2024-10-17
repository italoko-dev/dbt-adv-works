with
    sales_reason as (
        select
           salesreasonid as sales_reason_pk
           , name as sales_reason_name
           , reason_type as sales_reason_type
        from {{ source('adv_works', 'salesreason') }}        
    )

select *
from sales_reason
