with
    sales_reason as (
        select
            cast({{ dbt_utils.generate_surrogate_key(
                ['salesreasonid', 'name', 'reasontype']
            ) }} as string) as sales_reason_sk
           , cast(salesreasonid as int) as sales_reason_pk
           , cast(name as string) as sales_reason_name
           , cast(reasontype as string) as sales_reason_type
        from {{ source('adv_works', 'salesreason') }}        
    )

select *
from sales_reason
