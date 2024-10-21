with
    customer as (
        select
            cast({{ dbt_utils.generate_surrogate_key(
                ['customerid']) 
            }} as string) as customer_sk
            , cast(customerid as int) as customer_pk
            , cast(personid as int) as person_fk
            , cast(territoryid as int) as territory_fk
        from {{ source('adv_works', 'customer') }}        
    )

select *
from customer
