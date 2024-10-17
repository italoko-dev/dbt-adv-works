with
    customer as (
        select
            customerid as customer_pk
            , personid as person_fk
            , territoryid as territory_fk
        from {{ source('adv_works', 'customer') }}        
    )

select *
from customer
