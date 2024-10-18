with
    customer as (
        select 
            customer_pk
            , person_fk
            , territory_fk
        from {{ ref('stg_customer') }}
    )
    
    , person as (
        select 
            person_pk
            , person_full_name
            , person_type
        from {{ ref('stg_person') }}
    ) 

    , customer_detail as (
        select 
            customer.customer_pk
            -- , customer.person_fk

            -- , person.person_pk
            , person.person_full_name as customer_name
            -- , person.person_type
        from customer inner join person on 
            customer.person_fk = person.person_pk
    )

select *
from customer_detail
