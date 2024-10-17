with
    sales_person as (
        select
            businessentityid as sales_person_pk
            , territoryid as sales_person_territory_fk
            -- , salesquota as sales_person_quota
            -- , bonus as sales_person_bonus
            -- , salesytd as sales_person_ytd
            -- , saleslastyear as sales_person_last_year
        from {{ source('adv_works', 'salesperson') }}        
    )

select *
from sales_person
