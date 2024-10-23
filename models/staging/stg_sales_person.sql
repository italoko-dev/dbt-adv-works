with
    sales_person as (
        select
            cast({{ dbt_utils.generate_surrogate_key(
                ['businessentityid', 'territoryid']
            ) }} as string) as sales_person_sk
            , cast(businessentityid as int) as sales_person_pk
            , cast(territoryid as int) as sales_person_territory_fk
            -- , salesquota as sales_person_quota
            -- , bonus as sales_person_bonus
            -- , salesytd as sales_person_ytd
            -- , saleslastyear as sales_person_last_year
        from {{ source('adv_works', 'salesperson') }}
    )

select *
from sales_person
