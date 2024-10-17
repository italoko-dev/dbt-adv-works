with
    geo_country as (
        select
            cast(countryregioncode as string) as country_pk
            , cast(name as string) as country_name
        from {{ source('adv_works', 'countryregion') }}        
    )

select *
from geo_country
