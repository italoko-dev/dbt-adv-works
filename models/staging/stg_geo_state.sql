with
    geo_state as (
        select
            cast(stateprovinceid as int) as state_province_pk
            , cast(name as string) as state_province_name
            , cast(countryregioncode as string) as state_province_country_code
        from {{ source('adv_works', 'stateprovince') }}        
    )

select *
from geo_state
