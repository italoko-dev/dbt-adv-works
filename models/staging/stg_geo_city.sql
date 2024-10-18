with
    geo_address as (
        select
            cast(addressid as int) as address_pk
            , cast(stateprovinceid as int) as state_province_fk

            , cast(city as string) as address_city_name
        from {{ source('adv_works', 'address') }}        
    )

select *
from geo_address
