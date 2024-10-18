with
    country as (
        select 
            country_pk
            , country_name as country
        from {{ ref('stg_geo_country') }}
    )

    , state as (
        select 
            state_province_country_code
            , state_province_pk
            , state_province_name as state
        from {{ ref('stg_geo_state') }}
    )

    , city as (
        select 
            address_pk
            , state_province_fk
            , address_city_name as city
        from {{ ref('stg_geo_city') }} geo_city
    )

    , geography_detail as (
        select
            city.address_pk
            , city.city 
            , state.state
            , country.*
        from city 
        left join state on 
            city.state_province_fk = state.state_province_pk
        left join country on 
            state.state_province_country_code = country.country_pk
    )

select *
from geography_detail
