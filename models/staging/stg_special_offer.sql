with 
    special_offer as (
        select
            cast(specialofferid as int) as special_offer_pk
            , cast(md5(
                specialofferid::string || '-' || description
            ) as string) as special_offer_sk
            , cast(description as string) as special_offer_description
            , cast(discountpct as numeric(18,2)) as special_offer_discount_pct
            , cast(type as string) as special_offer_type
            , cast(category as string) as special_offer_category
            , cast(startdate as date) as special_offer_start_date
            , cast(enddate as date) as special_offer_end_date
        from {{ source('adv_works', 'specialoffer') }}
    )  

select * 
from special_offer
