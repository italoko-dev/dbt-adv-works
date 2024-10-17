with
    cred_card as (
        select
            cast(creditcardid as int) as cred_card_pk
            , cast(md5(
                creditcardid:: string || '-' || cardnumber :: string)
            as string) as cred_card_sk 
            , cast(cardtype as string) as cred_card_type
        from {{ source('adv_works', 'creditcard') }}
    )    

select *   
from cred_card
