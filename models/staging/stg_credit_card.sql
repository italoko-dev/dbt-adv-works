with
    cred_card as (
        select
            cast({{ dbt_utils.generate_surrogate_key(
                ['creditcardid', 'cardnumber']
            ) }} as string) as cred_card_sk
            , cast(creditcardid as int) as cred_card_pk
            , cast(cardtype as string) as cred_card_type
        from {{ source('adv_works', 'creditcard') }}
    )    

select *
from cred_card
