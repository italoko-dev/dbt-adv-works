with
    cred_card as (
        select
            cred_card_sk
            , cred_card_pk
            , cred_card_type
        from {{ ref('stg_credit_card') }}
    )    

select *
from cred_card
