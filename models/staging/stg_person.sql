with
    person as (
        select
            cast(businessentityid as int) as person_pk
            , cast(md5(
                businessentityid::string || '-' || firstname || ' '
                || coalesce(middlename, '') || ' ' || lastname
            ) as string) as person_sk
            , cast(persontype as string) as person_type
            , cast(
                firstname || ' ' || coalesce(middlename, '') || ' ' || lastname
            as string) as person_full_name
        from {{ source('adv_works', 'person') }}
    )    

select *   
from person
