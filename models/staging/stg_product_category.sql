with 
    prod_category as (
        select
            cast(productcategoryid as int) as prod_category_pk
            , cast(name as string) as prod_category
        from {{ source('adv_works', 'productcategory') }}
    )

select *
from prod_category
