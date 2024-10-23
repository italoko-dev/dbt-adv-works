with 
    prod_subcategory as (
        select
            cast(productsubcategoryid as int) as prod_subcategory_pk
            , cast(productcategoryid as int) as prod_category_fk
            , cast(name as string) as prod_subcategory
        from {{ source('adv_works', 'productsubcategory') }}
    )

select *
from prod_subcategory
