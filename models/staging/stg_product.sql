with 
    product as (
        select 
            cast({{ dbt_utils.generate_surrogate_key(
                ['productid', 'productnumber']
            ) }} as string) as product_sk
            , cast(productid as int) as product_pk
            , cast(productsubcategoryid as int )product_subcategory_fk
            , cast(name as string) as product_name
            , cast(productnumber as string) as product_number
            , cast(color as string) as product_color
            , cast(standardcost as numeric(18,2)) as product_standard_cost
            , cast(listprice as numeric(18,2)) as product_list_price
        from {{ source('adv_works', 'product') }}
    )

select * 
from product
