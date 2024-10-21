with 
    products as ( --504
        select
            product_sk
            , product_pk
            , product_subcategory_fk
            , product_name
            , product_number
            , product_color
            , product_standard_cost
            , product_list_price
        from {{ ref('stg_product') }}
    )

    , category as (
        select
            prod_category_pk
            , prod_category
        from {{ ref('stg_product_category') }}
    )

    , subcategory as ( 
        select
            prod_subcategory_pk
            , prod_category_fk
            , prod_subcategory
        from {{ ref('stg_product_subcategory') }}
    )

    , product_detailed as (
        select 
            prod.product_sk
            , prod.product_pk
            , prod.product_name
            , prod.product_number

            , category.prod_category
            , subcategory.prod_subcategory

            , prod.product_color
            , prod.product_standard_cost
            , prod.product_list_price
        from products prod 
        left join subcategory on 
            prod.product_subcategory_fk = subcategory.prod_subcategory_pk
        left join category on 
            subcategory.prod_category_fk = category.prod_category_pk
    )
select *
from product_detailed
