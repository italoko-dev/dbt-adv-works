with
    sales_order_detail as (
        select 
            cast(salesorderid as int) as sales_order_header_fk
            , cast(salesorderdetailid as int) as sales_order_detail_pk
            , cast(productid as int) as sales_order_product_fk
            , cast(specialofferid as int) as sales_order_special_offer_fk
            
            , cast(orderqty as int) as sales_order_qtd
            , cast(unitprice as numeric(18,4)) as sales_order_prod_unit_price
            , cast(unitpricediscount as numeric(18,4)) as sales_order_prod_discount
        from {{ source('adv_works', 'salesorderdetail') }}        
    )

select *
from sales_order_detail
