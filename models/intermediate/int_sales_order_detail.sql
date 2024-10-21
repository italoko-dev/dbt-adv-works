with  
    sales as (
        select 
            sales_order_date
            , sales_order_header_pk
            , sales_order_product_fk
            , sales_order_customer_fk
            , sales_order_address_id_fk
            , sales_order_sales_person_fk
            , sales_order_cred_card_fk
            , sales_order_qtd
            , sales_order_prod_unit_price
            , sales_order_prod_discount
            , sales_order_special_offer_fk
            , sales_order_status
            , sales_order_subtotal
            , sales_order_taxamt
            , sales_order_freight
            , sales_order_total_due
        from {{ ref('int_sales') }}
    )

    , product as (
        select 
            product_pk
            , product_sk
        from {{ ref('int_product') }}
    )

    , geography as (
        select 
            address_pk
            , address_sk
        from {{ ref('int_geography') }}
    ) 

    , customer as (
        select
            customer_pk
            , customer_sk
        from {{ ref('int_customer') }}
    )

    , sales_person as (
        select 
            sales_person_pk
            , sales_person_sk
        from {{ ref('int_sales_person') }}
    )

    , sales_reason as (
        select 
            sales_order_fk
            , sales_reason_sk
        from {{ ref('int_sales_reason') }}
    )

    , cred_card as (
        select
            cred_card_pk
            , cred_card_sk
        from {{ ref('int_credit_card') }}
    )

    , sales_detailed as (
        select 
            sales.sales_order_date
            , sales.sales_order_header_pk
            , prod.product_sk
            , customer.customer_sk
            , geo.address_sk
            , sales_reason.sales_reason_sk
            , sales_person.sales_person_sk
            , cred_card.cred_card_sk

            , sales.sales_order_qtd
            , sales.sales_order_prod_unit_price
            , sales.sales_order_prod_discount
            , sales.sales_order_status
            , sales.sales_order_subtotal
            , sales.sales_order_taxamt
            , sales.sales_order_freight
            , sales.sales_order_total_due
        from sales
        left join product prod on 
            sales.sales_order_product_fk = prod.product_pk
        left join geography geo on 
            sales.sales_order_address_id_fk = geo.address_pk
        left join customer on 
            sales.sales_order_customer_fk = customer.customer_pk
        left join sales_person on 
            sales.sales_order_sales_person_fk = sales_person.sales_person_pk
        left join cred_card on 
            sales.sales_order_cred_card_fk = cred_card.cred_card_pk
        left join sales_reason on 
            sales.sales_order_header_pk = sales_reason.sales_order_fk
    )
select *
from sales_detailed
