with
    sales_header as ( -- cnt 31465
        select
            sales_order_header_pk
            , to_number(to_char(sales_order_date, 'YYYYMMDD')) as sales_order_date_fk
            , sales_order_customer_fk
            , sales_order_sales_person_fk
            , sales_order_territory_fk
            , sales_order_cred_card_fk
            , sales_order_address_id_fk
                        
            , sales_order_date
            , sales_order_status
            
            , sales_order_subtotal
            , sales_order_freight
            , sales_order_taxamt
            , sales_order_total_due
        from {{ ref('stg_sales_order_header') }}
    )

    , sales_detail as ( -- 121317  produtos vendidos (COUNT(SALES_ORDER_HEADER_FK)), em 31465 vendas (COUNT(DISTINCT SALES_ORDER_HEADER_FK))
        select
            sales_order_header_fk
            , sales_order_detail_pk
            , sales_order_product_fk
            , sales_order_special_offer_fk
            
            , sales_order_qtd
            , sales_order_prod_unit_price
            , sales_order_prod_discount
        from {{ ref('stg_sales_order_detail') }} 
    )

    , detailed_sale as (
        select
            sales_header.sales_order_date
            , sales_header.sales_order_header_pk
            -- sales_detail.sales_order_header_fk
            -- , sales_detail.sales_order_detail_pk
            , sales_header.sales_order_date_fk
            , sales_detail.sales_order_product_fk
            , sales_detail.sales_order_qtd
            , sales_detail.sales_order_prod_unit_price
            , sales_detail.sales_order_prod_discount
            , sales_detail.sales_order_special_offer_fk
            
            , sales_header.sales_order_customer_fk
            , sales_header.sales_order_sales_person_fk
            , sales_header.sales_order_territory_fk
            , sales_header.sales_order_cred_card_fk
            , sales_header.sales_order_address_id_fk
            
            , sales_header.sales_order_status
            , sales_header.sales_order_subtotal
            , sales_header.sales_order_taxamt
            , sales_header.sales_order_freight
            , sales_header.sales_order_total_due
        from sales_detail left join sales_header on 
            sales_detail.sales_order_header_fk = sales_header.sales_order_header_pk
    )
select  *
from detailed_sale
