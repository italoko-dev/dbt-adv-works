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
            , case 
                when sales_order_status = 1 then 'In Process'
                when sales_order_status = 2 then 'Approved'
                when sales_order_status = 3 then 'Backordered'
                when sales_order_status = 4 then 'Rejected'
                when sales_order_status = 5 then 'Shipped'
                when sales_order_status = 6 then 'Cancelled'
            end as sales_order_status
            , sales_order_freight
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
            , sales_header.sales_order_freight
        from sales_detail left join sales_header on 
            sales_detail.sales_order_header_fk = sales_header.sales_order_header_pk
    )

    , metrics as (
        select
            sales_order_date
            , sales_order_header_pk
            , sales_order_date_fk
            , sales_order_product_fk
            , sales_order_special_offer_fk
            , sales_order_customer_fk
            , sales_order_sales_person_fk
            , sales_order_territory_fk
            , sales_order_cred_card_fk
            , sales_order_address_id_fk
            , sales_order_qtd
            , sales_order_prod_unit_price
            , sales_order_prod_discount
            , sales_order_freight / (count(sales_order_header_pk) over (
                    partition by sales_order_header_pk
                )
            ) as sales_order_freight_by_product
            , sales_order_qtd 
                * sales_order_prod_unit_price
                * (1 - sales_order_prod_discount)
            as sales_order_subtotal -- liquido
            , sales_order_qtd * sales_order_prod_unit_price as sales_order_total_due -- bruto
            , sales_order_status
        from detailed_sale
    )
select  *
from metrics
