with
    sales_order_header as (
        select
            cast(salesorderid as int) as sales_order_header_pk
            , cast(customerid as int) as sales_order_customer_fk
            , cast(salespersonid as int) as sales_order_sales_person_fk
            , cast(territoryid as int) as sales_order_territory_fk
            , cast(creditcardid as int) as sales_order_cred_card_fk
            
            , cast(orderdate as date) as sales_order_date
            , cast(status as int) as sales_order_status
            
            , cast(subtotal as numeric(18,4)) as sales_order_subtotal
            , cast(taxamt as numeric(18,4)) as sales_order_taxamt
            , cast(totaldue as numeric(18,4)) as sales_order_total_due
        from {{ source('adv_works', 'salesorderheader') }}        
    )

select *
from sales_order_header
