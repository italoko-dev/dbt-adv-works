with
    sales_2011 as (
        select 
            sum(sales_order_qtd * sales_order_prod_unit_price) as sales_order_total_due -- = 12646112.1607
        from {{ ref('fact_sales_order_detail') }}
        where 
            left(sales_order_date, 4) = '2011'
    )
select sales_order_total_due
from sales_2011
where sales_order_total_due not between 12646112.1500 and 12646112.1700