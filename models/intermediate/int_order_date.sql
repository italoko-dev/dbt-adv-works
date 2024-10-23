with  
    sales_header as (
        select
            sales_order_header_pk
            , sales_order_date
        from {{ ref('stg_sales_order_header') }}
    )

    , sales_header_dates as (
        select
            *
            ,  year(sales_order_date) as sales_order_date_year
            ,  month(sales_order_date) as sales_order_date_month
            ,  to_char(sales_order_date, 'YYYY-MM') sales_order_date_year_month
            ,  day(sales_order_date) as sales_order_date_day
        from sales_header
    )

select *
from sales_header_dates
