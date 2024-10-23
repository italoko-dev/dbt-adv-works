with  
    raw_generated_data as (
        -- min(sales_order_date) = 2011-05-31, max(sales_order_date) = 2014-06-30
        {{ dbt_date.get_date_dimension("2011-01-01", "2020-12-31") }}
    )
    
    , selected_generated_data as (
        select
            to_number(to_char(date_day, 'YYYYMMDD')) as date_day_key
            , date_day as sales_order_date 
            , year_number as  sales_order_date_year
            , quarter_of_year as sales_order_date_quarter
            , month_of_year as sales_order_date_month
            , month_name as sales_order_date_month_name
            , day_of_week as sales_order_date_week
            , day_of_week_name as sales_order_date_week_name
            , day_of_week_name_short as sales_order_date_week_name_short
            , day_of_month as sales_order_date_day
        from raw_generated_data
    )

select *
from selected_generated_data    
