with
    sales_person as (
        select 
            sales_person_pk
            , sales_person_territory_fk
        from {{ ref('stg_sales_person') }}
    )
    
    , employee as (
        select 
            employee_pk
            , employee_job_title
        from {{ ref('stg_employee') }}
    )
    
    , person as (
        select 
            person_pk
            , person_full_name
            , person_type
        from {{ ref('stg_person') }}
    )

    , sales_person_detail as (
        select 
            sales_person.sales_person_pk
            , person.person_full_name as sales_person_name
            , employee.employee_job_title
        from sales_person 
        left join employee on 
            sales_person.sales_person_pk = employee.employee_pk
        left join person on 
            employee.employee_pk = person.person_pk
    )

select *
from sales_person_detail
