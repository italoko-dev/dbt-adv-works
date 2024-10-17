with
    employee as (
        select
            businessentityid as employee_pk
            , nationalidnumber as employee_nat_id_number
            , jobtitle as employee_job_title
        from {{ source('adv_works', 'employee') }}        
    )

select *
from employee
