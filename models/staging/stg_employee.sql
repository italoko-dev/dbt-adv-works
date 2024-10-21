with
    employee as (
        select
            cast(businessentityid as int) as employee_pk
            , cast(nationalidnumber as string) as employee_nat_id_number
            , cast(jobtitle as string) as employee_job_title
        from {{ source('adv_works', 'employee') }}        
    )

select *
from employee
