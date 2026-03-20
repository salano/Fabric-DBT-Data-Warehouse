
    
    with test_main_sql as (
  
    
    
    

select
    sk_fs_customer as unique_field,
    count(*) as n_records

from "DWH"."gold"."gt_fs_customers"
where sk_fs_customer is not null
group by sk_fs_customer
having count(*) > 1



  
  ),
  dbt_internal_test as (
    select  * from test_main_sql
  )
  select
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from dbt_internal_test