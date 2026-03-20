
    
    with test_main_sql as (
  
    
    
    

select
    dbt_id_business_key as unique_field,
    count(*) as n_records

from "DWH"."gold"."gt_fs_customers"
where dbt_id_business_key is not null
group by dbt_id_business_key
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