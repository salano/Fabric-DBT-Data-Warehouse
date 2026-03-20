
    
    with test_main_sql as (
  
    
    
    



select dbt_current_flag
from "DWH"."gold"."gt_fs_customers"
where dbt_current_flag is null



  
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