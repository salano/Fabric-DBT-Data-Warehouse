
    
    with test_main_sql as (
  
    
    
    

select
    SupplierId as unique_field,
    count(*) as n_records

from "bronze"."dbo"."bt_fs_suppliers"
where SupplierId is not null
group by SupplierId
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