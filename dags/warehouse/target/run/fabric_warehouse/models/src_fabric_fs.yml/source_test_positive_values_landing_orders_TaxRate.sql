
    
    with test_main_sql as (
  
    
SELECT *
FROM "bronze"."dbo"."bt_fs_orders"
WHERE TaxRate <= 0

  
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