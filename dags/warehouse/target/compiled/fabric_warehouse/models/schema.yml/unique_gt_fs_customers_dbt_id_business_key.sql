
    
    

select
    dbt_id_business_key as unique_field,
    count(*) as n_records

from "DWH"."gold"."gt_fs_customers"
where dbt_id_business_key is not null
group by dbt_id_business_key
having count(*) > 1


