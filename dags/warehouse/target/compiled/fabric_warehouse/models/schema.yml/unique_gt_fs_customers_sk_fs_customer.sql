
    
    

select
    sk_fs_customer as unique_field,
    count(*) as n_records

from "DWH"."gold"."gt_fs_customers"
where sk_fs_customer is not null
group by sk_fs_customer
having count(*) > 1


