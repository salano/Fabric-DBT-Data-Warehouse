
    
    

select
    Email as unique_field,
    count(*) as n_records

from "bronze"."dbo"."bt_fs_customers"
where Email is not null
group by Email
having count(*) > 1


