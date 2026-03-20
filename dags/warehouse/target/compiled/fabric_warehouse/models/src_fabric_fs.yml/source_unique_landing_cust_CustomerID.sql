
    
    

select
    CustomerID as unique_field,
    count(*) as n_records

from "bronze"."dbo"."bt_fs_customers"
where CustomerID is not null
group by CustomerID
having count(*) > 1


