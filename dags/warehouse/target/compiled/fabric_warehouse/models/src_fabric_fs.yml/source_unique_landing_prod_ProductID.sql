
    
    

select
    ProductID as unique_field,
    count(*) as n_records

from "bronze"."dbo"."bt_fs_products"
where ProductID is not null
group by ProductID
having count(*) > 1


