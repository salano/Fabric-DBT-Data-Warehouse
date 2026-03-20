
    
    

select
    OrderID as unique_field,
    count(*) as n_records

from "bronze"."dbo"."bt_fs_orders"
where OrderID is not null
group by OrderID
having count(*) > 1


