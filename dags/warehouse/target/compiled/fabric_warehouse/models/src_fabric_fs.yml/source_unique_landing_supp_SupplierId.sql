
    
    

select
    SupplierId as unique_field,
    count(*) as n_records

from "bronze"."dbo"."bt_fs_suppliers"
where SupplierId is not null
group by SupplierId
having count(*) > 1


