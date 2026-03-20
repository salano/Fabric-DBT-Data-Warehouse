
    
    

select
    Country as unique_field,
    count(*) as n_records

from "bronze"."dbo"."bt_fs_continents"
where Country is not null
group by Country
having count(*) > 1


