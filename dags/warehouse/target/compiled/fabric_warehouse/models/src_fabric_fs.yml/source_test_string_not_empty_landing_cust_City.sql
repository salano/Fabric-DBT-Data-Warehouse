
    select 
        City
    from 
        "bronze"."dbo"."bt_fs_customers"
    where 
        TRIM (City) = ''
