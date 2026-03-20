
    select 
        Country
    from 
        "bronze"."dbo"."bt_fs_customers"
    where 
        TRIM (Country) = ''
