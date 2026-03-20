
    select 
        Zip
    from 
        "bronze"."dbo"."bt_fs_customers"
    where 
        TRIM (Zip) = ''
