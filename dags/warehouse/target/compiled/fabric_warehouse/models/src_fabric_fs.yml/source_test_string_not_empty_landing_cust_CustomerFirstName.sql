
    select 
        CustomerFirstName
    from 
        "bronze"."dbo"."bt_fs_customers"
    where 
        TRIM (CustomerFirstName) = ''
