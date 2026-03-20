
    select 
        CustomerLastName
    from 
        "bronze"."dbo"."bt_fs_customers"
    where 
        TRIM (CustomerLastName) = ''
