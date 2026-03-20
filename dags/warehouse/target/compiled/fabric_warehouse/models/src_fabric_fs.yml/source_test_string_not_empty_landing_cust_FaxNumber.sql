
    select 
        FaxNumber
    from 
        "bronze"."dbo"."bt_fs_customers"
    where 
        TRIM (FaxNumber) = ''
