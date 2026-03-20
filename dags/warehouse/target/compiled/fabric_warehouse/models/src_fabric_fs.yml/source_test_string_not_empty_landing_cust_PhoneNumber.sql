
    select 
        PhoneNumber
    from 
        "bronze"."dbo"."bt_fs_customers"
    where 
        TRIM (PhoneNumber) = ''
