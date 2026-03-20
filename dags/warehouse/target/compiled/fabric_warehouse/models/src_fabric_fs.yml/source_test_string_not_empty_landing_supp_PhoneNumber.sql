
    select 
        PhoneNumber
    from 
        "bronze"."dbo"."bt_fs_suppliers"
    where 
        TRIM (PhoneNumber) = ''
