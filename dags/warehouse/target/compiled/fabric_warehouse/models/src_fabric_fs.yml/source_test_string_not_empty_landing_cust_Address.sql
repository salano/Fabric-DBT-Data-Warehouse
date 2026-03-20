
    select 
        Address
    from 
        "bronze"."dbo"."bt_fs_customers"
    where 
        TRIM (Address) = ''
