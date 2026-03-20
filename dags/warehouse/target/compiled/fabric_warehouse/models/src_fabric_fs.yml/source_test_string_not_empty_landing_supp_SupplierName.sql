
    select 
        SupplierName
    from 
        "bronze"."dbo"."bt_fs_suppliers"
    where 
        TRIM (SupplierName) = ''
