
    select 
        ProductName
    from 
        "bronze"."dbo"."bt_fs_products"
    where 
        TRIM (ProductName) = ''
