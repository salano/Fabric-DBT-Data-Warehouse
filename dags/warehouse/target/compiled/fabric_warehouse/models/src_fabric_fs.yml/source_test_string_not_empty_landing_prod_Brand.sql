
    select 
        Brand
    from 
        "bronze"."dbo"."bt_fs_products"
    where 
        TRIM (Brand) = ''
