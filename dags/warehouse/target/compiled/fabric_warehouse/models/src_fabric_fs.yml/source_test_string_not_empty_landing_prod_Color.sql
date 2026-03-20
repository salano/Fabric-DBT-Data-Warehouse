
    select 
        Color
    from 
        "bronze"."dbo"."bt_fs_products"
    where 
        TRIM (Color) = ''
