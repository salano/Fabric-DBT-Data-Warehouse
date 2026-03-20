
    select 
        Country
    from 
        "bronze"."dbo"."bt_fs_continents"
    where 
        TRIM (Country) = ''
