
    select 
        LastEditedWhen
    from 
        "bronze"."dbo"."bt_fs_orders"
    where 
       LastEditedWhen > getdate()
