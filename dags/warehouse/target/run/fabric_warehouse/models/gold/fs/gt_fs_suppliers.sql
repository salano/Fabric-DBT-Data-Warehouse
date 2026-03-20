

    
        
            delete from "DWH"."silver"."gt_fs_suppliers"
            where (
                sk_fs_supplier) in (
                select (sk_fs_supplier)
                from "DWH"."silver"."gt_fs_suppliers__dbt_tmp"
            )
    OPTION (LABEL = 'dbt-fabric-dw');

        
    

    insert into "DWH"."silver"."gt_fs_suppliers" ("sk_fs_supplier", "sk_fs_supplier_master", "dbt_id_sourcesystem", "dbt_id_business_key", "dbt_current_flag", "dbt_valid_from", "dbt_valid_to", "mod_valid_from", "mod_valid_to", "dbt_updated_at", "SupplierName", "PhoneNumber", "Email")
    (
        select "sk_fs_supplier", "sk_fs_supplier_master", "dbt_id_sourcesystem", "dbt_id_business_key", "dbt_current_flag", "dbt_valid_from", "dbt_valid_to", "mod_valid_from", "mod_valid_to", "dbt_updated_at", "SupplierName", "PhoneNumber", "Email"
        from "DWH"."silver"."gt_fs_suppliers__dbt_tmp"
    )
    OPTION (LABEL = 'dbt-fabric-dw');

