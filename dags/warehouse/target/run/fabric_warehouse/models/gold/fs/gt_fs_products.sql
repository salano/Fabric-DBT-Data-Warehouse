

    
        
            delete from "DWH"."gold"."gt_fs_products"
            where (
                sk_fs_product) in (
                select (sk_fs_product)
                from "DWH"."gold"."gt_fs_products__dbt_tmp"
            )
    OPTION (LABEL = 'dbt-fabric-dw');

        
    

    insert into "DWH"."gold"."gt_fs_products" ("sk_fs_product", "sk_fs_product_master", "dbt_id_sourcesystem", "dbt_id_business_key", "dbt_current_flag", "dbt_valid_from", "dbt_valid_to", "mod_valid_from", "mod_valid_to", "dbt_updated_at", "ProductName", "Color", "Brand", "Size", "Barcode", "TaxRate", "UnitPrice", "SupplierId")
    (
        select "sk_fs_product", "sk_fs_product_master", "dbt_id_sourcesystem", "dbt_id_business_key", "dbt_current_flag", "dbt_valid_from", "dbt_valid_to", "mod_valid_from", "mod_valid_to", "dbt_updated_at", "ProductName", "Color", "Brand", "Size", "Barcode", "TaxRate", "UnitPrice", "SupplierId"
        from "DWH"."gold"."gt_fs_products__dbt_tmp"
    )
    OPTION (LABEL = 'dbt-fabric-dw');

