

    
        
            delete from "DWH"."gold"."gt_fs_customers"
            where (
                sk_fs_customer) in (
                select (sk_fs_customer)
                from "DWH"."gold"."gt_fs_customers__dbt_tmp"
            )
    OPTION (LABEL = 'dbt-fabric-dw');

        
    

    insert into "DWH"."gold"."gt_fs_customers" ("sk_fs_customer", "sk_fs_customer_master", "dbt_id_sourcesystem", "dbt_id_business_key", "dbt_current_flag", "dbt_valid_from", "dbt_valid_to", "mod_valid_from", "mod_valid_to", "dbt_updated_at", "CustomerFirstName", "CustomerLastName", "PhoneNumber", "FaxNumber", "Email", "Address", "City", "State", "Zip", "Country")
    (
        select "sk_fs_customer", "sk_fs_customer_master", "dbt_id_sourcesystem", "dbt_id_business_key", "dbt_current_flag", "dbt_valid_from", "dbt_valid_to", "mod_valid_from", "mod_valid_to", "dbt_updated_at", "CustomerFirstName", "CustomerLastName", "PhoneNumber", "FaxNumber", "Email", "Address", "City", "State", "Zip", "Country"
        from "DWH"."gold"."gt_fs_customers__dbt_tmp"
    )
    OPTION (LABEL = 'dbt-fabric-dw');

