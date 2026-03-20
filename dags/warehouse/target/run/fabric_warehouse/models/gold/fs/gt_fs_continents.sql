

    
        
            delete from "DWH"."gold"."gt_fs_continents"
            where (
                sk_fs_continent) in (
                select (sk_fs_continent)
                from "DWH"."gold"."gt_fs_continents__dbt_tmp"
            )
    OPTION (LABEL = 'dbt-fabric-dw');

        
    

    insert into "DWH"."gold"."gt_fs_continents" ("sk_fs_continent", "sk_fs_continent_master", "dbt_id_sourcesystem", "dbt_id_business_key", "dbt_current_flag", "dbt_valid_from", "dbt_valid_to", "mod_valid_from", "mod_valid_to", "dbt_updated_at", "Continent", "Country", "updated_at")
    (
        select "sk_fs_continent", "sk_fs_continent_master", "dbt_id_sourcesystem", "dbt_id_business_key", "dbt_current_flag", "dbt_valid_from", "dbt_valid_to", "mod_valid_from", "mod_valid_to", "dbt_updated_at", "Continent", "Country", "updated_at"
        from "DWH"."gold"."gt_fs_continents__dbt_tmp"
    )
    OPTION (LABEL = 'dbt-fabric-dw');

