


WITH snapshot_table AS (
	SELECT 
        HASHBYTES('SHA2_256',
    lower(convert(varchar(50), hashbytes('md5', coalesce(convert(varchar(8000), concat(coalesce(cast(driving.source_businesskey as VARCHAR(8000)), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(driving.source_system as VARCHAR(8000)), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(driving.dbt_valid_from as VARCHAR(8000)), '_dbt_utils_surrogate_key_null_'))), '')), 2))
) AS sk_fs_customer,
        HASHBYTES('SHA2_256',
    lower(convert(varchar(50), hashbytes('md5', coalesce(convert(varchar(8000), concat(coalesce(cast(driving.source_businesskey as VARCHAR(8000)), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(driving.source_system as VARCHAR(8000)), '_dbt_utils_surrogate_key_null_'))), '')), 2))
) AS sk_fs_customer_master,
        driving.source_system AS dbt_id_sourcesystem,
        driving.source_businesskey AS dbt_id_business_key,
        CAST(
            CASE 
                WHEN driving.dbt_valid_to IS NULL THEN 1
                ELSE 0
            END AS BIT) AS dbt_current_flag,
        CAST(driving.dbt_valid_from AS DATE) AS dbt_valid_from,
        CAST(COALESCE(driving.dbt_valid_to, '3000-12-31') AS DATE) AS dbt_valid_to,
        CAST(driving.dbt_valid_from AS DATE) AS mod_valid_from, 
        CAST(CASE
                WHEN driving.dbt_valid_to IS NULL 
                THEN '3000-12-31'
             ELSE DATEADD(DAY, -1, driving.dbt_valid_to)
        END AS DATE) AS mod_valid_to,
		CAST(driving.dbt_updated_at AS DATE) AS dbt_updated_at,
        CustomerFirstName,
        CustomerLastName,
        PhoneNumber,
        FaxNumber,
        Email,
        Address,
        City,
        State,
        Zip,
        Country
	FROM "DWH"."silver"."st_fs_customers" driving

    

    
        --Add only dummy record
    Union ALL
    
	SELECT 
        -- dummy record for artifical foreign keys
        -1  AS sk_fs_customer,
        -1  AS sk_fs_customer_master,
        'N/A'  AS dbt_id_sourcesystem,
        -1  AS dbt_id_business_key,
        1  AS dbt_current_flag,
        CAST('1900-01-01' AS DATE) AS dbt_valid_from,
        CAST('3000-12-31' AS DATE) AS dbt_valid_to,
        CAST('1900-01-01' AS DATE) AS mod_valid_from,
        CAST('3000-12-31' AS DATE) AS mod_valid_to,
		CAST('1900-01-01' AS DATE) AS dbt_updated_at,
        'N/A' AS CustomerFirstName,
        'N/A' AS CustomerLastName,
        'N/A' AS PhoneNumber,
        'N/A' AS FaxNumber,
        'N/A' AS Email,
        'N/A' AS Address,
        'N/A' AS City,
        'N/A' AS State,
        'N/A' AS Zip,
        'N/A' AS Country

         
)
    select
        sk_fs_customer,
        sk_fs_customer_master,
        dbt_id_sourcesystem,
        dbt_id_business_key,
        dbt_current_flag,
        dbt_valid_from,
        dbt_valid_to,
        mod_valid_from,
        mod_valid_to,
        dbt_updated_at,
        CustomerFirstName,
        CustomerLastName,
        PhoneNumber,
        FaxNumber,
        Email,
        Address,
        City,
        State,
        Zip,
        Country
    FROM snapshot_table