


WITH snapshot_table AS (
	SELECT 
        HASHBYTES('SHA2_256',
    lower(convert(varchar(50), hashbytes('md5', coalesce(convert(varchar(8000), concat(coalesce(cast(driving.source_businesskey as VARCHAR(8000)), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(driving.source_system as VARCHAR(8000)), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(driving.dbt_valid_from as VARCHAR(8000)), '_dbt_utils_surrogate_key_null_'))), '')), 2))
) AS sk_fs_product,
        HASHBYTES('SHA2_256',
    lower(convert(varchar(50), hashbytes('md5', coalesce(convert(varchar(8000), concat(coalesce(cast(driving.source_businesskey as VARCHAR(8000)), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(driving.source_system as VARCHAR(8000)), '_dbt_utils_surrogate_key_null_'))), '')), 2))
) AS sk_fs_product_master,
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
        ProductName,
        Color,
        Brand,
        Size,
        Barcode,
        TaxRate,
        UnitPrice,
        SupplierId
	FROM "DWH"."silver"."st_fs_products" driving
    WHERE 1=1
    

    UNION ALL

	SELECT 
        -- dummy record for artifical foreign keys
        -1  AS sk_fs_product,
        -1  AS sk_fs_product_master,
        'N/A'  AS dbt_id_sourcesystem,
        -1  AS dbt_id_business_key,
        1  AS dbt_current_flag,
        CAST('1900-01-01' AS DATE) AS dbt_valid_from,
        CAST('3000-12-31' AS DATE) AS dbt_valid_to,
        CAST('1900-01-01' AS DATE) AS mod_valid_from,
        CAST('3000-12-31' AS DATE) AS mod_valid_to,
		CAST('1900-01-01' AS DATE) AS dbt_updated_at,
        'N/A' AS ProductName,
        'N/A' AS Color,
        'N/A' AS Brand,
        'N/A' AS Size,
        'N/A' AS Barcode,
        -1.0 AS TaxRate,
        -1.0 AS UnitPrice,
        -1 AS SupplierId
)
SELECT 
    
        sk_fs_product,
        sk_fs_product_master,
        dbt_id_sourcesystem,
        dbt_id_business_key,
        dbt_current_flag,
        dbt_valid_from,
        dbt_valid_to,
        mod_valid_from,
        mod_valid_to,
        dbt_updated_at,
        ProductName,
        Color,
        Brand,
        Size,
        Barcode,
        TaxRate,
        UnitPrice,
        SupplierId
FROM snapshot_table
WHERE 1=1