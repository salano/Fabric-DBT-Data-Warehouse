
{% snapshot st_fs_suppliers %}

{{
  config(
    target_schema='silver',
    strategy='check',
    check_cols='all',
    unique_key='source_businesskey',
    updated_at='ingestion_date',
    invalidate_hard_deletes=True,
    schema='silver',
    alias='st_fs_suppliers',
    file_format='delta',

  )
}}


WITH ranked_source AS ( 


    SELECT
        SupplierId,
        SupplierName,
        PhoneNumber,
        cast(Email as varchar(150)) Email,
        ValidFrom,
        ValidTo,
        source_updated_at,
        source_businesskey,
        source_system,
        ingestion_date,
        ingestion_year,
        ingestion_month,
        ingestion_day,
        ROW_NUMBER() OVER (PARTITION BY [source_businesskey],[source_system] ORDER BY [ingestion_date] DESC,[source_updated_at] DESC) AS [dbt_row_num]
    FROM {{ source('landing', 'supp') }}

/* DATA CLEANSING, FORMATTING, DEDUPLICATION */


) SELECT 
    * 
  FROM 
    ranked_source
  WHERE 
    [dbt_row_num] = 1

{% endsnapshot %}