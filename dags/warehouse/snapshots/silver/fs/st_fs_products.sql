
{% snapshot st_fs_products %}

{{
  config(
    target_schema='silver',
    strategy='check',
    check_cols='all',
    unique_key='source_businesskey',
    updated_at='ingestion_date',
    invalidate_hard_deletes=True,
    schema='silver',
    alias='st_fs_products',
    file_format='delta',

  )
}}


WITH ranked_source AS ( 

    SELECT

        ProductID,
        ProductName,
        Color,
        Brand,
        Size,
        Barcode,
        TaxRate,
        UnitPrice,
        SupplierId,
        source_updated_at,
        source_businesskey,
        source_system,
        ingestion_date,
        ingestion_year,
        ingestion_month,
        ingestion_day,
        ROW_NUMBER() OVER (PARTITION BY [source_businesskey],[source_system] ORDER BY [ingestion_date] DESC,[source_updated_at] DESC) AS [dbt_row_num]
    FROM {{ source('landing', 'prod') }}

/* DATA CLEANSING, FORMATTING, DEDUPLICATION */


) SELECT 
    * 
  FROM 
    ranked_source
  WHERE 
    [dbt_row_num] = 1

{% endsnapshot %}