
{% snapshot st_fs_customers %}

{{
  config(
    target_schema='silver',
    strategy='check',
    check_cols='all',
    unique_key='source_businesskey',
    updated_at='ingestion_date',
    invalidate_hard_deletes=True,
    schema='silver',
    alias='st_fs_customers',
    file_format='delta',

  )
}}


WITH ranked_source AS ( 

SELECT
        CustomerID,
        CustomerFirstName,
        CustomerLastName,
        PhoneNumber,
        FaxNumber,
        Email,
        Address,
        City,
        State,
        Zip,
        Country,
        source_updated_at,
        source_businesskey,
        source_system,
        ingestion_date,
        cast(ingestion_year as int)  as ingestion_year,
        cast(ingestion_month as int) as ingestion_month ,
        cast(ingestion_day as int) as ingestion_day,
        ROW_NUMBER() OVER (PARTITION BY [source_businesskey],[source_system] ORDER BY [ingestion_date] DESC,[source_updated_at] DESC) AS [dbt_row_num]
    FROM {{ source('landing', 'cust') }}

/* DATA CLEANSING, FORMATTING, DEDUPLICATION */


) SELECT 
    * 
  FROM 
    ranked_source
  WHERE 
    [dbt_row_num] = 1

{% endsnapshot %}