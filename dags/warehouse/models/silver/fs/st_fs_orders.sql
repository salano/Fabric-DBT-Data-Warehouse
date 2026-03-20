{{
  config(
    materialized='incremental',
    unique_key='OrderID'
  )
}}


WITH ranked_source AS ( 


    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY [source_businesskey],[source_system] ORDER BY [ingestion_date] DESC,[source_updated_at] DESC) AS [dbt_row_num]
    FROM {{ source('landing', 'orders') }}

/* DATA CLEANSING, FORMATTING, DEDUPLICATION */


) SELECT 
    *
  FROM 
    ranked_source

{% if is_incremental() %}
    where LastEditedWhen > (select coalesce(max(LastEditedWhen),'1900-01-01 00:00:00') from {{ this }})
    and [dbt_row_num] = 1
{% endif %}