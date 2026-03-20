
      
  
    
  
    
  
    
  
    
  
    
  
    
  
    
  
    
  
    
  
    
  
    
  
    
  
    
  
    
  
    
  
    
  
    
  
    
  
    
  
    
  
    
  update DBT_INTERNAL_DEST
  set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to
  from "silver"."st_fs_products" as DBT_INTERNAL_DEST
  inner join "silver"."st_fs_products__dbt_tmp" as DBT_INTERNAL_SOURCE
  on DBT_INTERNAL_SOURCE.dbt_scd_id = DBT_INTERNAL_DEST.dbt_scd_id
  where DBT_INTERNAL_SOURCE.dbt_change_type in ('update', 'delete')
  
    and DBT_INTERNAL_DEST.dbt_valid_to is null
  
  
    OPTION (LABEL = 'dbt-fabric-dw');


  insert into "silver"."st_fs_products" ("ProductID", "ProductName", "Color", "Brand", "Size", "Barcode", "TaxRate", "UnitPrice", "SupplierId", "source_updated_at", "source_businesskey", "source_system", "ingestion_date", "ingestion_year", "ingestion_month", "ingestion_day", "dbt_row_num", "dbt_updated_at", "dbt_valid_from", "dbt_valid_to", "dbt_scd_id")
  select DBT_INTERNAL_SOURCE."ProductID", DBT_INTERNAL_SOURCE."ProductName", DBT_INTERNAL_SOURCE."Color", DBT_INTERNAL_SOURCE."Brand", DBT_INTERNAL_SOURCE."Size", DBT_INTERNAL_SOURCE."Barcode", DBT_INTERNAL_SOURCE."TaxRate", DBT_INTERNAL_SOURCE."UnitPrice", DBT_INTERNAL_SOURCE."SupplierId", DBT_INTERNAL_SOURCE."source_updated_at", DBT_INTERNAL_SOURCE."source_businesskey", DBT_INTERNAL_SOURCE."source_system", DBT_INTERNAL_SOURCE."ingestion_date", DBT_INTERNAL_SOURCE."ingestion_year", DBT_INTERNAL_SOURCE."ingestion_month", DBT_INTERNAL_SOURCE."ingestion_day", DBT_INTERNAL_SOURCE."dbt_row_num", DBT_INTERNAL_SOURCE."dbt_updated_at", DBT_INTERNAL_SOURCE."dbt_valid_from", DBT_INTERNAL_SOURCE."dbt_valid_to", DBT_INTERNAL_SOURCE."dbt_scd_id" from "silver"."st_fs_products__dbt_tmp" as DBT_INTERNAL_SOURCE
  where  DBT_INTERNAL_SOURCE.dbt_change_type = 'insert'
  
    OPTION (LABEL = 'dbt-fabric-dw');


  