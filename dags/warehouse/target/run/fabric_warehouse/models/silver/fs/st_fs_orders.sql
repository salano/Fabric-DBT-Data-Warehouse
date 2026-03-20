-- back compat for old kwarg name
  
  
  
        
            
	    
	    
            
        
    

    

    merge into "DWH"."silver"."st_fs_orders" as DBT_INTERNAL_DEST
        using "DWH"."silver"."st_fs_orders__dbt_tmp" as DBT_INTERNAL_SOURCE
        on ((DBT_INTERNAL_SOURCE.OrderID = DBT_INTERNAL_DEST.OrderID))

    
    when matched then update set
        "OrderID" = DBT_INTERNAL_SOURCE."OrderID","ProductID" = DBT_INTERNAL_SOURCE."ProductID","SupplierId" = DBT_INTERNAL_SOURCE."SupplierId","CustomerID" = DBT_INTERNAL_SOURCE."CustomerID","Quantity" = DBT_INTERNAL_SOURCE."Quantity","UnitPrice" = DBT_INTERNAL_SOURCE."UnitPrice","TaxRate" = DBT_INTERNAL_SOURCE."TaxRate","LastEditedBy" = DBT_INTERNAL_SOURCE."LastEditedBy","LastEditedWhen" = DBT_INTERNAL_SOURCE."LastEditedWhen","LastEditedDate" = DBT_INTERNAL_SOURCE."LastEditedDate","source_updated_at" = DBT_INTERNAL_SOURCE."source_updated_at","source_businesskey" = DBT_INTERNAL_SOURCE."source_businesskey","source_system" = DBT_INTERNAL_SOURCE."source_system","ingestion_date" = DBT_INTERNAL_SOURCE."ingestion_date","ingestion_year" = DBT_INTERNAL_SOURCE."ingestion_year","ingestion_month" = DBT_INTERNAL_SOURCE."ingestion_month","ingestion_day" = DBT_INTERNAL_SOURCE."ingestion_day","dbt_row_num" = DBT_INTERNAL_SOURCE."dbt_row_num"
    

    when not matched then insert
        ("OrderID", "ProductID", "SupplierId", "CustomerID", "Quantity", "UnitPrice", "TaxRate", "LastEditedBy", "LastEditedWhen", "LastEditedDate", "source_updated_at", "source_businesskey", "source_system", "ingestion_date", "ingestion_year", "ingestion_month", "ingestion_day", "dbt_row_num")
    values
        ("OrderID", "ProductID", "SupplierId", "CustomerID", "Quantity", "UnitPrice", "TaxRate", "LastEditedBy", "LastEditedWhen", "LastEditedDate", "source_updated_at", "source_businesskey", "source_system", "ingestion_date", "ingestion_year", "ingestion_month", "ingestion_day", "dbt_row_num")

;
