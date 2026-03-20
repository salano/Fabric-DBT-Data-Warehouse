-- back compat for old kwarg name
  
  
  
        
            
	    
	    
            
        
    

    

    merge into "DWH"."gold"."gt_fs_orders" as DBT_INTERNAL_DEST
        using "DWH"."gold"."gt_fs_orders__dbt_tmp" as DBT_INTERNAL_SOURCE
        on ((DBT_INTERNAL_SOURCE.OrderID = DBT_INTERNAL_DEST.OrderID))

    
    when matched then update set
        "sk_fs_customer" = DBT_INTERNAL_SOURCE."sk_fs_customer","sk_fs_product" = DBT_INTERNAL_SOURCE."sk_fs_product","sk_fs_supplier" = DBT_INTERNAL_SOURCE."sk_fs_supplier","sk_fs_continent" = DBT_INTERNAL_SOURCE."sk_fs_continent","OrderID" = DBT_INTERNAL_SOURCE."OrderID","ProductID" = DBT_INTERNAL_SOURCE."ProductID","SupplierId" = DBT_INTERNAL_SOURCE."SupplierId","CustomerID" = DBT_INTERNAL_SOURCE."CustomerID","Quantity" = DBT_INTERNAL_SOURCE."Quantity","UnitPrice" = DBT_INTERNAL_SOURCE."UnitPrice","TaxRate" = DBT_INTERNAL_SOURCE."TaxRate","total_revenue" = DBT_INTERNAL_SOURCE."total_revenue","total_tax" = DBT_INTERNAL_SOURCE."total_tax","total_revenue_with_tax" = DBT_INTERNAL_SOURCE."total_revenue_with_tax","LastEditedBy" = DBT_INTERNAL_SOURCE."LastEditedBy","LastEditedWhen" = DBT_INTERNAL_SOURCE."LastEditedWhen","LastEditedDate" = DBT_INTERNAL_SOURCE."LastEditedDate","source_updated_at" = DBT_INTERNAL_SOURCE."source_updated_at","source_businesskey" = DBT_INTERNAL_SOURCE."source_businesskey","source_system" = DBT_INTERNAL_SOURCE."source_system","ingestion_date" = DBT_INTERNAL_SOURCE."ingestion_date","ingestion_year" = DBT_INTERNAL_SOURCE."ingestion_year","ingestion_month" = DBT_INTERNAL_SOURCE."ingestion_month","ingestion_day" = DBT_INTERNAL_SOURCE."ingestion_day"
    

    when not matched then insert
        ("sk_fs_customer", "sk_fs_product", "sk_fs_supplier", "sk_fs_continent", "OrderID", "ProductID", "SupplierId", "CustomerID", "Quantity", "UnitPrice", "TaxRate", "total_revenue", "total_tax", "total_revenue_with_tax", "LastEditedBy", "LastEditedWhen", "LastEditedDate", "source_updated_at", "source_businesskey", "source_system", "ingestion_date", "ingestion_year", "ingestion_month", "ingestion_day")
    values
        ("sk_fs_customer", "sk_fs_product", "sk_fs_supplier", "sk_fs_continent", "OrderID", "ProductID", "SupplierId", "CustomerID", "Quantity", "UnitPrice", "TaxRate", "total_revenue", "total_tax", "total_revenue_with_tax", "LastEditedBy", "LastEditedWhen", "LastEditedDate", "source_updated_at", "source_businesskey", "source_system", "ingestion_date", "ingestion_year", "ingestion_month", "ingestion_day")

;
