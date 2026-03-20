
  
    
    
    USE [DWH];
    
    

    EXEC('create view "gold"."gt_fs_orders__dbt_tmp_vw" as 


WITH silver_table AS ( 

    SELECT
        coalesce(cust.sk_fs_customer, -1) sk_fs_customer,
        coalesce(prod.sk_fs_product, -1) sk_fs_product,
        coalesce(supp.sk_fs_supplier, -1) sk_fs_supplier,
        coalesce(cont.sk_fs_continent, -1) sk_fs_continent,
        orders.OrderID,
        orders.ProductID,
        orders.SupplierId,
        orders.CustomerID,
        orders.Quantity,
        orders.UnitPrice,
        orders.TaxRate,
        round(cast(orders.Quantity as float) * orders.UnitPrice,2) AS total_revenue,
        case when orders.TaxRate = 0 then 0
                else round(cast(orders.Quantity as float) * orders.UnitPrice * cast((orders.TaxRate/100) as float),2)
        end AS total_tax,
        round(cast(orders.Quantity as float) * orders.UnitPrice,2) + case when orders.TaxRate = 0 then 0
                else round(cast(orders.Quantity as float) * orders.UnitPrice * cast((orders.TaxRate/100) as float),2)
        end as total_revenue_with_tax,
        LastEditedBy,
        LastEditedWhen,
        LastEditedDate,
        source_updated_at,
        source_businesskey,
        source_system,
        ingestion_date,
        ingestion_year,
        ingestion_month,
        ingestion_day
    FROM
    "DWH"."silver"."st_fs_orders" orders
    left join "DWH"."gold"."gt_fs_customers" cust
        on orders.CustomerID = cust.dbt_id_business_key
    left join "DWH"."gold"."gt_fs_products" prod
        on orders.ProductID = prod.dbt_id_business_key
    left join "DWH"."gold"."gt_fs_continents" cont
        on cust.Country = cont.dbt_id_business_key
    left join "DWH"."silver"."gt_fs_suppliers" supp
        on orders.SupplierId = supp.dbt_id_business_key


) SELECT 
    *
  FROM 
    silver_table

;');




    
    
            EXEC('CREATE TABLE "DWH"."gold"."gt_fs_orders" AS SELECT * FROM "DWH"."gold"."gt_fs_orders__dbt_tmp_vw" 
    OPTION (LABEL = ''dbt-fabric-dw'');
');
        

    

  