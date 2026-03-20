{{
    config(
        materialized = "view",
        schema='gold'
    )
}}

WITH
    base_query AS (
        /*---------------------------------------------------------------------------
        1) Base Query: Retrieves core columns from gold.orders_facts and gold.products
        ---------------------------------------------------------------------------*/
        SELECT
            o.OrderID,
            o.LastEditedWhen,
            o.CustomerID,
            o.total_revenue as Revenue,
            o.Quantity,
            p.dbt_id_business_key ProductID,
            p.ProductName,
            p.UnitPrice
        FROM gold.gt_fs_orders as o
            LEFT JOIN gold.gt_fs_products p ON o.sk_fs_product = p.sk_fs_product
        WHERE
            o.sk_fs_product IS NOT NULL -- only consider valid order dates
    ),
    product_aggregations AS (
        /*---------------------------------------------------------------------------
        2) Product Aggregations: Summarizes key metrics at the product level
        ---------------------------------------------------------------------------*/
        SELECT
            ProductID,
            ProductName,
            UnitPrice,
            DATEDIFF(
                MONTH,
                MIN(LastEditedWhen),
                MAX(LastEditedWhen)
            ) AS lifespan,
            MAX(LastEditedWhen) AS last_order_date,
            COUNT(DISTINCT OrderID) AS total_orders,
            COUNT(DISTINCT CustomerID) AS total_customers,
            round(SUM(Revenue), 2) AS total_revenue,
            round(SUM(Quantity), 2) AS total_quantity,
            ROUND(
                AVG(
                    CAST(Revenue AS FLOAT) / NULLIF(Quantity, 0)
                ),
                1
            ) AS avg_selling_price
        FROM base_query
        GROUP BY
            ProductID,
            ProductName,
            UnitPrice
    )

/*---------------------------------------------------------------------------
3) Final Query: Combines all product results into one output
---------------------------------------------------------------------------*/
SELECT
    ProductID,
    ProductName,
    UnitPrice,
    last_order_date,
    DATEDIFF(
        MONTH,
        last_order_date,
        GETDATE ()
    ) AS recency_in_months,
    CASE
        WHEN total_revenue > 50000 THEN 'High-Performer'
        WHEN total_revenue >= 10000 THEN 'Mid-Range'
        ELSE 'Low-Performer'
    END AS product_segment,
    lifespan,
    total_orders,
    total_revenue,
    total_quantity,
    total_customers,
    avg_selling_price,
    -- Average Order Revenue (AOR)
    CASE
        WHEN total_orders = 0 THEN 0
        ELSE round(
            total_revenue / total_orders,
            2
        )
    END AS avg_order_revenue,

-- Average Monthly Revenue
CASE
    WHEN lifespan = 0 THEN total_revenue
    ELSE round(total_revenue / lifespan, 2)
END AS avg_monthly_revenue
FROM product_aggregations