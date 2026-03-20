with customers_data as 
    (
        select CustomerID, ingestion_date,CONVERT(VARCHAR(19), ingestion_date) AS formatted_time
        from 
        {{source('landing','cust')}}
    )
select 
    *
FROM
    customers_data