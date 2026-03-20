
  
    
    
    USE [DWH];
    
    

    EXEC('create view "gold"."gt_fs_dates__dbt_temp__dbt_tmp_vw" as 

WITH date_spine AS
( -- 103 = dd/mm/yyyy

    SELECT dates = DATEADD(DAY,[value] - 1,CONVERT(DATE,''01/01/1980'', 103))
    FROM GENERATE_SERIES(
           1
          ,DATEDIFF(DAY
                   ,CONVERT(DATE,''01/01/1980'', 103)
                   ,DATEADD(YEAR,50,CONVERT(DATE,GETDATE(), 103))) + 1
          ,1)

)
SELECT
   SK_Date        = dates
  ,[DateDesc]	= CONVERT(CHAR(11),dates,120)
  ,[WeekNbr]      = DATEPART(ISO_WEEK,dates)
  ,[MonthNbr]	= DATEPART(MONTH,dates)
  ,[QuarterNbr]	= DATEPART(QUARTER,dates)
  ,[Year]		= DATEPART(YEAR, dates)
  ,[DayName]		= CAST(DATENAME(WEEKDAY, dates) AS VARCHAR(20))
  ,[WeekName]	= ''W'' + CAST(DATEPART(ISO_WEEK,dates) AS VARCHAR(2))
  ,[MonthName]	= CAST(DATENAME(MONTH,dates) AS VARCHAR(20))
  ,[QuarterName]	= ''Q'' + CAST(DATEPART(QUARTER,dates) AS CHAR(1))
  ,[YearWeek]	= CAST(DATEPART(YEAR, dates) AS CHAR(4)) + 
                       RIGHT(''0'' + CAST(DATEPART(ISO_WEEK, dates) AS VARCHAR(2)),2)
  ,[YearWeekDesc] = CAST(DATEPART(YEAR, dates) AS CHAR(4)) + '' W'' + CAST(DATEPART(ISO_WEEK,dates) AS VARCHAR(2))
  ,[YearMonth]	= CAST(DATEPART(YEAR, dates) AS CHAR(4)) + 
                        SUBSTRING(CONVERT(VARCHAR(6),dates,112),5,2)
  ,[YearMonthDesc] = CAST(DATEPART(YEAR, dates) AS CHAR(4)) + ''-'' + 
                         RIGHT(''0'' + CAST(DATEPART(MONTH, dates) AS VARCHAR(2)),2)
  ,[YearMonthDescFull]	= CAST(DATENAME(MONTH,dates) AS VARCHAR(20)) + '' '' + CAST(DATEPART(YEAR, dates) AS CHAR(4))
  ,[YearQuarter]	= CAST(DATEPART(YEAR, dates) AS CHAR(4)) + CAST(DATEPART(QUARTER, dates) AS CHAR(1))
  ,[YearQuarterDesc] = CAST(DATEPART(YEAR, dates) AS CHAR(4)) + '' '' +
                             ''Q'' + CAST(DATEPART(QUARTER,dates) AS CHAR(1))    
  ,[IsMonthFirstDay] = IIF(dates = DATEADD(MONTH, DATEDIFF(MONTH, 0, dates), 0), ''yes'', ''no'' )
  ,[IsMonthLastDay] = IIF(dates = EOMONTH(dates,0),''yes'',''no'')
  ,[MonthFirstDay] = DATEADD(DAY,1,EOMONTH(dates,-1))
  ,[MonthLastDay]	 = EOMONTH(dates,0)
  ,[DayOfWeek]	 = DATEPART(WEEKDAY, dates)
  ,[DayOfMonth]	 = DATEPART(DAY, dates)
  ,[DayOfQuarter]	 = DATEDIFF(DAY,DATEADD(QUARTER,DATEDIFF(QUARTER,0,dates),0),dates) + 1
  ,[DayOfYear]	 = DATEPART(DAYOFYEAR, dates)
FROM date_spine d;;');




    
    
            EXEC('CREATE TABLE "DWH"."gold"."gt_fs_dates__dbt_temp" AS SELECT * FROM "DWH"."gold"."gt_fs_dates__dbt_temp__dbt_tmp_vw" 
    OPTION (LABEL = ''dbt-fabric-dw'');
');
        

    

  
  