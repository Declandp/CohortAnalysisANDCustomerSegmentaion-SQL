-- Cleaning data
-- Total records =392692

-- Selecting only records which have CustomerID and Qty and UnitPrice are both > 0 and also records which have a proper date time value
-- Creating a temporary table for the entire clean data
CREATE TEMPORARY TABLE retail_main

WITH  online_retail AS (
SELECT *
FROM `online retail`
WHERE customerID !=0 AND  STR_TO_DATE(InvoiceDate, '%Y-%m-%d %H:%i:%s') IS NOT NULL
)
-- Selecting only records which have a postive Qty sold and unit price
, Qty_price AS (
SELECT * 
FROM online_retail
WHERE Quantity > 0 AND UnitPrice > 0 AND InvoiceDate 
)
-- Checking for Duplicate records and removing them
,duplicate_chk AS (
SELECT * , ROW_NUMBER() OVER(PARTITION BY InvoiceNo,StockCode,Quantity ORDER BY InvoiceDAte) AS dup_flag
FROM Qty_price)
SELECT *
FROM duplicate_chk
WHERE dup_flag=1;


-- BEGIN Cohort Analysis

SELECT * FROM retail_main; -- 389669 row(s) returned

-- UNique Identifier (CustomerID)
-- Initial start Date  (First Invoice Date)
-- Revenue Data

CREATE TEMPORARY TABLE Cohort
SELECT
    CustomerID,
    MIN(InvoiceDate) AS first_purchase_date,
    DATE(CONCAT(YEAR(MIN(InvoiceDate)), '-', (MONTH(MIN(InvoiceDate))), '-01')) AS Cohort_Date
FROM retail_main
GROUP BY CustomerID;

SELECT * FROM Cohort;

-- Create Cohort Index(It is the number of months that have passed since the customers first engagement)
CREATE TEMPORARY TABLE Cohort_retention
SELECT 	mmm.*,
		year_diff*12 +month_diff+1 AS Cohort_index
FROM (
SELECT 	mm.*,
		invoice_year - Cohort_year AS year_diff,
       invoice_month - Cohort_month AS month_diff
FROM
(SELECT 	rm.*,
		c.Cohort_Date,
        YEAR(rm.InvoiceDate) AS invoice_year,
        Month(rm.InvoiceDate) AS invoice_month,
        YEAR(c.Cohort_date) AS Cohort_year,
        MONTH(c.Cohort_date) AS Cohort_month
FROM retail_main rm
left JOIN Cohort c
ON rm.CustomerID = c.CustomerID
)mm
)mmm;

SELECT * FROM Cohort_retention;

-- Pivoting the data based on the cohort index

CREATE TEMPORARY TABLE cohort_pivot
SELECT 
   tbl.Cohort_date,
    SUM(CASE WHEN Cohort_index = 1 THEN cnt ELSE 0 END) AS `1`,
    SUM(CASE WHEN Cohort_index = 2 THEN cnt ELSE 0 END) AS `2`,
    SUM(CASE WHEN Cohort_index = 3 THEN cnt ELSE 0 END) AS `3`,
    SUM(CASE WHEN Cohort_index = 4 THEN cnt ELSE 0 END) AS `4`,
    SUM(CASE WHEN Cohort_index = 5 THEN cnt ELSE 0 END) AS `5`,
    SUM(CASE WHEN Cohort_index = 6 THEN cnt ELSE 0 END) AS `6`,
    SUM(CASE WHEN Cohort_index = 7 THEN cnt ELSE 0 END) AS `7`,
    SUM(CASE WHEN Cohort_index = 8 THEN cnt ELSE 0 END) AS `8`,
    SUM(CASE WHEN Cohort_index = 9 THEN cnt ELSE 0 END) AS `9`,
    SUM(CASE WHEN Cohort_index = 10 THEN cnt ELSE 0 END) AS `10`,
    SUM(CASE WHEN Cohort_index = 11 THEN cnt ELSE 0 END) AS `11`,
    SUM(CASE WHEN Cohort_index = 12 THEN cnt ELSE 0 END) AS `12`,
    SUM(CASE WHEN Cohort_index = 13 THEN cnt ELSE 0 END) AS `13`
FROM (select 
		COUNT(DISTINCT CustomerID) cnt,
		Cohort_Date,
		cohort_index
	from Cohort_retention
    GROUP BY 2,3
    ORDER BY cohort_index)tbl
GROUP BY Cohort_Date;

SELECT * FROM cohort_pivot;



-- Creating cohort Retention rate
SELECT 
    Cohort_Date,
    (1.0 * `1`/`1` * 100) as `1`, 
    1.0 * `2`/`1` * 100 as `2`, 
    1.0 * `3`/`1` * 100 as `3`,  
    1.0 * `4`/`1` * 100 as `4`,  
    1.0 * `5`/`1` * 100 as `5`, 
    1.0 * `6`/`1` * 100 as `6`, 
    1.0 * `7`/`1` * 100 as `7`, 
    1.0 * `8`/`1` * 100 as `8`, 
    1.0 * `9`/`1` * 100 as `9`, 
    1.0 * `10`/`1` * 100 as `10`,   
    1.0 * `11`/`1` * 100 as `11`,  
    1.0 * `12`/`1` * 100 as `12`,  
    1.0 * `13`/`1` * 100 as `13`
FROM `cohort_pivot`
ORDER BY Cohort_Date;


-- Customer segmentation USiNG RFM method and segmenting using Quartile Segmentation

-- Calculating the Recency,Frequency and Monetary Values
With rfm AS( 
SELECT DISTINCT	CustomerID,
		MIN(datediff(now(),InvoiceDate)) AS Recency,
        Count(DISTINCT InvoiceNo) AS Frequency,
        SUM(Quantity*UnitPrice),0 AS Monetary
FROM retail_main
GROUP BY 1
ORDER BY 1
)
-- Creating scores for R F M using quartile segmentation
,rfm_score AS (
SELECT 	*,
		NTILE(4) OVER(ORDER BY Recency) AS Recency_quartile,
		NTILE(4) OVER(ORDER BY Frequency) AS Frequency_quartile,
		NTILE(4) OVER(ORDER BY Monetary) AS Monetary_quartile
FROM rfm
) 
-- Segmenting customers based on certain quartile values of the RFM scores
SELECT 	*,
		CASE
        WHEN Recency_quartile = 4 AND Frequency_quartile = 4 AND Monetary_quartile = 4 THEN 'Champion'
        WHEN Recency_quartile = 4 AND Frequency_quartile = 4 AND Monetary_quartile = 3 THEN 'High Recency, High Frequency, Moderate Monetary'
        WHEN Recency_quartile = 4 AND Frequency_quartile = 3 AND Monetary_quartile = 4 THEN 'High Recency, Moderate Frequency, High Monetary'
        WHEN Recency_quartile = 3 AND Frequency_quartile = 4 AND Monetary_quartile = 4 THEN 'Moderate Recency, High Frequency, High Monetary'
        WHEN Recency_quartile = 4 AND Frequency_quartile = 3 AND Monetary_quartile = 3 THEN 'High Recency, Moderate Frequency, Moderate Monetary'
		ELSE 'Other'
    END AS Segment
FROM rfm_score;