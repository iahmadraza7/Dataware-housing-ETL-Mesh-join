USE Metro_DW;

-- Query1
SELECT 
    DATE_FORMAT(Order_Date, '%Y-%m') AS Month,
    CASE 
        WHEN WEEKDAY(Order_Date) IN (0, 1, 2, 3, 4) THEN 'Weekday'
        ELSE 'Weekend'
    END AS Day_Type,
    Product_Name,
    SUM(Sale) AS Total_Revenue
FROM DW_Transactions
GROUP BY Month, Day_Type, Product_Name
ORDER BY Month, Total_Revenue DESC
LIMIT 5;

-- Query 2
SELECT 
    Store_Name,
    CONCAT('Q', QUARTER(Order_Date)) AS Quarter,
    YEAR(Order_Date) AS Year,
    SUM(Sale) AS Quarterly_Revenue,
    ROUND((SUM(Sale) - LAG(SUM(Sale), 1) OVER (PARTITION BY Store_Name ORDER BY YEAR(Order_Date), QUARTER(Order_Date))) / 
          LAG(SUM(Sale), 1) OVER (PARTITION BY Store_Name ORDER BY YEAR(Order_Date), QUARTER(Order_Date)) * 100, 2) AS Growth_Rate
FROM DW_Transactions
WHERE YEAR(Order_Date) = 2017
GROUP BY Store_Name, Year, Quarter
ORDER BY Store_Name, Quarter;

-- Query 3
SELECT 
    Store_Name,
    Supplier_Name,
    -- Assuming a column for product category, adjust if you have one
    Product_Name,  -- Or replace with `Product_Category` if available
    SUM(Sale) AS Total_Sales
FROM DW_Transactions
JOIN Products ON DW_Transactions.Product_ID = Products.ProductID
JOIN Suppliers ON Products.supplierID = Suppliers.supplierID
JOIN Stores ON Products.storeID = Stores.storeID
GROUP BY Store_Name, Supplier_Name, Product_Name
ORDER BY Store_Name, Supplier_Name, Total_Sales DESC;

-- Query 4
SELECT 
    Product_Name,
    CASE 
        WHEN MONTH(Order_Date) IN (12, 1, 2) THEN 'Winter'
        WHEN MONTH(Order_Date) IN (3, 4, 5) THEN 'Spring'
        WHEN MONTH(Order_Date) IN (6, 7, 8) THEN 'Summer'
        WHEN MONTH(Order_Date) IN (9, 10, 11) THEN 'Fall'
    END AS Season,
    Store_Name,
    SUM(Sale) AS Total_Sales
FROM DW_Transactions
GROUP BY Product_Name, Season, Store_Name
ORDER BY Product_Name, Season, Store_Name;


-- Query 5
WITH Monthly_Revenue AS (
    SELECT 
        Store_Name,
        Supplier_Name,
        DATE_FORMAT(Order_Date, '%Y-%m') AS Month,
        SUM(Sale) AS Revenue
    FROM DW_Transactions
    GROUP BY Store_Name, Supplier_Name, Month
),
Volatility AS (
    SELECT 
        Store_Name,
        Supplier_Name,
        Month,
        Revenue,
        LAG(Revenue) OVER (PARTITION BY Store_Name, Supplier_Name ORDER BY Month) AS Prev_Revenue,
        ROUND(((Revenue - LAG(Revenue) OVER (PARTITION BY Store_Name, Supplier_Name ORDER BY Month)) / 
              LAG(Revenue) OVER (PARTITION BY Store_Name, Supplier_Name ORDER BY Month)) * 100, 2) AS Volatility_Percentage
    FROM Monthly_Revenue
)
SELECT * FROM Volatility;


-- Query 6
SELECT 
    T1.Product_Name AS Product_A,
    T2.Product_Name AS Product_B,
    COUNT(*) AS Frequency
FROM DW_Transactions T1
JOIN DW_Transactions T2 
    ON T1.Order_ID = T2.Order_ID 
    AND T1.Product_ID < T2.Product_ID  
GROUP BY T1.Product_Name, T2.Product_Name
ORDER BY Frequency DESC
LIMIT 5;




-- Query 7
SELECT 
    Store_Name,
    Supplier_Name,
    Product_Name,
    YEAR(Order_Date) AS Year,
    SUM(Sale) AS Total_Revenue
FROM DW_Transactions
JOIN Products ON DW_Transactions.Product_ID = Products.ProductID
JOIN Suppliers ON Products.supplierID = Suppliers.supplierID
JOIN Stores ON Products.storeID = Stores.storeID
GROUP BY ROLLUP (Store_Name, Supplier_Name, Product_Name, Year)
ORDER BY Year, Store_Name, Supplier_Name, Product_Name;


-- Query 8
SELECT 
    Product_Name,
    CASE 
        WHEN MONTH(Order_Date) BETWEEN 1 AND 6 THEN 'H1'
        ELSE 'H2'
    END AS Half,
    SUM(Sale) AS Total_Revenue,
    SUM(Quantity) AS Total_Quantity
FROM DW_Transactions
GROUP BY Product_Name, Half
ORDER BY Product_Name, Half;


-- Query 9
SELECT 
    Product_Name,
    DATE(Order_Date) AS Sale_Date,
    SUM(Sale) AS Daily_Sales,
    AVG(SUM(Sale)) OVER (PARTITION BY Product_Name) AS Avg_Daily_Sales,
    CASE 
        WHEN SUM(Sale) > 2 * AVG(SUM(Sale)) OVER (PARTITION BY Product_Name) THEN 'Outlier'
        ELSE 'Normal'
    END AS Status
FROM 
    DW_Transactions
GROUP BY 
    Product_Name, Sale_Date
ORDER BY 
    Product_Name, Sale_Date;


-- Query 10
CREATE OR REPLACE VIEW STORE_QUARTERLY_SALES AS
SELECT 
    Store_Name,
    YEAR(Order_Date) AS Year,
    QUARTER(Order_Date) AS Quarter,
    SUM(Sale) AS Total_Quarterly_Sales
FROM 
    DW_Transactions
GROUP BY 
    Store_Name, Year, Quarter
ORDER BY 
    Store_Name, Year, Quarter;


SELECT * 
FROM STORE_QUARTERLY_SALES
WHERE Year = 2019;


