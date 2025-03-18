-- Drop the database if it exists
DROP DATABASE IF EXISTS Metro_DW;

-- Create a new database
CREATE DATABASE Metro_DW;
USE Metro_DW;

-- Drop existing tables if they exist
DROP TABLE IF EXISTS DW_Transactions;
DROP TABLE IF EXISTS Transactions;
DROP TABLE IF EXISTS Products;
DROP TABLE IF EXISTS Customers;
DROP TABLE IF EXISTS Suppliers;
DROP TABLE IF EXISTS Stores;
DROP TABLE IF EXISTS Time;

-- Dimension Table: Suppliers
CREATE TABLE Suppliers (
    supplierID INT PRIMARY KEY,
    supplierName VARCHAR(255)
);

-- Dimension Table: Stores
CREATE TABLE Stores (
    storeID INT PRIMARY KEY,
    storeName VARCHAR(255)
);

-- Dimension Table: Products
CREATE TABLE Products (
    ProductID INT PRIMARY KEY,
    productName VARCHAR(255),
    productPrice FLOAT,
    supplierID INT,
    storeID INT,
    FOREIGN KEY (supplierID) REFERENCES Suppliers(supplierID) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (storeID) REFERENCES Stores(storeID) ON DELETE CASCADE ON UPDATE CASCADE
);

-- Dimension Table: Customers
CREATE TABLE Customers (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(255),
    gender VARCHAR(10)
);

-- Dimension Table: Time
CREATE TABLE Time (
    time_id INT PRIMARY KEY AUTO_INCREMENT,
    Order_Date DATETIME,
    Year INT,
    Month INT,
    Day INT,
    Weekday VARCHAR(10),
    Quarter VARCHAR(5)
);

-- Fact Table: Transactions
CREATE TABLE Transactions (
    Order_ID INT PRIMARY KEY,
    Order_Date DATETIME,
    ProductID INT,
    customer_id INT,
    Quantity_Ordered INT,
    TOTAL_SALE FLOAT,
    storeID INT,
    time_id INT,
    FOREIGN KEY (ProductID) REFERENCES Products(ProductID) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (customer_id) REFERENCES Customers(customer_id) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (storeID) REFERENCES Stores(storeID) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (time_id) REFERENCES Time(time_id) ON DELETE CASCADE ON UPDATE CASCADE
);

-- Enriched Fact Table: DW_Transactions (if required for some reporting)
CREATE TABLE DW_Transactions (
    Order_ID INT PRIMARY KEY,
    Order_Date DATETIME,
    Product_ID INT,
    Product_Name VARCHAR(255),
    Customer_ID INT,
    Customer_Name VARCHAR(255),
    Gender VARCHAR(10),
    Quantity INT,
    Product_Price FLOAT,
    Supplier_ID INT,
    Supplier_Name VARCHAR(255),
    Store_ID INT,
    Store_Name VARCHAR(255),
    Sale FLOAT,
    FOREIGN KEY (Product_ID) REFERENCES Products(ProductID) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (Customer_ID) REFERENCES Customers(customer_id) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (Store_ID) REFERENCES Stores(storeID) ON DELETE CASCADE ON UPDATE CASCADE
);


-- Check tables creation
SHOW TABLES;
