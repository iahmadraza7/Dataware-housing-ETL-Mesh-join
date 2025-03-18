Setup and Run Instructions

 Step 1: Install Requirements
 Install: IntelliJ IDEA, MySQL Server, OpenJDK 11+, MySQL JDBC Driver.
 Ensure MySQL server is running.

 Step 2: Set Up Database
 Open MySQL Workbench.
 Run the `CreateDW.sql` file to create the Metro_DW schema and tables.

 Step 3: Import Data
 Import CSV files into MySQL:
   `customers_data.csv` → Customers
   `products_cleaned.csv` → Products
   `transactions_cleaned.csv` → Transactions

 Step 4: Test Database Connection
 Modify connection details in TestConnection.java (URL, username, password).
 Run the file to verify the connection.

 Step 5: Run MESHJOIN ETL
 Open MeshJoin.java and run.
 It will process realtime data and populate DW_Transactions.

 Step 6: Execute Queries
 Run OLAP queries in queries.sql to analyze the data.



 Notes
 Ensure required software is installed.
 Follow the setup steps carefully.
 Run TestConnection.java to check connectivity, then run MeshJoin.java for ETL processing.



 Troubleshooting
 If errors occur, check console logs, verify table names, and reimport data if needed.
