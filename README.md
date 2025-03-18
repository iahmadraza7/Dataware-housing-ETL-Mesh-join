# Dataware-housing-ETL-Mesh-join
Building and Analyzing a Near-Real-Time Data Warehouse Prototype for METRO Shopping Store in Pakistan

National University of Computer
and Emerging Sciences
DW & BI
o ProductID: INT (Primary Key)
o productName: VARCHAR(255)
o productPrice: FLOAT
o supplierID: INT (Foreign Key referencing Suppliers)
o storeID: INT (Foreign Key referencing Stores)
4. Customers
o customer_id: INT (Primary Key)
o customer_name: VARCHAR(255)
o gender: VARCHAR(10)
5. Time
o time_id: INT (Primary Key, Auto Increment)
o Order_Date: DATETIME
o Year: INT
o Month: INT
o Day: INT
o Weekday: VARCHAR(10)
o Quarter: VARCHAR(5)
National University of Computer
and Emerging Sciences
DW & BI
Fact Table
DW_Transactions
• Order_ID: INT (Primary Key)
• Order_Date: DATETIME
• Product_ID: INT (Foreign Key referencing Products)
• Product_Name: VARCHAR(255)
• Customer_ID: INT (Foreign Key referencing Customers)
• Customer_Name: VARCHAR(255)
• Gender: VARCHAR(10)
• Quantity: INT
• Product_Price: FLOAT
• Supplier_ID: INT (Foreign Key referencing Suppliers)
• Supplier_Name: VARCHAR(255)
• Store_ID: INT (Foreign Key referencing Stores)
• Store_Name: VARCHAR(255)
• Sale: FLOAT
Data Insertion and ETL Process
The ETL process is divided into the following steps:
1. Extract: Data is extracted from CSV files (products, customers,
transactions) by python code and manually import.
2. Transform: Transformation includes enriching the data by creating
relationships between products, stores, and suppliers.
3. Load: Data is loaded into the appropriate tables in the Data Warehouse
(DW)
MESHJOIN Algorithm
The MESHJOIN algorithm is implemented in Java to process transactional data in
real time. It combines a sliding window of transactions with disk-based partitions
of product metadata to enrich transactions.
National University of Computer
and Emerging Sciences
DW & BI
Steps in MESHJOIN:
1. Load Customers into Hash Table:
o Read the Customers table into an in-memory hash table for quick
lookup.
2. Partition Products:
o Divide the Products table into manageable partitions to fit in
memory.
3. Stream Transactions:
o Load a sliding window of transactions from the Transactions table.
4. Enrich Transactions:
o For each transaction, lookup metadata from the hash tables for
Customers and the current product partition.
5. Insert into DW_Transactions:
o Store the enriched transactions in the DW_Transactions fact table.
6. Repeat:
o Continue processing transactions by cycling through product
partitions.
How MESHJOIN Works with Real-Time Data:
1. Stream Data Ingestion:
o The transaction data is continuously fed into the system, processed in
chunks, and stored in a queue (streamBuffer).
o The incoming data is then enriched using Product and Customer
National University of Computer
and Emerging Sciences
DW & BI
data stored in hash tables.
2. Disk Buffer:
o The DiskBuffer class is used to load partitions of product data in
memory. Each product partition is processed separately, allowing the
algorithm to efficiently join incoming transactions with the
corresponding product data without overloading the memory.
o The partition size is controlled, ensuring that the system can handle
large datasets without running out of memory.
3. Joining Process:
o Once a partition of products is loaded into memory, it is joined with
the streamed transaction data (i.e., the incoming transaction). This
is done in real-time, and once the join is complete, the results are
inserted into the DW_Transactions table.
o Each transaction is enriched with product name, price, supplier,
customer name, and other relevant details. This ensures that the
transaction data is ready for analysis.
4. Batch Insert:
o After enriching the transaction data, a batch insert operation is
performed into the DW_Transactions table. The DW_Transactions
table holds all the enriched transactional data, which is the final
target for reporting and analysis.
o The ON DUPLICATE KEY UPDATE part of the INSERT query
ensures that if there are multiple entries for the same order, the
quantities and sales are aggregated.
5. Memory Management:
o Each chunk of customer data in the hash table, once processed, is
removed from memory, ensuring that only the relevant data is held in
memory at any given time. This approach minimizes memory usage
and optimizes the processing speed for large datasets.
Shortcomings of MESHJOIN
1. Memory Limitation:
o MESHJOIN depends on hash tables for in-memory lookup. Large
datasets may exceed available memory, leading to slower
performance or failures.
2. Data Latency:
o Although optimized for real-time processing, the algorithm
introduces latency due to partition loading and transaction
enrichment.
3. Static Partitioning:
o The algorithm assumes fixed-size partitions, which may not align
National University of Computer
and Emerging Sciences
DW & BI
with the actual distribution of transaction and metadata sizes.
What Did I Learn from the Project?
1. Data Warehousing Concepts:
o Learned how to design and implement a star schema for a real-world
data warehouse.
2. MESHJOIN Algorithm:
o Gained a deep understanding of the MESHJOIN algorithm for real-
time ETL processing.
3. Java and SQL Integration:
o Practiced integrating Java code with a MySQL database using JDBC,
handling connections, and executing queries programmatically.
4. Error Debugging:
o Developed skills in debugging SQL and Java code errors and
ensuring seamless interaction between the two.
5. Business Intelligence:
o Learned how to write OLAP queries for advanced analytics,
providing actionable business insights.
Project Workflow
1. Setting Up the Environment
• Install MySQL Workbench and Java Development Kit (JDK).
• Set up IntelliJ IDEA as the Java IDE.
2. Database Creation
• Use the Create-DW.sql script to create the database schema in MySQL
Workbench.
3. Data Loading
• Load cleaned datasets (customers_data.csv, products_cleaned.csv,
transactions_cleaned.csv) into their respective tables.
4. Running MESHJOIN
• Compile and run the MeshJoin.java file in IntelliJ IDEA to process
transactions and populate the DW_Transactions fact table.
5. OLAP Queries
• Execute the OLAP queries from queries.sql in MySQL Workbench to
analyze the data warehouse.
