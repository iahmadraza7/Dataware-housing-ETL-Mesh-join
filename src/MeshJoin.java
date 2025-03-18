import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

public class MeshJoin {

    // Customer class
    static class Customer {
        int customerId;
        String customerName;
        String gender;

        public Customer(int customerId, String customerName, String gender) {
            this.customerId = customerId;
            this.customerName = customerName;
            this.gender = gender;
        }
    }

    // Product class
    static class Product {
        int productId;
        String productName;
        double productPrice;
        int supplierId;
        String supplierName;
        int storeId;
        String storeName;

        public Product(int productId, String productName, double productPrice, int supplierId, String supplierName, int storeId, String storeName) {
            this.productId = productId;
            this.productName = productName;
            this.productPrice = productPrice;
            this.supplierId = supplierId;
            this.supplierName = supplierName;
            this.storeId = storeId;
            this.storeName = storeName;
        }
    }

    // Transaction class
    static class Transaction {
        int orderId;
        LocalDateTime orderDate;
        int productId;
        int customerId;
        int quantity;
        int timeId;

        public Transaction(int orderId, LocalDateTime orderDate, int productId, int customerId, int quantity, int timeId) {
            this.orderId = orderId;
            this.orderDate = orderDate;
            this.productId = productId;
            this.customerId = customerId;
            this.quantity = quantity;
            this.timeId = timeId;
        }
    }

    // DiskBuffer class to load product partitions
    static class DiskBuffer {
        private List<Product> productPartitions;
        private int partitionSize;
        private int currentPartitionIndex;

        public DiskBuffer(List<Product> productList, int partitionSize) {
            this.partitionSize = partitionSize;
            this.productPartitions = productList;
            this.currentPartitionIndex = 0;
        }

        public List<Product> loadNextPartition() {
            int fromIndex = currentPartitionIndex * partitionSize;
            int toIndex = Math.min(fromIndex + partitionSize, productPartitions.size());

            if (fromIndex >= productPartitions.size()) {
                currentPartitionIndex = 0;
                fromIndex = 0;
                toIndex = Math.min(partitionSize, productPartitions.size());
            }

            List<Product> partition = productPartitions.subList(fromIndex, toIndex);
            currentPartitionIndex++;
            return partition;
        }
    }

    private static final String DB_URL = "jdbc:mysql://localhost:3306/metro_dw";
    private static final String DB_USER = "root";
    private static final String DB_PASSWORD = "raza";

    private static final int PARTITION_SIZE = 100;
    private static final int STREAM_WINDOW_SIZE = 500;

    public static void main(String[] args) {
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)) {
            System.out.println("Connected to the database.");
            performMeshJoin(conn);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void performMeshJoin(Connection conn) throws SQLException {
        System.out.println("Starting real-time MESHJOIN ETL process...");

        Queue<Transaction> streamBuffer = new LinkedBlockingQueue<>(STREAM_WINDOW_SIZE);
        Map<Integer, Customer> customerHashTable = new HashMap<>();
        DiskBuffer diskBuffer = loadDiskBuffer(conn);

        String transactionQuery = "SELECT * FROM Transactions ORDER BY Order_Date LIMIT ?";
        String insertQuery = "INSERT INTO DW_Transactions (Order_ID, Order_Date, Product_ID, Product_Name, Customer_ID, " +
                "Customer_Name, Gender, Quantity, Product_Price, Supplier_ID, Supplier_Name, Store_ID, Store_Name, Sale) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                "ON DUPLICATE KEY UPDATE Quantity = Quantity + VALUES(Quantity), Sale = Sale + VALUES(Sale)";

        try (PreparedStatement transactionStmt = conn.prepareStatement(transactionQuery);
             PreparedStatement insertStmt = conn.prepareStatement(insertQuery)) {

            loadCustomersIntoHashTable(conn, customerHashTable);

            while (true) {
                transactionStmt.setInt(1, STREAM_WINDOW_SIZE);
                ResultSet rs = transactionStmt.executeQuery();

                if (!rs.isBeforeFirst()) {
                    break;
                }

                while (rs.next()) {
                    Transaction transaction = new Transaction(
                            rs.getInt("Order_ID"),
                            rs.getTimestamp("Order_Date").toLocalDateTime(),
                            rs.getInt("ProductID"),
                            rs.getInt("customer_id"),
                            rs.getInt("Quantity_Ordered"),
                            rs.getInt("time_id")
                    );
                    streamBuffer.add(transaction);
                }

                List<Product> productPartition = diskBuffer.loadNextPartition();
                Map<Integer, Product> productHashTable = buildProductHashTable(productPartition);

                while (!streamBuffer.isEmpty()) {
                    Transaction transaction = streamBuffer.poll();
                    if (transaction == null) continue;

                    Customer customer = customerHashTable.get(transaction.customerId);
                    Product product = productHashTable.get(transaction.productId);

                    if (customer == null || product == null) {
                        System.err.println("Missing metadata for Transaction ID: " + transaction.orderId);
                        continue;
                    }

                    insertStmt.setInt(1, transaction.orderId);
                    insertStmt.setTimestamp(2, Timestamp.valueOf(transaction.orderDate));
                    insertStmt.setInt(3, transaction.productId);
                    insertStmt.setString(4, product.productName);
                    insertStmt.setInt(5, transaction.customerId);
                    insertStmt.setString(6, customer.customerName);
                    insertStmt.setString(7, customer.gender);
                    insertStmt.setInt(8, transaction.quantity);
                    insertStmt.setDouble(9, product.productPrice);
                    insertStmt.setInt(10, product.supplierId);
                    insertStmt.setString(11, product.supplierName);
                    insertStmt.setInt(12, product.storeId);
                    insertStmt.setString(13, product.storeName);
                    insertStmt.setDouble(14, transaction.quantity * product.productPrice);
                    insertStmt.addBatch();
                }
                insertStmt.executeBatch();
                System.out.println("Processed a batch of transactions with the current partition.");
            }

            System.out.println("Real-time MESHJOIN ETL process completed successfully.");
        }
    }

    private static DiskBuffer loadDiskBuffer(Connection conn) throws SQLException {
        List<Product> productList = new ArrayList<>();
        String productQuery = "SELECT p.ProductID, p.productName, p.productPrice, p.supplierID, s.supplierName, " +
                "p.storeID, st.storeName " +
                "FROM Products p " +
                "JOIN Suppliers s ON p.supplierID = s.supplierID " +
                "JOIN Stores st ON p.storeID = st.storeID";

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(productQuery)) {
            while (rs.next()) {
                productList.add(new Product(
                        rs.getInt("ProductID"),
                        rs.getString("productName"),
                        rs.getDouble("productPrice"),
                        rs.getInt("supplierID"),
                        rs.getString("supplierName"),
                        rs.getInt("storeID"),
                        rs.getString("storeName")
                ));
            }
        }

        return new DiskBuffer(productList, PARTITION_SIZE);
    }

    private static void loadCustomersIntoHashTable(Connection conn, Map<Integer, Customer> customerHashTable) throws SQLException {
        String customerQuery = "SELECT * FROM Customers";

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(customerQuery)) {
            while (rs.next()) {
                Customer customer = new Customer(
                        rs.getInt("customer_id"),
                        rs.getString("customer_name"),
                        rs.getString("gender")
                );
                customerHashTable.put(customer.customerId, customer);
            }
        }
    }

    private static Map<Integer, Product> buildProductHashTable(List<Product> productPartition) {
        Map<Integer, Product> productHashTable = new HashMap<>();
        for (Product product : productPartition) {
            productHashTable.put(product.productId, product);
        }
        return productHashTable;
    }
}
