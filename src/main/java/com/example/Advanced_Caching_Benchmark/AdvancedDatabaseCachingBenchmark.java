package com.example.Advanced_Caching_Benchmark;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

// Add logger
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hello world!
 *
 */
public class AdvancedDatabaseCachingBenchmark {
    private static final Logger logger = LoggerFactory.getLogger(AdvancedDatabaseCachingBenchmark.class);

    private static final String DB_URL = "jdbc:mysql://localhost:3306/testdb";
    private static final String DB_USER = "root";
    private static final String DB_PASSWORD = "WJ28@krhps";
    private static final int NUM_ELEMENTS = 1000;
    private static final int L1_CACHE_SIZE = 1000;
    private static final int L2_CACHE_SIZE = 1000;
    private static final int L2_CACHE_DURATION_MINUTES = 10;

    private static HikariDataSource dataSource; // Use HikariCP DataSource
    private static Map<Integer, String> l1Cache;
    private static Cache<Integer, String> l2Cache;

    // Atomic variables for tracking times
    private static AtomicLong dbInsertTime = new AtomicLong(0);
    private static AtomicLong dbRetrieveTime = new AtomicLong(0);
    private static AtomicLong l1CacheInsertTime = new AtomicLong(0);
    private static AtomicLong l1CacheRetrieveTime = new AtomicLong(0);
    private static AtomicLong l2CacheInsertTime = new AtomicLong(0);
    private static AtomicLong l2CacheRetrieveTime = new AtomicLong(0);
    private static AtomicLong multilevelCacheRetrieveTime = new AtomicLong(0);

    public static void main(String[] args) {
        try {
            setupDatabase();
            setupCaches();

            // Use ExecutorService for multithreading
            ExecutorService executor = Executors.newFixedThreadPool(10); // Adjust pool size as needed

            // Measure database insert time with multithreading
            long startInsertTime = System.nanoTime();
            for (int i = 0; i < NUM_ELEMENTS; i++) {
                final int id = i;
                executor.submit(() -> {
                    try {
                        insertIntoDatabase(id, "Value" + id);
                    } catch (SQLException e) {
                        logger.error("Error inserting into database for ID: " + id, e);
                    }
                });
            }
            executor.shutdown();
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            long endInsertTime = System.nanoTime();
            dbInsertTime.set(endInsertTime - startInsertTime);

            // Measure database retrieve time with multithreading
            executor = Executors.newFixedThreadPool(10); // Create a new executor for retrieval
            long startRetrieveTime = System.nanoTime();
            for (int i = 0; i < NUM_ELEMENTS; i++) {
                final int id = i;
                executor.submit(() -> {
                    try {
                        retrieveFromDatabase(id);
                    } catch (SQLException e) {
                        logger.error("Error retrieving from database for ID: " + id, e);
                    }
                });
            }
            executor.shutdown();
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            long endRetrieveTime = System.nanoTime();
            dbRetrieveTime.set(endRetrieveTime - startRetrieveTime);

            l1CacheInsertTime.set(benchMarkL1CacheInsert());
            l1CacheRetrieveTime.set(benchMarkL1CacheRetrieve());
            l2CacheInsertTime.set(benchMarkL2CacheInsert());
            l2CacheRetrieveTime.set(benchMarkL2CacheRetrieve());
            multilevelCacheRetrieveTime.set(benchmarkMultilevelCacheRetrieve());

            // Print database size
            long dbSize = getDatabaseSize();
            logger.info("Database Size: " + dbSize + " rows");

            printResults();

            // Example usage of checkIdExists method
            int idToCheck = 500; // Change this to the ID you want to check
            boolean exists = checkIdExists(idToCheck);
            logger.info("ID " + idToCheck + " exists: " + exists);
        } catch (Exception e) {
            logger.error("An error occurred", e);
        } finally {
            if (dataSource != null) {
                dataSource.close(); // Close the data source when done
            }
        }
    }

    private static void setupDatabase() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(DB_URL);
        config.setUsername(DB_USER);
        config.setPassword(DB_PASSWORD);
        config.setMaximumPoolSize(10); // Adjust the pool size as needed

        dataSource = new HikariDataSource(config); // Initialize HikariCP DataSource
        try (Connection connection = dataSource.getConnection(); // Get connection from pool
             Statement statement = connection.createStatement()) {
            statement.executeUpdate("CREATE TABLE IF NOT EXISTS test_table (id INT PRIMARY KEY, value VARCHAR(255))");
        } catch (SQLException e) {
            logger.error("Error setting up database", e);
        }
    }

    private static void setupCaches() {
        l1Cache = new LinkedHashMap<Integer, String>(L1_CACHE_SIZE, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Integer, String> eldest) {
                return size() > L1_CACHE_SIZE;
            }
        };
        l2Cache = CacheBuilder.newBuilder()
                .maximumSize(L2_CACHE_SIZE)
                .expireAfterAccess(L2_CACHE_DURATION_MINUTES, TimeUnit.MINUTES)
                .build();
    }

    private static void insertIntoDatabase(int id, String value) throws SQLException {
        String sql = "INSERT INTO test_table (id, value) VALUES (?, ?)";
        try (Connection connection = dataSource.getConnection(); // Get connection from pool
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, id);
            statement.setString(2, value);
            statement.executeUpdate();
        }
    }

    private static void retrieveFromDatabase(int id) throws SQLException {
        String sql = "SELECT * FROM test_table WHERE id = ?";
        try (Connection connection = dataSource.getConnection(); // Get connection from pool
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, id);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    resultSet.getString("value");
                }
            }
        }
    }

    private static long benchMarkL1CacheInsert() {
        long startTime = System.nanoTime();
        for (int i = 0; i < NUM_ELEMENTS; i++) {
            l1Cache.put(i, "Value" + i);
        }
        long endTime = System.nanoTime();
        return endTime - startTime;
    }

    private static long benchMarkL1CacheRetrieve() {
        long startTime = System.nanoTime();
        for (int i = 0; i < NUM_ELEMENTS; i++) {
            l1Cache.get(i);
        }
        long endTime = System.nanoTime();
        return endTime - startTime;
    }

    private static long benchMarkL2CacheInsert() {
        long startTime = System.nanoTime();
        for (int i = 0; i < NUM_ELEMENTS; i++) {
            l2Cache.put(i, "Value" + i);
        }
        long endTime = System.nanoTime();
        return endTime - startTime;
    }

    private static long benchMarkL2CacheRetrieve() {
        long startTime = System.nanoTime();
        for (int i = 0; i < NUM_ELEMENTS; i++) {
            l2Cache.getIfPresent(i);
        }
        long endTime = System.nanoTime();
        return endTime - startTime;
    }

    private static long benchmarkMultilevelCacheRetrieve() throws SQLException {
        long startTime = System.nanoTime();
        for (int i = 0; i < NUM_ELEMENTS; i++) {
            String value = l1Cache.get(i);
            if (value == null) {
                value = l2Cache.getIfPresent(i);
                if (value == null) {
                    // Retrieve from the database
                    retrieveFromDatabase(i);
                } else {
                    // Store in L1 cache for faster access next time
                    l1Cache.put(i, value);
                }
            }
        }
        long endTime = System.nanoTime();
        return endTime - startTime;
    }

    private static long getDatabaseSize() throws SQLException {
        String sql = "SELECT COUNT(*) FROM test_table";
        try (Connection connection = dataSource.getConnection(); // Get connection from pool
             PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet resultSet = statement.executeQuery()) {
            if (resultSet.next()) {
                return resultSet.getLong(1);
            }
        }
        return 0;
    }

    private static void printResults() {
        logger.info("Database Insert Time: " + dbInsertTime.get() + " ns");
        logger.info("Database Retrieve Time: " + dbRetrieveTime.get() + " ns");
        logger.info("L1 Cache Insert Time: " + l1CacheInsertTime.get() + " ns");
        logger.info("L1 Cache Retrieve Time: " + l1CacheRetrieveTime.get() + " ns");
        logger.info("L2 Cache Insert Time: " + l2CacheInsertTime.get() + " ns");
        logger.info("L2 Cache Retrieve Time: " + l2CacheRetrieveTime.get() + " ns");
        logger.info("Multilevel Cache Retrieve Time: " + multilevelCacheRetrieveTime.get() + " ns");
    }

    // Method to check if an ID exists in the database
    private static boolean checkIdExists(int id) {
        String sql = "SELECT COUNT(*) FROM test_table WHERE id = ?";
        try (Connection connection = dataSource.getConnection(); // Get connection from pool
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, id);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    return resultSet.getInt(1) > 0;
                }
            }
        } catch (SQLException e) {
            logger.error("Error checking ID existence", e);
        }
        return false;
    }
}
