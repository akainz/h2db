package org.example;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Random;

public class H2DatabaseExample {
    // JDBC URL, username, and password of the database
    private static final String JDBC_URL = "jdbc:h2:./db/test";
    private static final String USERNAME = "sa";
    private static final String PASSWORD = "";

    public static void main(String[] args) throws ClassNotFoundException {
        try {
            if(new File("db/test.mv.db").exists()){
                new File("db/test.mv.db").delete();
            }
            var clazz=Class.forName("org.h2.Driver");
            // Connect to the database
            Connection connection = DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD);
            // Create a table
            createTable(connection);

            // Insert 10 rows into the table
            insertRows(connection);

            // Update the rows
            updateRows(connection);

            // Close the connection
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void createTable(Connection connection) throws SQLException {

        String createTableSQL = "CREATE TABLE IF NOT EXISTS sample_table (id INT PRIMARY KEY, " +
                "name VARCHAR(1048576), " +
                "name2 VARCHAR(1048576)," +
                "name3 VARCHAR(1048576)," +
                "name4 VARCHAR(1048576)," +
                "name5 VARCHAR(1048576), " +
                "name6 VARCHAR(1048576), " +
                "name7 VARCHAR(1048576), " +
                "name8 VARCHAR(1048576), " +
                "name9 VARCHAR(1048576), " +
                "name10 VARCHAR(1048576), " +
                "COUNT INT DEFAULT 0)";
        try (PreparedStatement preparedStatement = connection.prepareStatement(createTableSQL)) {
            preparedStatement.executeUpdate();
            System.out.println("Table created successfully."+ size());
        }
    }

    private static void insertRows(Connection connection) throws SQLException {
        String insertSQL = "INSERT INTO sample_table (id, name, name2, name3, name4, name5, name6, name7, name8, name9, name10) VALUES (?, ?,?,?,?,?,?,?,?,?,?)";
        try (PreparedStatement preparedStatement = connection.prepareStatement(insertSQL)) {
            int maxCount = 100000;
            for (int i = 1; i <= maxCount; i++) {
                preparedStatement.setInt(1, i);
                for (int j = 2; j <= 11; j++) {
                    preparedStatement.setString(j, randomString(1024));
                }

                preparedStatement.executeUpdate();

            }
            int tableSize = (int) getTableSize(connection, "sample_table");
            System.out.println("Rows inserted successfully. File "+ size() +" actual data " + FileUtils.byteCountToDisplaySize(tableSize));
        }
    }
    private static long getTableSize(Connection connection, String tableName) throws SQLException {
        var rs= connection.createStatement().executeQuery("CALL DISK_SPACE_USED('sample_table')");
        rs.next();
        return rs.getLong(1);
    }
    private static String randomString(int targetStringLength){
        int leftLimit = 97; // letter 'a'
        int rightLimit = 122; // letter 'z'
        Random random = new Random();
        StringBuilder buffer = new StringBuilder(targetStringLength);
        for (int i = 0; i < targetStringLength; i++) {
            int randomLimitedInt = leftLimit + (int)
                    (random.nextFloat() * (rightLimit - leftLimit + 1));
            buffer.append((char) randomLimitedInt);
        }
        return buffer.toString();
    }

    private static String size() {
        return FileUtils.byteCountToDisplaySize(new File("db/test.mv.db").length());
    }

    private static void updateRows(Connection connection) throws SQLException {
        String updateSQL = "UPDATE sample_table SET COUNT = 0";
       connection.createStatement().execute(updateSQL);
        System.out.println("Rows updated successfully. "+ size());

    }
}
