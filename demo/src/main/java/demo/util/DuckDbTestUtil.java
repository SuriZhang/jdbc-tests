package demo.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import javax.annotation.Nullable;

import org.duckdb.DuckDBConnection;

public final class DuckDbTestUtil implements TestUtil {

    @Override
    public String getDatabase() {
        return null;
    }

    @Override
    public int getPort() {
        return -1;
    }

    @Override
    public String getServer() {
        return null;
    }

    @Override
    public String getURL() {
        // store duckdb data in tmp.db file otherwise it will be stored in memory
        // using relative path without leading slash here
        return "jdbc:log4jdbc:duckdb:src/test/java/demo/data/duckdb/tmp.db";
    }

    @Override
    public Connection openReplicationConnection(@Nullable Connection con) throws Exception {
        // See https://duckdb.org/docs/api/java#startup--shutdown
        if (con != null) {
            // read-write replication
            return ((DuckDBConnection) con).duplicate();
        }
        // read-only replication
        return openConnection();
    }

    @Override
    public Connection openPriviligedConnection() throws SQLException {
        throw new UnsupportedOperationException("Unimplemented method 'openPriviligedConnection'");
    }

    @Override
    public Connection openConnection() throws SQLException {
        return DriverManager.getConnection(getURL());
    }

    @Override
    public Connection openConnection(Properties properties) throws SQLException {
        return DriverManager.getConnection(getURL(), properties);
    }

    @Override
    public void createSchema(Connection con, String schema) throws SQLException {
        Statement stmt = con.createStatement();
        try {
            dropSchema(con, schema);
            String sql = "CREATE SCHEMA " + schema;
            stmt.executeUpdate(sql);
        } finally {
            TestUtil.closeQuietly(stmt);
        }
    }

    @Override
    public void dropSchema(Connection con, String schema) throws SQLException {
        Statement stmt = con.createStatement();
        try {
            String sql = "DROP SCHEMA IF EXISTS " + schema + " CASCADE";
            stmt.executeUpdate(sql);
        } finally {
            TestUtil.closeQuietly(stmt);
        }
    }

    @Override
    public void createTable(Connection con, String table, String columns) throws SQLException {
        Statement stmt = con.createStatement();
        try {
            dropTable(con, table);
            String sql = "CREATE TABLE " + table + " (" + columns + ")";
            stmt.executeUpdate(sql);
        } finally {
            TestUtil.closeQuietly(stmt);
        }
    }

    @Override
    public void dropTable(Connection con, String table) throws SQLException {
        Statement stmt = con.createStatement();
        try {
            String sql = "DROP TABLE IF EXISTS " + table;
            stmt.executeUpdate(sql);
        } finally {
            TestUtil.closeQuietly(stmt);
        }
    }

    @Override
    public void createView(Connection con, String view, String query) throws SQLException {
        Statement stmt = con.createStatement();
        try {
            dropView(con, view);
            String sql = "CREATE VIEW " + view + " AS " + query;
            stmt.executeUpdate(sql);
        } finally {
            TestUtil.closeQuietly(stmt);
        }
    }

    @Override
    public void dropView(Connection con, String view) throws SQLException {
        Statement stmt = con.createStatement();
        try {
            String sql = "DROP VIEW IF EXISTS " + view;
            stmt.executeUpdate(sql);
        } finally {
            TestUtil.closeQuietly(stmt);
        }
    }

    @Override
    public Connection openReadOnlyConnection(String option) throws Exception {
        Properties ro_prop = new Properties();
        ro_prop.setProperty("duckdb.read_only", "true");
        return DriverManager.getConnection(getURL(), ro_prop);
    }

    @Override
    public void createFunction(Connection con, String name, String arguments, String query) throws SQLException {
        Statement stmt = con.createStatement();
        try {
            String sql = "CREATE FUNCTION " + name + "(" + arguments + ") AS " + query;
            stmt.executeUpdate(sql);
        } finally {
            TestUtil.closeQuietly(stmt);
        }
    }

    @Override
    public void dropFunction(Connection con, String function, String arguments) throws SQLException {
        Statement stmt = con.createStatement();
        try {
            String sql = "DROP FUNCTION IF EXISTS " + function;
            stmt.executeUpdate(sql);
        } finally {
            TestUtil.closeQuietly(stmt);
        }
    }

    @Override
    public void createObject(Connection con, String objectType, String objectName, String columnsAndOtherStuff)
            throws SQLException {
        Statement stmt = con.createStatement();
        try {
            dropObject(con, objectType, objectName);
            String sql = "CREATE " + objectType + " " + objectName + " " + columnsAndOtherStuff;
            stmt.executeUpdate(sql);
        } finally {
            TestUtil.closeQuietly(stmt);
        }
    }

    @Override
    public void dropObject(Connection con, String objectType, String objectName) throws SQLException {
        Statement stmt = con.createStatement();
        try {
            if (con.getAutoCommit()) {
                // Not in a transaction so ignore error for missing object
                stmt.executeUpdate("DROP " + objectType + " IF EXISTS " + objectName + " CASCADE");
            } else {
                // In a transaction so do not ignore errors for missing object
                stmt.executeUpdate("DROP " + objectType + " " + objectName + " CASCADE");
            }
        } finally {
            TestUtil.closeQuietly(stmt);
        }
    }

    @Override
    public void closeConnection(Connection con) throws SQLException {
        ((DuckDBConnection) con).close();
    }

}
