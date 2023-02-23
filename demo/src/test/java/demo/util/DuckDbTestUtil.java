package demo.util;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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
        return "jdbc:duckdb:src/test/java/demo/data/duckdb/tmp.db";
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
    public void closeConnection(Connection con) throws SQLException {
        if (con != null) {
            con.close();
        }
    }

    @Override
    public void createSchema(Connection con, String schema) throws SQLException {
        Statement stmt = con.createStatement();
        try {
            dropSchema(con, schema);
            stmt.executeUpdate("CREATE SCHEMA " + schema);
        } finally {
            stmt.close();
        }
    }

    @Override
    public void dropSchema(Connection con, String schema) throws SQLException {
        Statement stmt = con.createStatement();
        try {
            stmt.executeUpdate("DROP SCHEMA " + schema);
        } finally {
            stmt.close();
        }
    }

    @Override
    public void createTable(Connection con, String table, String columns) throws SQLException {
        Statement stmt = con.createStatement();
        try {
            // dropTable(con, table);
            stmt.execute("CREATE TABLE " + table + " (" + columns + ")");

        } finally {
            stmt.close();
        }
    }

    @Override
    public void dropTable(Connection con, String table) throws SQLException {
        Statement stmt = con.createStatement();
        try {
            stmt.executeUpdate("DROP TABLE " + table);
        } finally {
            stmt.close();
        }
    }

    @Override
    public void createView(Connection con, String view, String query) throws SQLException {
        Statement stmt = con.createStatement();
        try {
            dropView(con, view);
            stmt.executeUpdate("CREATE VIEW " + view + " AS " + query);
        } finally {
            stmt.close();
        }
    }

    @Override
    public void dropView(Connection con, String view) throws SQLException {
        Statement stmt = con.createStatement();
        try {
            stmt.executeUpdate("DROP VIEW " + view);
        } finally {
            stmt.close();
        }
    }

    @Override
    public void assertNumberOfRows(Connection con, String tableName, int expectedRows, String message)
            throws SQLException {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = con.prepareStatement("select count(*) from " + tableName + " as t");
            rs = ps.executeQuery();
            rs.next();
            assertEquals(message, expectedRows, rs.getInt(1));
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (ps != null) {
                ps.close();
            }
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
            stmt.executeUpdate("CREATE FUNCTION " + name + "(" + arguments + ") AS " + query);
        } finally {
            stmt.close();
        }
    }

    @Override
    public void dropFunction(Connection con, String function, String arguments) throws SQLException {
        Statement stmt = con.createStatement();
        try {
            stmt.executeUpdate("DROP FUNCTION " + function);
        } finally {
            stmt.close();
        }
    }

    @Override
    public void createObject(Connection con, String objectType, String objectName, String columnsAndOtherStuff)
            throws SQLException {
        Statement stmt = con.createStatement();
        try {
            dropObject(con, objectType, objectName);
            stmt.executeUpdate("CREATE " + objectType + " " + objectName + " " + columnsAndOtherStuff);
        } finally {
            stmt.close();
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
            stmt.close();
        }
    }

}
