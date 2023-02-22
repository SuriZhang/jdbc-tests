package demo.util;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public final class DuckDbTestUtil implements TestUtil {

    @Override
    public String getDatabase() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int getPort() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getServer() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getURL() {
        return "jdbc:duckdb:/home/anxing/Documents/jdbc-tests/demo/src/test/java/demo/data/duckdb/tmp.db";
    }

    @Override
    public Connection openReplicationConnection() throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void initDriver() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'initDriver'");
    }

    @Override
    public Connection openPriviligedConnection() throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'openPriviligedConnection'");
    }

    @Override
    public Connection openConnection() throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'openConnection'");
    }

    @Override
    public Connection openConnection(Properties properties) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'openConnection'");
    }

    @Override
    public void closeConnection(Connection con) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'closeConnection'");
    }

    @Override
    public void createSchema(Connection con, String schema) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'createSchema'");
    }

    @Override
    public void dropSchema(Connection con, String schema) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'dropSchema'");
    }

    @Override
    public void createTable(Connection con, String table, String columns) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'createTable'");
    }

    @Override
    public void dropTable(Connection con, String table) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'dropTable'");
    }

    @Override
    public void createView(Connection con, String view, String query) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'createView'");
    }

    @Override
    public void dropView(Connection con, String view) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'dropView'");
    }

    @Override
    public void assertNumberOfRows(Connection con, String tableName, int expectedRows, String message)
            throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'assertNumberOfRows'");
    }

    @Override
    public Connection openReadOnlyConnection() throws Exception {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'openReadOnlyConnection'");
    }

    @Override
    public void createFunction(Connection con, String name, String arguments, String query) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'createFunction'");
    }

    @Override
    public void dropFunction(Connection con, String function, String arguments) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'dropFunction'");
    }

    @Override
    public void createObject(Connection con, String objectType, String objectName, String columnsAndOtherStuff)
            throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'createObject'");
    }

    @Override
    public void dropObject(Connection con, String objectType, String objectName) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'dropObject'");
    }
    
}
