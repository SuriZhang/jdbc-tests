package demo.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import javax.annotation.Nullable;

public interface TestUtil {

    String getURL();

    String getServer();

    int getPort();

    String getDatabase();

    Connection openPriviligedConnection() throws SQLException;

    Connection openReplicationConnection(@Nullable Connection con) throws Exception;

    Connection openReadOnlyConnection(@Nullable String option) throws Exception;

    Connection openConnection() throws SQLException;

    Connection openConnection(Properties properties) throws SQLException;

    void closeConnection(@Nullable Connection con) throws SQLException;

    void createSchema(Connection con, String schema) throws SQLException;

    void dropSchema(Connection con, String schema) throws SQLException;

    void createTable(Connection con, String table, String columns) throws SQLException;

    void dropTable(Connection con, String table) throws SQLException;

    void createView(Connection con, String view, String query) throws SQLException;

    void dropView(Connection con, String view) throws SQLException;

    void createFunction(Connection con, String name, String arguments, String query) throws SQLException;

    void dropFunction(Connection con, String function, String arguments) throws SQLException;

    void createObject(Connection con, String objectType, String objectName, String columnsAndOtherStuff)
            throws SQLException;

    void dropObject(Connection con, String objectType, String objectName) throws SQLException;

    void assertNumberOfRows(Connection con, String tableName, int expectedRows, String message)
            throws SQLException;

    public static File getFile(String name) {
        if (name == null) {
            throw new IllegalArgumentException("null file name is not expected");
        }
        if (name.startsWith("/")) {
            return new File(name);
        }
        return new File(System.getProperty(name + ".relative.path", "src/"), name);
    }

    public static Properties loadPropertyFiles(String... names) {
        Properties p = new Properties();
        for (String name : names) {
            for (int i = 0; i < 2; i++) {
                // load x.properties, then x.local.properties
                if (i == 1 && name.endsWith(".properties") && !name.endsWith(".local.properties")) {
                    name = name.replaceAll("\\.properties$", ".local.properties");
                }
                File f = getFile(name);
                if (!f.exists()) {
                    System.out.println("Configuration file " + f.getAbsolutePath()
                            + " does not exist. Consider adding it to specify test db host and login");
                    continue;
                }
                try {
                    p.load(new FileInputStream(f));
                } catch (IOException ex) {
                    // ignore
                }
            }
        }
        return p;
    }

}
