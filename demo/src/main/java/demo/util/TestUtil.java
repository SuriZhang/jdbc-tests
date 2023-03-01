package demo.util;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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

    public static void closeConnection(Connection con) throws SQLException {
        if (con != null) {
            con.close();
        }
    }

    public static File getFile(String name) {
        if (name == null) {
            throw new IllegalArgumentException("null file name is not expected");
        }
        if (name.startsWith("/")) {
            return new File(name);
        }
        return new File(System.getProperty(name + ".relative.path", "src/"), name);
    }

    public static Properties loadPropertyFiles(String name) {
        Properties p = new Properties();
        if (!name.endsWith(".properties")) {
            System.out.println("Configuration file must end with .properties but found: " +  name);
        }
        File f = getFile(name);
        if (!f.exists()) {
            System.out.println("Configuration file " + f.getAbsolutePath()
                    + " does not exist. Consider adding it to specify test db host and login");
        }
        try {
            p.load(new FileInputStream(f));
        } catch (IOException ex) {
            // ignore
        }

        return p;
    }

    /**
     * Close a Statement and ignore any errors during closing.
     */
    public static void closeQuietly(@Nullable Statement stmt) {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException ignore) {
                ignore.getMessage();
            }
        }
    }

    /**
     * Close a ResultSet and ignore any errors during closing.
     */
    public static void closeQuietly(@Nullable ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException ignore) {
            }
        }
    }

    /**
     * Close a Connection and ignore any errors during closing.
     */
    public static void closeQuietly(@Nullable Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException ignore) {
            }
        }
    }

    /**
     * Close a resource and ignore any errors during closing.
     */
    public static void closeQuietly(@Nullable Closeable resource) {
        if (resource != null) {
            try {
                resource.close();
            } catch (Exception ignore) {
            }
        }
    }

    /**
     * Percent-encode all occurrence of the the percent sign (%) in the given
     * string.
     * 
     * @param strToEncode
     *                    the string to encode
     * @return the encoded string
     */
    public static String encodePercent(String strToEncode) {
        return strToEncode.replaceAll("%", "%25");
    }

}
