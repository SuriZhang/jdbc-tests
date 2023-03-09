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

public abstract class TestUtil {
    public abstract String getDatabase();

    public abstract Connection openPriviligedConnection() throws SQLException;

    public abstract Connection openReplicationConnection(@Nullable Connection con) throws Exception;

    public abstract Connection openReadOnlyConnection(@Nullable String option) throws Exception;

    public abstract Connection openConnection() throws SQLException;

    public abstract Connection openConnection(Properties properties) throws SQLException;

    public abstract void createSchema(Connection con, String schema) throws SQLException;

    public abstract void dropSchema(Connection con, String schema) throws SQLException;

    public abstract void createTable(Connection con, String table, String columns) throws SQLException;

    public abstract void dropTable(Connection con, String table) throws SQLException;

    public abstract void createView(Connection con, String view, String query) throws SQLException;

    public abstract void dropView(Connection con, String view) throws SQLException;

    public abstract void createFunction(Connection con, String name, String arguments, String query)
            throws SQLException;

    public abstract void dropFunction(Connection con, String function, String arguments) throws SQLException;

    public abstract void createObject(Connection con, String objectType, String objectName, String columnsAndOtherStuff)
            throws SQLException;

    public abstract void dropObject(Connection con, String objectType, String objectName) throws SQLException;

    // TODO: create more methods for sql queries: select, insert, update, delete,
    // alter table etc.

    public abstract void closeConnection(Connection con) throws SQLException;

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
            System.out.println("Configuration file must end with .properties but found: " + name);
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
