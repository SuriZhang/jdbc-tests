package demo.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import javax.annotation.Nullable;

import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.jdbc.NonRegisteringDriver;

public final class MySqlTestUtil implements TestUtil {

    @Override
    public void createSchema(Connection con, String schema) throws SQLException {
        // SCHEMA is synonym for DATABASE in MySQL
        // see https://dev.mysql.com/doc/refman/8.0/en/create-database.html
        createObject(con, "DATABASE", schema, "");
    }

    @Override
    public void createTable(Connection con, String table, String columns) throws SQLException {
        createObject(con, "TABLE", table, "(" + columns + ")");
    }

    @Override
    public void createView(Connection con, String view, String query) throws SQLException {
        createObject(con, "VIEW", view, "AS " + query);
    }

    @Override
    public void dropSchema(Connection con, String schema) throws SQLException {
        dropObject(con, "DATABASE", schema);
    }

    @Override
    public void dropTable(Connection con, String table) throws SQLException {
        dropObject(con, "TABLE", table);

    }

    @Override
    public void dropView(Connection con, String view) throws SQLException {
        dropObject(con, "VIEW", view);
    }

    @Override
    public String getDatabase() {
        return System.getProperty("database", "test");
    }

    @Override
    public int getPort() {
        return Integer.parseInt(System.getProperty("port", "3306"));
    }

    @Override
    public String getServer() {
        return System.getProperty("server", "localhost");
    }

    @Override
    public String getURL() {
        String server = getServer();
        int port = getPort();
        String database = getDatabase();

        // // copied from mysql-connector-j::BaseTestCase
        // // TODO: need to verify
        // return "jdbc:mysql:///test";

        return "jdbc:mysql://"
        + server + ":" + port + "/"
        + database+ "?serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true";
    }

    public void initDriver() {
        Properties p = TestUtil.loadPropertyFiles("mysql.build.properties");
        p.putAll(System.getProperties());
        System.getProperties().putAll(p);
    }

    @Override
    public Connection openConnection() throws SQLException {
        return openConnection(new Properties());
    }

    @Override
    public Connection openConnection(Properties props) throws SQLException {
        initDriver();

        // Allow properties to override the user name.
        String user = props.getProperty("user");
        if (user == null) {
            user = System.getProperty("user");
        }
        if (user == null) {
            throw new IllegalArgumentException(
                    "user name is not specified. Please specify 'user' property via -D or build.properties");
        }
        props.setProperty("user", user);

        // Allow properties to override the password.
        String password = props.getProperty("passowrd");
        if (password == null) {
            password = System.getProperty("password") != null ? System.getProperty("password") : "";
        }
        props.setProperty("password", password);

        props.setProperty("sslMode", "DISABLED"); // testsuite is built upon non-SSL default connection
        props.setProperty("allowPublicKeyRetrieval", "true");
        props.setProperty("createDatabaseIfNotExist", "true");

        String url = getURL();
        return DriverManager.getConnection(url, props);
    }

    @Override
    public Connection openPriviligedConnection() throws SQLException {
        initDriver();
        Properties properties = new Properties();

        properties.setProperty("user", System.getProperty("privilegedUser"));
        properties.setProperty("password", System.getProperty("privilegedPassword"));

        return openConnection(properties);
    }

    @Override
    public Connection openReadOnlyConnection(@Nullable String option) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("user", System.getProperty("user"));
        properties.setProperty("password", System.getProperty("password"));
        properties.setProperty("sslMode", "DISABLED"); // testsuite is built upon non-SSL default connection
        properties.setProperty("allowPublicKeyRetrieval", "true");
        properties.setProperty("readOnly", "true");

        return openConnection(properties);
    }

    @Override
    public Connection openReplicationConnection(@Nullable Connection con) throws Exception {
        String replicationUrl = getSourceReplicaUrl(ConnectionUrl.Type.REPLICATION_CONNECTION.getScheme());
        
        Properties properties = new Properties();
        
        properties.setProperty("user", System.getProperty("privilegedUser"));
        properties.setProperty("password", System.getProperty("privilegedPassword"));
        properties.setProperty("sslMode", "DISABLED"); // testsuite is built upon non-SSL default connection
        properties.setProperty("allowPublicKeyRetrieval", "true");
        Connection replConn = new NonRegisteringDriver().connect(replicationUrl, properties);
        return replConn;
    }

    protected String getSourceReplicaUrl(String protocol) throws SQLException {
        String hostPortPair = TestUtil.encodePercent(getServer() + ":" + getPort());
        return String.format("%s//%s,%s/", protocol, hostPortPair, hostPortPair);
    }

    @Override
    public void createFunction(Connection con, String name, String arguments, String query) throws SQLException {
        String functionDefn = "(" + arguments + ") " + query;
        createObject(con, "FUNCTION", name, functionDefn);
    }

    @Override
    public void dropFunction(Connection con, String function, String arguments) throws SQLException {
        dropObject(con, "FUNCTION", function);
    }

    @Override
    public void createObject(Connection con, String objectType, String objectName, String columnsAndOtherStuff)
            throws SQLException {
        Statement st = con.createStatement();

        StringBuilder createSql = new StringBuilder(
                objectName.length() + objectType.length() + columnsAndOtherStuff.length() + 10);
        try {
            dropObject(con, objectType, objectName);
            createSql.append("CREATE ");
            createSql.append(objectType);
            createSql.append(" ");
            createSql.append(objectName);
            createSql.append(" ");
            createSql.append(columnsAndOtherStuff);

            st.executeUpdate(createSql.toString());
        } catch (SQLException sqlEx) {
            if ("42S01".equals(sqlEx.getSQLState())) {
                System.err.println(
                        "WARN: Stale mysqld table cache preventing table creation - flushing tables and trying again");
                st.executeUpdate("FLUSH TABLES"); // some bug in 5.1 on the mac causes tables to not disappear from the
                                                  // cache
                st.executeUpdate(createSql.toString());
            } else {
                throw sqlEx;
            }
        } finally {
            TestUtil.closeQuietly(st);
        }
    }

    @Override
    public void dropObject(Connection con, String objectType, String objectName) throws SQLException {
        Statement st = con.createStatement();
        try {
            if (!objectType.equalsIgnoreCase("USER")
                    || ((JdbcConnection) con).getSession().versionMeetsMinimum(5, 7, 8)) {
                st.executeUpdate("DROP " + objectType + " IF EXISTS " + objectName);
            } else {
                st.executeUpdate("DROP " + objectType + " " + objectName);
            }
            st.executeUpdate("flush privileges");
        } finally {
            TestUtil.closeQuietly(st);
        }
    }

    

}
