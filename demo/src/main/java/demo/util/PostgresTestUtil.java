package demo.util;

/**
 *  This class is a copy of org.postgresql.test.TestUtil with minor modifications.
 *  We kept all the methods on purpose in case of future usage.
 */

import org.postgresql.PGConnection;
import org.postgresql.core.TransactionState;
import org.postgresql.core.BaseConnection;
import org.postgresql.jdbc.PgConnection;

import demo.ResourceLock;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nullable;

public final class PostgresTestUtil implements TestUtil {
    /*
     * The case is as follows:
     * 1. Typically the database and hostname are taken from System.properties or
     * build.properties or build.local.properties
     * That enables to override test DB via system property
     * 2. There are tests where different DBs should be used (e.g. SSL tests), so we
     * can't just use DB name from system property
     * That is why _test_ properties exist: they overpower System.properties and
     * build.properties
     */
    public final String SERVER_HOST_PORT_PROP = "_test_hostport";
    public final String DATABASE_PROP = "_test_database";

    private final ResourceLock lock = new ResourceLock();

    /*
     * Returns the Test database JDBC URL
     */
    @Override
    public String getURL() {
        return getURL(getServer(), +getPort());
    }

    public String getURL(String server, int port) {
        return getURL(server + ":" + port, getDatabase());
    }

    public String getURL(String hostport, String database) {
        String protocolVersion = "";
        if (getProtocolVersion() != 0) {
            protocolVersion = "&protocolVersion=" + getProtocolVersion();
        }

        String options = "";
        if (getOptions() != null) {
            options = "&options=" + getOptions();
        }

        String binaryTransfer = "";
        if (getBinaryTransfer() != null && !getBinaryTransfer().equals("")) {
            binaryTransfer = "&binaryTransfer=" + getBinaryTransfer();
        }

        String receiveBufferSize = "";
        if (getReceiveBufferSize() != -1) {
            receiveBufferSize = "&receiveBufferSize=" + getReceiveBufferSize();
        }

        String sendBufferSize = "";
        if (getSendBufferSize() != -1) {
            sendBufferSize = "&sendBufferSize=" + getSendBufferSize();
        }

        String ssl = "";
        if (getSSL() != null) {
            ssl = "&ssl=" + getSSL();
        }

        return "jdbc:postgresql://"
                + hostport + "/"
                + database
                + "?ApplicationName=Driver Tests"
                + protocolVersion
                + options
                + binaryTransfer
                + receiveBufferSize
                + sendBufferSize
                + ssl;
    }

    /*
     * Returns the Test server
     */
    @Override
    public String getServer() {
        return System.getProperty("server", "localhost");
    }

    /*
     * Returns the Test port
     */
    @Override
    public int getPort() {
        return Integer.parseInt(System.getProperty("port", System.getProperty("def_pgport")));
    }

    /*
     * Returns the server side prepared statement threshold.
     */
    public int getPrepareThreshold() {
        return Integer.parseInt(System.getProperty("preparethreshold", "5"));
    }

    public int getProtocolVersion() {
        return Integer.parseInt(System.getProperty("protocolVersion", "0"));
    }

    public String getOptions() {
        return System.getProperty("options");
    }

    /*
     * Returns the Test database
     */
    @Override
    public String getDatabase() {
        return System.getProperty("database");
    }

    /*
     * Returns the Postgresql username
     */
    public String getUser() {
        return System.getProperty("user");
    }

    /*
     * Returns the user's password
     */
    public String getPassword() {
        return System.getProperty("password");
    }

    /*
     * Returns password for default callbackhandler
     */
    public String getSslPassword() {
        return System.getProperty("sslpassword");
    }

    /*
     * Return the GSSEncMode for the tests
     */
    public String getGSSEncMode() {
        return System.getProperty("gssEncMode", "allow"); // adapted from pgjdbc GSSEncMode, default to allow
    }

    /*
     * Returns the user for SSPI authentication tests
     */
    public String getSSPIUser() {
        return System.getProperty("sspiusername");
    }

    /*
     * postgres like user
     */
    public String getPrivilegedUser() {
        return System.getProperty("privilegedUser");
    }

    public String getPrivilegedPassword() {
        return System.getProperty("privilegedPassword");
    }

    /*
     * Returns the binary transfer mode to use
     */
    public String getBinaryTransfer() {
        return System.getProperty("binaryTransfer");
    }

    public int getSendBufferSize() {
        return Integer.parseInt(System.getProperty("sendBufferSize", "-1"));
    }

    public int getReceiveBufferSize() {
        return Integer.parseInt(System.getProperty("receiveBufferSize", "-1"));
    }

    public String getSSL() {
        return System.getProperty("ssl");
    }

    {
        try {
            initDriver();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Unable to initialize driver", e);
        }
    }

    private boolean initialized = false;

    private Properties sslTestProperties = null;

    private void initSslTestProperties() {
        try (ResourceLock ignore = lock.obtain()) {
            if (sslTestProperties == null) {
                sslTestProperties = TestUtil.loadPropertyFiles("ssltest.properties");
            }
        }
    }

    private String getSslTestProperty(String name) {
        initSslTestProperties();
        return sslTestProperties.getProperty(name);
    }

    // public void assumeSslTestsEnabled() {
    //     Assume.assumeTrue(Boolean.parseBoolean(getSslTestProperty("enable_ssl_tests")));
    // }

    public String getSslTestCertPath(String name) {
        File certdir = TestUtil.getFile(getSslTestProperty("certdir"));
        return new File(certdir, name).getAbsolutePath();
    }

    public void initDriver() {
        try (ResourceLock ignore = lock.obtain()) {
            if (initialized) {
                return;
            }

            Properties p = TestUtil.loadPropertyFiles("postgresql.build.properties");
            p.putAll(System.getProperties());
            System.getProperties().putAll(p);

            initialized = true;
        }
    }

    /**
     * Get a connection using a privileged user mostly for tests that the ability to
     * load C functions
     * now as of 4/14.
     *
     * @return connection using a privileged user mostly for tests that the ability
     *         to load C
     *         functions now as of 4/14
     */
    @Override
    public Connection openPriviligedConnection() throws SQLException {
        initDriver();
        Properties properties = new Properties();

        properties.setProperty("gssEncMode", getGSSEncMode());
        properties.setProperty("user", getPrivilegedUser());
        properties.setProperty("password", getPrivilegedPassword());
        properties.setProperty("options", "-c synchronous_commit=on");
        return DriverManager.getConnection(getURL(), properties);
    }

    @Override
    public Connection openReplicationConnection(@Nullable Connection con) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("assumeMinServerVersion", "9.4");
        properties.setProperty("protocolVersion", "3");
        properties.setProperty("replication", "database");
        // Only simple query protocol available for replication connection
        properties.setProperty("preferQueryMode", "simple");
        properties.setProperty("user", getPrivilegedUser());
        properties.setProperty("password", getPrivilegedPassword());
        properties.setProperty("options", "-c synchronous_commit=on");
        return openConnection(properties);
    }

    @Override
    public Connection openReadOnlyConnection(String option) throws Exception {
        // option can be "ignore", "transaction", "always"
        Properties props = new Properties();
        props.setProperty("readOnlyMode", option);
        return openConnection(props);
    }

    /**
     * Helper - opens a connection.
     *
     * @return connection
     */
    @Override
    public Connection openConnection() throws SQLException {
        return openConnection(new Properties());
    }

    /*
     * Helper - opens a connection with the allowance for passing additional
     * parameters, like
     * "compatible".
     */
    @Override
    public Connection openConnection(Properties props) throws SQLException {
        initDriver();

        // Allow properties to override the user name.
        String user = props.getProperty("user");
        if (user == null) {
            user = getUser();
        }
        if (user == null) {
            throw new IllegalArgumentException(
                    "user name is not specified. Please specify 'user' property via -D or build.properties");
        }
        props.setProperty("user", user);

        // Allow properties to override the password.
        String password = props.getProperty("passowrd");
        if (password == null) {
            password = getPassword() != null ? getPassword() : "";
        }
        props.setProperty("password", password);

        String sslPassword = getSslPassword();
        if (sslPassword != null) {
            props.setProperty("sslpassword", sslPassword);
        }

        if (!props.containsKey("prepareThreshold")) {
            props.setProperty("prepareThreshold", Integer.toString(getPrepareThreshold()));
        }
        if (!props.containsKey("preferQueryMode")) {
            String value = System.getProperty("preferQueryMode");
            if (value != null) {
                props.setProperty("preferQueryMode", value);
            }
        }
        // Enable Base4 tests to override host,port,database
        String hostport = props.getProperty(SERVER_HOST_PORT_PROP, getServer() + ":" + getPort());
        String database = props.getProperty(DATABASE_PROP, getDatabase());

        // Set GSSEncMode for tests only in the case the property is already missing
        if (props.getProperty("gssEncMode") == null) {
            props.put("gssEncMode", getGSSEncMode());
        }
        String url = getURL(hostport, database);
        return DriverManager.getConnection(url, props);
    }

    /*
     * Helper - creates a test schema for use by a test
     */
    @Override
    public void createSchema(Connection con, String schema) throws SQLException {
        Statement st = con.createStatement();
        try {
            // Drop the schema
            dropSchema(con, schema);

            // Now create the schema
            String sql = "CREATE SCHEMA " + schema;

            st.executeUpdate(sql);
        } finally {
            TestUtil.closeQuietly(st);
        }
    }

    /*
     * Helper - drops a schema
     */
    @Override
    public void dropSchema(Connection con, String schema) throws SQLException {
        dropObject(con, "SCHEMA", schema);
    }

    /*
     * Helper - creates a test table for use by a test
     */
    @Override
    public void createTable(Connection con, String table, String columns) throws SQLException {
        Statement st = con.createStatement();
        try {
            // Drop the table
            dropTable(con, table);

            // Now create the table
            String sql = "CREATE TABLE " + table + " (" + columns + ")";

            st.executeUpdate(sql);
        } finally {
            TestUtil.closeQuietly(st);
        }
    }

    /**
     * Helper creates a temporary table.
     *
     * @param con     Connection
     * @param table   String
     * @param columns String
     */
    public void createTempTable(Connection con, String table, String columns)
            throws SQLException {
        Statement st = con.createStatement();
        try {
            // Drop the table
            dropTable(con, table);

            // Now create the table
            st.executeUpdate("create temp table " + table + " (" + columns + ")");
        } finally {
            TestUtil.closeQuietly(st);
        }
    }

    /*
     * Helper - creates a unlogged table for use by a test.
     * Unlogged tables works from PostgreSQL 9.1+
     */
    public void createUnloggedTable(Connection con, String table, String columns)
            throws SQLException {
        Statement st = con.createStatement();
        try {
            // Drop the table
            dropTable(con, table);

            String unlogged = haveMinimumServerVersion(con, 901000) ? "UNLOGGED" : ""; // ServerVersion.v9_1

            // Now create the table
            st.executeUpdate("CREATE " + unlogged + " TABLE " + table + " (" + columns + ")");
        } finally {
            TestUtil.closeQuietly(st);
        }
    }

    /*
     * Helper - creates a view
     */
    @Override
    public void createView(Connection con, String viewName, String query)
            throws SQLException {
        try (Statement st = con.createStatement()) {
            // Drop the view
            dropView(con, viewName);

            String sql = "CREATE VIEW " + viewName + " AS " + query;

            st.executeUpdate(sql);
        }
    }

    /*
     * Helper - creates a materialized view
     */
    public void createMaterializedView(Connection con, String matViewName, String query)
            throws SQLException {
        try (Statement st = con.createStatement()) {
            // Drop the view
            dropMaterializedView(con, matViewName);

            String sql = "CREATE MATERIALIZED VIEW " + matViewName + " AS " + query;

            st.executeUpdate(sql);
        }
    }

    /**
     * Helper creates an enum type.
     *
     * @param con    Connection
     * @param name   String
     * @param values String
     */
    public void createEnumType(Connection con, String name, String values)
            throws SQLException {
        Statement st = con.createStatement();
        try {
            dropType(con, name);

            // Now create the table
            st.executeUpdate("create type " + name + " as enum (" + values + ")");
        } finally {
            TestUtil.closeQuietly(st);
        }
    }

    /**
     * Helper creates an composite type.
     *
     * @param con    Connection
     * @param name   String
     * @param values String
     */
    public void createCompositeType(Connection con, String name, String values) throws SQLException {
        createCompositeType(con, name, values, true);
    }

    /**
     * Helper creates an composite type.
     *
     * @param con    Connection
     * @param name   String
     * @param values String
     */
    public void createCompositeType(Connection con, String name, String values, boolean shouldDrop)
            throws SQLException {
        Statement st = con.createStatement();
        try {
            if (shouldDrop) {
                dropType(con, name);
            }
            // Now create the type
            st.executeUpdate("CREATE TYPE " + name + " AS (" + values + ")");
        } finally {
            TestUtil.closeQuietly(st);
        }
    }

    /**
     * Drops a domain.
     *
     * @param con    Connection
     * @param domain String
     */
    public void dropDomain(Connection con, String domain)
            throws SQLException {
        dropObject(con, "DOMAIN", domain);
    }

    /**
     * Helper creates a domain.
     *
     * @param con    Connection
     * @param name   String
     * @param values String
     */
    public void createDomain(Connection con, String name, String values)
            throws SQLException {
        Statement st = con.createStatement();
        try {
            dropDomain(con, name);
            // Now create the table
            st.executeUpdate("create domain " + name + " as " + values);
        } finally {
            TestUtil.closeQuietly(st);
        }
    }

    /*
     * drop a sequence because older versions don't have dependency information for
     * serials
     */
    public void dropSequence(Connection con, String sequence) throws SQLException {
        dropObject(con, "SEQUENCE", sequence);
    }

    /*
     * Helper - drops a table
     */
    public void dropTable(Connection con, String table) throws SQLException {
        dropObject(con, "TABLE", table);
    }

    /*
     * Helper - drops a view
     */
    public void dropView(Connection con, String view) throws SQLException {
        dropObject(con, "VIEW", view);
    }

    /*
     * Helper - drops a materialized view
     */
    public void dropMaterializedView(Connection con, String matView) throws SQLException {
        dropObject(con, "MATERIALIZED VIEW", matView);
    }

    /*
     * Helper - drops a type
     */
    public void dropType(Connection con, String type) throws SQLException {
        dropObject(con, "TYPE", type);
    }

    @Override
    public void createObject(Connection con, String type, String name, String columnsAndOtherStuff)
            throws SQLException {
        Statement st = con.createStatement();
        try {
            // Drop the object
            dropObject(con, type, name);

            st.executeUpdate("create " + type + " " + name + " " + columnsAndOtherStuff);
        } finally {
            TestUtil.closeQuietly(st);
        }
    }

    @Override
    public void createFunction(Connection con, String name, String arguments, String query) throws SQLException {
        Statement stmt = con.createStatement();
        try {
            stmt.executeUpdate("CREATE FUNCTION " + name + "(" + arguments + ") " + query + " LANGUAGE SQL");
        } finally {
            TestUtil.closeQuietly(stmt);
        }
    }

    /*
     * Drops a function with a given signature.
     */
    @Override
    public void dropFunction(Connection con, String name, String arguments) throws SQLException {
        dropObject(con, "FUNCTION", name + "(" + arguments + ")");
    }

    @Override
    public void dropObject(Connection con, String type, String name) throws SQLException {
        Statement stmt = con.createStatement();
        try {
            if (con.getAutoCommit()) {
                // Not in a transaction so ignore error for missing object
                stmt.executeUpdate("DROP " + type + " IF EXISTS " + name + " CASCADE");
            } else {
                // In a transaction so do not ignore errors for missing object
                stmt.executeUpdate("DROP " + type + " " + name + " CASCADE");
            }
        } finally {
            TestUtil.closeQuietly(stmt);
        }
    }

    

    // public void assertTransactionState(String message, Connection con, TransactionState expected) {
    //     TransactionState actual = getTransactionState(con);
    //     assertEquals(message, expected, actual);
    // }

    /*
     * Helper - generates INSERT SQL - very simple
     */
    public String insertSQL(String table, String values) {
        return insertSQL(table, null, values);
    }

    public String insertSQL(String table, String columns, String values) {
        String s = "INSERT INTO " + table;

        if (columns != null) {
            s = s + " (" + columns + ")";
        }

        return s + " VALUES (" + values + ")";
    }

    /*
     * Helper - generates SELECT SQL - very simple
     */
    public String selectSQL(String table, String columns) {
        return selectSQL(table, columns, null, null);
    }

    public String selectSQL(String table, String columns, String where) {
        return selectSQL(table, columns, where, null);
    }

    public String selectSQL(String table, String columns, String where, String other) {
        String s = "SELECT " + columns + " FROM " + table;

        if (where != null) {
            s = s + " WHERE " + where;
        }
        if (other != null) {
            s = s + " " + other;
        }

        return s;
    }

    /*
     * Helper to prefix a number with leading zeros - ugly but it works...
     *
     * @param v value to prefix
     *
     * @param l number of digits (0-10)
     */
    public String fix(int v, int l) {
        String s = "0000000000".substring(0, l) + Integer.toString(v);
        return s.substring(s.length() - l);
    }

    public String escapeString(Connection con, String value) throws SQLException {
        if (con == null) {
            throw new NullPointerException("Connection is null");
        }
        if (con instanceof PgConnection) {
            return ((PgConnection) con).escapeString(value);
        }
        return value;
    }

    public boolean getStandardConformingStrings(Connection con) {
        if (con == null) {
            throw new NullPointerException("Connection is null");
        }
        if (con instanceof PgConnection) {
            return ((PgConnection) con).getStandardConformingStrings();
        }
        return false;
    }

    /**
     * Determine if the given connection is connected to a server with a version of
     * at least the given
     * version. This is convenient because we are working with a
     * java.sql.Connection, not an Postgres
     * connection.
     */
    public boolean haveMinimumServerVersion(Connection con, int version) throws SQLException {
        if (con == null) {
            throw new NullPointerException("Connection is null");
        }
        if (con instanceof PgConnection) {
            return ((PgConnection) con).haveMinimumServerVersion(version);
        }
        return false;
    }

    // public void assumeHaveMinimumServerVersion(int version)
    //         throws SQLException {
    //     try (Connection conn = openPriviligedConnection()) {
    //         Assume.assumeTrue(haveMinimumServerVersion(conn, version));
    //     }
    // }

    public boolean haveMinimumJVMVersion(String version) {
        String jvm = java.lang.System.getProperty("java.version");
        return (jvm.compareTo(version) >= 0);
    }

    public boolean haveIntegerDateTimes(Connection con) {
        if (con == null) {
            throw new NullPointerException("Connection is null");
        }
        if (con instanceof PgConnection) {
            return ((PgConnection) con).getQueryExecutor().getIntegerDateTimes();
        }
        return false;
    }

    /**
     * Print a ResultSet to System.out. This is useful for debugging tests.
     */
    public void printResultSet(ResultSet rs) throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();
        for (int i = 1; i <= rsmd.getColumnCount(); i++) {
            if (i != 1) {
                System.out.print(", ");
            }
            System.out.print(rsmd.getColumnName(i));
        }
        System.out.println();
        while (rs.next()) {
            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                if (i != 1) {
                    System.out.print(", ");
                }
                System.out.print(rs.getString(i));
            }
            System.out.println();
        }
    }

    public List<String> resultSetToLines(ResultSet rs) throws SQLException {
        List<String> res = new ArrayList<String>();
        ResultSetMetaData rsmd = rs.getMetaData();
        StringBuilder sb = new StringBuilder();
        while (rs.next()) {
            sb.setLength(0);
            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                if (i != 1) {
                    sb.append(',');
                }
                sb.append(rs.getString(i));
            }
            res.add(sb.toString());
        }
        return res;
    }

    public String join(List<String> list) {
        StringBuilder sb = new StringBuilder();
        for (String s : list) {
            if (sb.length() > 0) {
                sb.append('\n');
            }
            sb.append(s);
        }
        return sb.toString();
    }

    /*
     * Find the column for the given label. Only SQLExceptions for system or set-up
     * problems are
     * thrown. The PSQLState.UNDEFINED_COLUMN type exception is consumed to allow
     * cleanup. Relying on
     * the caller to detect if the column lookup was successful.
     */
    public int findColumn(PreparedStatement query, String label) throws SQLException {
        int returnValue = 0;
        ResultSet rs = query.executeQuery();
        if (rs.next()) {
            try {
                returnValue = rs.findColumn(label);
            } catch (SQLException sqle) {
            } // consume exception to allow cleanup of resource.
        }
        rs.close();
        return returnValue;
    }

    public void recreateLogicalReplicationSlot(Connection connection, String slotName, String outputPlugin)
            throws SQLException, InterruptedException, TimeoutException {
        // drop previous slot
        dropReplicationSlot(connection, slotName);

        PreparedStatement stm = null;
        try {
            stm = connection.prepareStatement("SELECT * FROM pg_create_logical_replication_slot(?, ?)");
            stm.setString(1, slotName);
            stm.setString(2, outputPlugin);
            stm.execute();
        } finally {
            TestUtil.closeQuietly(stm);
        }
    }

    public void recreatePhysicalReplicationSlot(Connection connection, String slotName)
            throws SQLException, InterruptedException, TimeoutException {
        // drop previous slot
        dropReplicationSlot(connection, slotName);

        PreparedStatement stm = null;
        try {
            stm = connection.prepareStatement("SELECT * FROM pg_create_physical_replication_slot(?)");
            stm.setString(1, slotName);
            stm.execute();
        } finally {
            TestUtil.closeQuietly(stm);
        }
    }

    public void dropReplicationSlot(Connection connection, String slotName)
            throws SQLException, InterruptedException, TimeoutException {
        if (haveMinimumServerVersion(connection, 90500)) { // 90500 for Server_version v9_5
            PreparedStatement stm = null;
            try {
                stm = connection.prepareStatement(
                        "select pg_terminate_backend(active_pid) from pg_replication_slots "
                                + "where active = true and slot_name = ?");
                stm.setString(1, slotName);
                stm.execute();
            } finally {
                TestUtil.closeQuietly(stm);
            }
        }

        waitStopReplicationSlot(connection, slotName);

        PreparedStatement stm = null;
        try {
            stm = connection.prepareStatement(
                    "select pg_drop_replication_slot(slot_name) "
                            + "from pg_replication_slots where slot_name = ?");
            stm.setString(1, slotName);
            stm.execute();
        } finally {
            TestUtil.closeQuietly(stm);
        }
    }

    public boolean isReplicationSlotActive(Connection connection, String slotName)
            throws SQLException {
        PreparedStatement stm = null;
        ResultSet rs = null;

        try {
            stm = connection.prepareStatement("select active from pg_replication_slots where slot_name = ?");
            stm.setString(1, slotName);
            rs = stm.executeQuery();
            return rs.next() && rs.getBoolean(1);
        } finally {
            TestUtil.closeQuietly(rs);
            TestUtil.closeQuietly(stm);
        }
    }

    /**
     * Execute a SQL query with a given connection and return whether any rows were
     * returned. No column data is fetched.
     */
    public boolean executeQuery(Connection conn, String sql) throws SQLException {
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        boolean hasNext = rs.next();
        rs.close();
        stmt.close();
        return hasNext;
    }

    /**
     * Execute a SQL query with a given connection, fetch the first row, and return
     * its
     * string value.
     */
    // public String queryForString(Connection conn, String sql) throws SQLException {
    //     Statement stmt = conn.createStatement();
    //     ResultSet rs = stmt.executeQuery(sql);
    //     Assert.assertTrue("Query should have returned exactly one row but none was found: " + sql, rs.next());
    //     String value = rs.getString(1);
    //     Assert.assertFalse("Query should have returned exactly one row but more than one found: " + sql, rs.next());
    //     rs.close();
    //     stmt.close();
    //     return value;
    // }

    /**
     * Execute a SQL query with a given connection, fetch the first row, and return
     * its
     * boolean value.
     */
    // public Boolean queryForBoolean(Connection conn, String sql) throws SQLException {
    //     Statement stmt = conn.createStatement();
    //     ResultSet rs = stmt.executeQuery(sql);
    //     Assert.assertTrue("Query should have returned exactly one row but none was found: " + sql, rs.next());
    //     Boolean value = rs.getBoolean(1);
    //     if (rs.wasNull()) {
    //         value = null;
    //     }
    //     Assert.assertFalse("Query should have returned exactly one row but more than one found: " + sql, rs.next());
    //     rs.close();
    //     stmt.close();
    //     return value;
    // }

    /**
     * Retrieve the backend process id for a given connection.
     */
    public int getBackendPid(Connection conn) throws SQLException {
        PGConnection pgConn = conn.unwrap(PGConnection.class);
        return pgConn.getBackendPID();
    }

    public boolean isPidAlive(Connection conn, int pid) throws SQLException {
        String sql = haveMinimumServerVersion(conn, 90200) // ServerVersion.v9_2
                ? "SELECT EXISTS (SELECT * FROM pg_stat_activity WHERE pid = ?)" // 9.2+ use pid column
                : "SELECT EXISTS (SELECT * FROM pg_stat_activity WHERE procpid = ?)"; // Use older procpid
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, pid);
            try (ResultSet rs = stmt.executeQuery()) {
                rs.next();
                return rs.getBoolean(1);
            }
        }
    }

    public boolean waitForBackendTermination(Connection conn, int pid)
            throws SQLException, InterruptedException {
        return waitForBackendTermination(conn, pid, Duration.ofSeconds(30), Duration.ofMillis(10));
    }

    /**
     * Wait for a backend process to terminate and return whether it actual
     * terminated within the maximum wait time.
     */
    public boolean waitForBackendTermination(Connection conn, int pid, Duration timeout, Duration sleepDelay)
            throws SQLException, InterruptedException {
        long started = System.currentTimeMillis();
        do {
            if (!isPidAlive(conn, pid)) {
                return true;
            }
            Thread.sleep(sleepDelay.toMillis());
        } while ((System.currentTimeMillis() - started) < timeout.toMillis());
        return !isPidAlive(conn, pid);
    }

    /**
     * Create a new connection to the same database as the supplied connection but
     * with the privileged credentials.
     */
    private Connection createPrivilegedConnection(Connection conn) throws SQLException {
        String url = conn.getMetaData().getURL();
        Properties props = new Properties(conn.getClientInfo());
        props.setProperty("USER", getPrivilegedUser());
        props.setProperty("PASSWORD", getPrivilegedPassword());
        return DriverManager.getConnection(url, props);
    }

    /**
     * Executed pg_terminate_backend(...) to terminate the server process for
     * a given process id with the given connection.
     * This method does not wait for the backend process to exit.
     */
    private boolean pgTerminateBackend(Connection privConn, int backendPid) throws SQLException {
        try (PreparedStatement stmt = privConn.prepareStatement("SELECT pg_terminate_backend(?)")) {
            stmt.setInt(1, backendPid);
            try (ResultSet rs = stmt.executeQuery()) {
                rs.next();
                return rs.getBoolean(1);
            }
        }
    }

    /**
     * Open a new privileged connection to the same database as connection and use
     * it to ask to terminate the connection.
     * If the connection is terminated, wait for its process to actual terminate.
     */
    public boolean terminateBackend(Connection conn) throws SQLException, InterruptedException {
        try (Connection privConn = createPrivilegedConnection(conn)) {
            int pid = getBackendPid(conn);
            if (!pgTerminateBackend(privConn, pid)) {
                return false;
            }
            return waitForBackendTermination(privConn, pid);
        }
    }

    /**
     * Open a new privileged connection to the same database as connection and use
     * it to ask to terminate the connection.
     * NOTE: This function does not wait for the process to terminate.
     */
    public boolean terminateBackendNoWait(Connection conn) throws SQLException {
        try (Connection privConn = createPrivilegedConnection(conn)) {
            int pid = getBackendPid(conn);
            return pgTerminateBackend(privConn, pid);
        }
    }

    public TransactionState getTransactionState(Connection conn) {
        return ((BaseConnection) conn).getTransactionState();
    }

    private void waitStopReplicationSlot(Connection connection, String slotName)
            throws InterruptedException, TimeoutException, SQLException {
        long startWaitTime = System.currentTimeMillis();
        boolean stillActive;
        long timeInWait = 0;

        do {
            stillActive = isReplicationSlotActive(connection, slotName);
            if (stillActive) {
                TimeUnit.MILLISECONDS.sleep(100L);
                timeInWait = System.currentTimeMillis() - startWaitTime;
            }
        } while (stillActive && timeInWait <= 30000);

        if (stillActive) {
            throw new TimeoutException("Wait stop replication slot " + timeInWait + " timeout occurs");
        }
    }

    /**
     * Executes given SQL via {@link Statement#execute(String)} on a given
     * connection.
     * 
     * @deprecated prefer {@link #execute(Connection, String)} since it yields
     *             easier for read code
     */
    @Deprecated
    public void execute(String sql, Connection connection) throws SQLException {
        execute(connection, sql);
    }

    /**
     * Executes given SQL via {@link Statement#execute(String)} on a given
     * connection.
     */
    public void execute(Connection connection, String sql) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
        }
    }

}
