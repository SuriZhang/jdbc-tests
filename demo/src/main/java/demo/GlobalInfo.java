package demo;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import demo.Schema.Table;
import demo.state.StateMachine;
import demo.util.TestDbms;
import demo.util.TestUtil;
import demo.util.TestUtilFactory;

public class GlobalInfo {
    // global state tracker
    public static final StateMachine stateMachine = new StateMachine();

    public static TestDbms testDbms;
    public static TestUtil testUtil;

    private static List<Connection> basicConnections = new ArrayList<>();
    // assume there can be multiple replication connections
    private static Map<Connection, List<Connection>> replicationConnections = new HashMap<>();
    // assume there can be multiple read only connections
    private static List<Connection> readOnlyConnections = new ArrayList<>();
    // assume there can be only one privileged connection
    private static Connection privilegedConnection;

    // store Connection:Table(Columns)
    // TODO: handle views later
    private static Map<Connection, List<Table>> tables = new HashMap<>();
    // store Connection:Schema
    private static Map<Connection, List<String>> schemas = new HashMap<>();

    public static TestUtil setTestDbms(TestDbms testDbms) {
        GlobalInfo.testDbms = testDbms;
        GlobalInfo.testUtil = TestUtilFactory.create(testDbms);
        return GlobalInfo.testUtil;
    }

    @Nullable
    public static List<Connection> allConnections() {
        List<Connection> connections = new ArrayList<>();
        if (!basicConnections.isEmpty()) {
            connections.addAll(basicConnections);
        }
        if (!readOnlyConnections.isEmpty()) {
            connections.addAll(readOnlyConnections);
        }
        if (privilegedConnection != null) {
            connections.add(privilegedConnection);
        }
        if (!replicationConnections.isEmpty()) {
            for (List<Connection> cons : replicationConnections.values()) {
                connections.addAll(cons);
            }
        }
        if (connections.isEmpty()) {
            return null;
        }
        return connections;
    }

    // random pick a connection for replication
    @Nullable
    public static Connection getAnyConnectionForReplication() {
        List<Connection> connections = allConnections();
        // remove replication connections from the list
        for (List<Connection> cons : replicationConnections.values()) {
            connections.removeAll(cons);
        }

        Connection[] cons = new Connection[connections.size()];
        cons = connections.toArray(cons);
        return Randomly.fromOptions(cons);
    }

    @Nullable
    public static Connection getAnyConnection() {
        List<Connection> connections = allConnections();
        if (connections == null) {
            return null;
        }
        Connection[] cons = new Connection[connections.size()];
        cons = connections.toArray(cons);
        return Randomly.fromOptions(cons);
    }

    public static void setBasicConnection(Connection connection) {
        GlobalInfo.basicConnections.add(connection);
    }

    public static void setReplicationConnection(Connection originalConnection, Connection replicationConnection) {
        if (GlobalInfo.replicationConnections.containsKey(originalConnection)) {
            GlobalInfo.replicationConnections.get(originalConnection).add(replicationConnection);
        } else {
            List<Connection> connections = new ArrayList<>();
            connections.add(replicationConnection);
            GlobalInfo.replicationConnections.put(originalConnection, connections);
        }
    }

    public static void setReadOnlyConnection(Connection readOnlyConnection) {
        GlobalInfo.readOnlyConnections.add(readOnlyConnection);
    }

    public static void setPrivilegedConnection(Connection privilegedConnection) {
        GlobalInfo.privilegedConnection = privilegedConnection;
    }

    @Nullable
    public static List<Connection> getBasicConnections() {
        return basicConnections.isEmpty()   ? null : basicConnections;
    }

    @Nullable
    public static List<Connection> getReplicationConnections(Connection originalConnection) {
        return GlobalInfo.replicationConnections.get(originalConnection);
    }

    @Nullable
    public static List<Connection> getReadOnlyConnections() {
        return readOnlyConnections.isEmpty() ? null : readOnlyConnections;
    }

    @Nullable
    public static Connection getPrivilegedConnection() {
        return privilegedConnection;
    }

    public static void removeConnection(Connection con) {
        if (basicConnections.contains(con)) {
            basicConnections.remove(con);
        }
        if (readOnlyConnections.contains(con)) {
            readOnlyConnections.remove(con);
        }
        if (privilegedConnection == con) {
            privilegedConnection = null;
        }
        if (replicationConnections.containsKey(con)) {
            replicationConnections.remove(con);
        }
        for (List<Connection> cons : replicationConnections.values()) {
            if (cons.contains(con)) {
                cons.remove(con);
            }
        }
    }

    public static void addNewTable(Connection con, Table table) {
        if (GlobalInfo.tables.containsKey(con)) {
            GlobalInfo.tables.get(con).add(table);
        } else {
            List<Table> tables = new ArrayList<>();
            tables.add(table);
            GlobalInfo.tables.put(con, tables);
        }
    }

    @Nullable
    public static List<Table> getTables(Connection con) {
        return GlobalInfo.tables.get(con);
    }

    public static void addNewSchema(Connection con, String schema) {
        if (GlobalInfo.schemas.containsKey(con)) {
            GlobalInfo.schemas.get(con).add(schema);
        } else {
            List<String> schemas = new ArrayList<>();
            schemas.add(schema);
            GlobalInfo.schemas.put(con, schemas);
        }
    }

    public static void removeSchema(Connection con, String schema) {
        if (GlobalInfo.schemas.containsKey(con)) {
            GlobalInfo.schemas.get(con).remove(schema);
        }
    }

    @Nullable
    public static List<String> getSchemas(Connection con) {
        return GlobalInfo.schemas.get(con);
    }

    @Nullable
    public static String getAnySchema(Connection con) {
        List<String> schemas = GlobalInfo.schemas.get(con);
        if (schemas == null) {
            return null;
        }
        return Randomly.fromOptions(schemas.toArray(new String[schemas.size()]));
    }

    @Nullable
    public static Table getAnyTable(Connection con) {
        List<Table> tables = GlobalInfo.tables.get(con);
        if (tables == null) {
            return null;
        }
        return Randomly.fromOptions(tables.toArray(new Table[tables.size()]));
    }

    public static void removeTable(Connection con, Table table) {
        if (GlobalInfo.tables.containsKey(con)) {
            GlobalInfo.tables.get(con).remove(table);
        }
    }

}
