package demo;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import demo.state.Randomly;
import demo.state.State;
import demo.util.TestDbms;
import demo.util.TestUtil;
import demo.util.TestUtilFactory;

public class GlobalInfo {
    // global state tracker
    public static State currentState = State.INTIAL;

    public static final TestDbms testDbms = TestDbms.POSTGRES;

    public static TestUtil testUtil = TestUtilFactory.create(testDbms);

    static Connection baseConnection;
    // assume there can be multiple replication connections
    static Map<Connection, List<Connection>> replicationConnections = new HashMap<>();
    // assume there can be multiple read only connections
    static List<Connection> readOnlyConnections = new ArrayList<>();
    // assume there can be only one privileged connection
    static Connection privilegedConnection;

    // random pick a connection for replication
    public static Connection getAnyConnectionForReplication() {
        List<Connection> connections = new ArrayList<>();
        if (baseConnection != null) {
            connections.add(baseConnection);
        }
        if (!readOnlyConnections.isEmpty()) {
            connections.addAll(readOnlyConnections);
        }
        if (privilegedConnection != null) {
            connections.add(privilegedConnection);
        }
        Connection[] cons = new Connection[connections.size()];
        cons = connections.toArray(cons);
        return Randomly.fromOptions(cons);
    }

    public static void setBaseConnection(Connection baseConnection) {
        GlobalInfo.baseConnection = baseConnection;
    }

    public static void setReplicationConnections(Connection originalConnection, Connection replicationConnection) {
        if (GlobalInfo.replicationConnections.containsKey(originalConnection)) {
            GlobalInfo.replicationConnections.get(originalConnection).add(replicationConnection);
        } else {
            List<Connection> connections = new ArrayList<>();
            connections.add(replicationConnection);
            GlobalInfo.replicationConnections.put(originalConnection, connections);
        }
    }

    public static void setReadOnlyConnections(Connection readOnlyConnection) {
        GlobalInfo.readOnlyConnections.add(readOnlyConnection);
    }

    public static void setPrivilegedConnection(Connection privilegedConnection) {
        GlobalInfo.privilegedConnection = privilegedConnection;
    }

    public static Connection getBaseConnection() {
        return baseConnection;
    }

    public static List<Connection> getReplicationConnections(Connection originalConnection) {
        return GlobalInfo.replicationConnections.get(originalConnection);
    }

    public static List<Connection> getReadOnlyConnections() {
        return readOnlyConnections;
    }


}
