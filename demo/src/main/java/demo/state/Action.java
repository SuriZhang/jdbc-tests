package demo.state;

import java.sql.Connection;
import java.sql.SQLException;

import demo.GlobalInfo;
import demo.Randomly;
import demo.Schema;
import demo.Schema.Table;
import demo.util.TestDbms;

public enum Action {
    OPEN_PRIVILIGED_CONNECTION {
        @Override
        public void invoke() throws Exception {
            // Connection con;
            // try {
            // con = GlobalInfo.testUtil.openPriviligedConnection();
            // } catch (SQLException e) {
            // // jump to EXCEPTION state if exception is caught
            // GlobalInfo.stateMachine.advanceState(State.EXCEPTION);
            // throw e;
            // }
            // GlobalInfo.setPrivilegedConnection(con);
            // make sure we update state only if statement execution was successful
            GlobalInfo.stateMachine.advanceState(State.CONNECTION_OPENED.nextState());
        }
    },
    OPEN_REPLICATION_CONNECTION {
        @Override
        public void invoke() throws Exception {
            // Connection con = GlobalInfo.getAnyConnectionForReplication();
            // if (con == null) {
            // GlobalInfo.stateMachine.advanceState(State.EXCEPTION);
            // throw new IllegalStateException("no connection available for replication");
            // }
            // Connection replicationConnection;
            // try {
            // replicationConnection = GlobalInfo.testUtil.openReplicationConnection(con);
            // } catch (SQLException e) {
            // // jump to EXCEPTION state if exception is caught
            // GlobalInfo.stateMachine.advanceState(State.EXCEPTION);
            // throw e;
            // }
            // GlobalInfo.setReplicationConnection(con, replicationConnection);
            // make sure we update state only if statement execution was successful
            GlobalInfo.stateMachine.advanceState(State.CONNECTION_OPENED.nextState());
        }
    },
    OPEN_READ_ONLY_CONNECTION {
        @Override
        public void invoke() throws Exception {
            // only postgres has options for read-only connections
            // String option = null;
            // if (GlobalInfo.testDbms == TestDbms.POSTGRES) {
            // option = Randomly.fromOptions("ignore", "transaction", "always");
            // }
            // Connection readOnlyConnection;
            // try {
            // readOnlyConnection = GlobalInfo.testUtil.openReadOnlyConnection(option);
            // } catch (Exception e) {
            // GlobalInfo.stateMachine.advanceState(State.EXCEPTION);
            // throw e;
            // }
            // GlobalInfo.setReadOnlyConnection(readOnlyConnection);
            // make sure we update state only if statement execution was successful
            GlobalInfo.stateMachine.advanceState(State.CONNECTION_OPENED.nextState());

        }
    },
    OPEN_CONNECTION {
        @Override
        public void invoke() throws Exception {
            // Connection con;
            // try {
            // con = GlobalInfo.testUtil.openConnection();
            // } catch (Exception e) {
            // GlobalInfo.stateMachine.advanceState(State.EXCEPTION);
            // throw e;
            // }
            // GlobalInfo.setBasicConnection(con);
            // make sure we update state only if statement execution was successful
            GlobalInfo.stateMachine.advanceState(State.CONNECTION_OPENED.nextState());
        }
    },
    CREATE_SCHEMA {
        @Override
        public void invoke() throws Exception {
            // Connection con = getAnyConnectionOrThrows();
            // String schema = Schema.generateSchemaName(con);
            // try {
            // GlobalInfo.testUtil.createSchema(con, schema);
            // } catch (SQLException e) {
            // // jump to EXCEPTION state if exception is caught
            // GlobalInfo.stateMachine.advanceState(State.EXCEPTION);
            // throw e;
            // }
            // GlobalInfo.addNewSchema(con, schema);
            GlobalInfo.stateMachine.advanceState(State.STATEMENT_EXECUTED.nextState());
        }
    },
    DROP_SCHEMA {
        @Override
        public void invoke() throws Exception {
            // Connection con = getAnyConnectionOrThrows();
            // String schema = GlobalInfo.getAnySchema(con);
            // if (schema == null) {
            // GlobalInfo.stateMachine.advanceState(State.EXCEPTION);
            // throw new IllegalStateException("no schema available");
            // }
            // try {
            // GlobalInfo.testUtil.dropSchema(con, schema);
            // } catch (SQLException e) {
            // // jump to EXCEPTION state if exception is caught
            // GlobalInfo.stateMachine.advanceState(State.EXCEPTION);
            // throw e;
            // }
            // GlobalInfo.removeSchema(con, schema);
            GlobalInfo.stateMachine.advanceState(State.STATEMENT_EXECUTED.nextState());
        }
    },
    CREATE_TABLE {
        @Override
        public void invoke() throws Exception {
            // Connection con = getAnyConnectionOrThrows();
            // Table table = Schema.generateTable(con);
            // try {
            // GlobalInfo.testUtil.createTable(con, table.getName(),
            // table.columnsToString());

            // } catch (SQLException e) {
            // // jump to EXCEPTION state if exception is caught
            // GlobalInfo.stateMachine.advanceState(State.EXCEPTION);
            // throw e;
            // }

            // GlobalInfo.addNewTable(con, table);
            GlobalInfo.stateMachine.advanceState(State.STATEMENT_EXECUTED.nextState());
        }
    },
    DROP_TABLE {
        @Override
        public void invoke() throws Exception {
            // Connection con = getAnyConnectionOrThrows();
            // Table table = GlobalInfo.getAnyTable(con);
            // if (table == null) {
            // GlobalInfo.stateMachine.advanceState(State.EXCEPTION);
            // throw new IllegalStateException("no table available");
            // }
            // try {
            // GlobalInfo.testUtil.dropTable(con, table.getName());

            // } catch (Exception e) {
            // // jump to EXCEPTION state if exception is caught
            // GlobalInfo.stateMachine.advanceState(State.EXCEPTION);
            // throw e;
            // }
            // GlobalInfo.removeTable(con, table);
            GlobalInfo.stateMachine.advanceState(State.STATEMENT_EXECUTED.nextState());
        }
    },
    CREATE_VIEW {
        @Override
        public void invoke() throws Exception {
            if (Randomly.getBooleanWithSmallProbability()) {
                GlobalInfo.stateMachine.advanceState(State.EXCEPTION);
                throw new UnsupportedOperationException("not implemented yet");
            } else {
                GlobalInfo.stateMachine.advanceState(State.STATEMENT_EXECUTED.nextState());
            }
        }
    },
    DROP_VIEW {
        @Override
        public void invoke() throws Exception {
            if (Randomly.getBooleanWithSmallProbability()) {
                GlobalInfo.stateMachine.advanceState(State.EXCEPTION);
                throw new UnsupportedOperationException("not implemented yet");
            } else {
                GlobalInfo.stateMachine.advanceState(State.STATEMENT_EXECUTED.nextState());
            }
        }
    },
    CREATE_FUNCTION {
        @Override
        public void invoke() throws Exception {
            if (Randomly.getBooleanWithSmallProbability()) {
                GlobalInfo.stateMachine.advanceState(State.EXCEPTION);
                throw new UnsupportedOperationException("not implemented yet");
            } else {
                GlobalInfo.stateMachine.advanceState(State.STATEMENT_EXECUTED.nextState());
            }
        }
    },
    DROP_FUNCTION {
        @Override
        public void invoke() throws Exception {
            if (Randomly.getBooleanWithSmallProbability()) {
                GlobalInfo.stateMachine.advanceState(State.EXCEPTION);
                throw new UnsupportedOperationException("not implemented yet");
            } else {
                GlobalInfo.stateMachine.advanceState(State.STATEMENT_EXECUTED.nextState());
            }
        }
    },
    // leaving out CREATE_OBJECT and DROP_OBJECT for now

    CLOSE_CONNECTION {
        @Override
        public void invoke() throws Exception {
            // Connection con = getAnyConnectionOrThrows();
            // try {
            // GlobalInfo.testUtil.closeConnection(con);

            // } catch (Exception e) {
            // // jump to EXCEPTION state if exception is caught
            // GlobalInfo.stateMachine.advanceState(State.EXCEPTION);
            // throw e;
            // }
            // GlobalInfo.removeConnection(con);

            GlobalInfo.stateMachine.advanceState(State.CONNECTION_CLOSED.nextState());

        }
    };

    public abstract void invoke() throws Exception;

    Connection getAnyConnectionOrThrows() throws Exception {
        Connection con = GlobalInfo.getAnyConnection();
        if (con == null) {
            GlobalInfo.stateMachine.advanceState(State.EXCEPTION);
            throw new Exception("no connection available");
        }
        return con;
    }
}
