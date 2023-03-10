package demo.state;

import java.sql.Connection;
import java.sql.SQLException;

import javax.annotation.Nullable;

import demo.GlobalInfo;
import demo.Randomly;
import demo.Schema;
import demo.Schema.Table;
import demo.util.TestDbms;

public enum Action {
    OPEN_PRIVILEGED_CONNECTION {
        @Override
        public void invoke() throws Exception {
            Connection con;
            try {
                con = GlobalInfo.testUtil.openPriviligedConnection();
            } catch (SQLException e) {
                // jump to EXCEPTION state if exception is caught
                GlobalInfo.stateMachine.setException(e);
                GlobalInfo.stateMachine.advanceState(State.EXCEPTION);
                return;
            }
            GlobalInfo.setPrivilegedConnection(con);
            // make sure we update state only if statement execution was successful
            GlobalInfo.stateMachine.advanceState(State.CONNECTION_OPENED.nextState());
        }
    },
    OPEN_REPLICATION_CONNECTION {
        @Override
        public void invoke() throws Exception {
            Connection con = GlobalInfo.getAnyConnectionForReplication();
            if (con == null) {
                GlobalInfo.stateMachine.advanceState(State.EXCEPTION);
                GlobalInfo.stateMachine.setException(new IllegalStateException("no connection available for replication"));
            }
            Connection replicationConnection;
            try {
                replicationConnection = GlobalInfo.testUtil.openReplicationConnection(con);
            } catch (SQLException e) {
                // jump to EXCEPTION state if exception is caught
                GlobalInfo.stateMachine.setException(e);
                GlobalInfo.stateMachine.advanceState(State.EXCEPTION);
                return;
            }
            GlobalInfo.setReplicationConnection(con, replicationConnection);
            // make sure we update state only if statement execution was successful
            GlobalInfo.stateMachine.advanceState(State.CONNECTION_OPENED.nextState());
        }
    },
    OPEN_READ_ONLY_CONNECTION {
        @Override
        public void invoke() throws Exception {
            // only postgres has options for read-only connections
            String option = null;
            if (GlobalInfo.testDbms == TestDbms.POSTGRES) {
                option = Randomly.fromOptions("ignore", "transaction", "always");
                System.out.println("\t option: " + option);
            }
            Connection readOnlyConnection;
            try {
                readOnlyConnection = GlobalInfo.testUtil.openReadOnlyConnection(option);
            } catch (Exception e) {
                GlobalInfo.stateMachine.advanceState(State.EXCEPTION);
                GlobalInfo.stateMachine.setException(e);
                return;
            }
            GlobalInfo.setReadOnlyConnection(readOnlyConnection);
            // make sure we update state only if statement execution was successful
            GlobalInfo.stateMachine.advanceState(State.CONNECTION_OPENED.nextState());

        }
    },
    OPEN_CONNECTION {
        @Override
        public void invoke() throws Exception {
            Connection con;
            try {
                con = GlobalInfo.testUtil.openConnection();
            } catch (Exception e) {
                GlobalInfo.stateMachine.advanceState(State.EXCEPTION);
                GlobalInfo.stateMachine.setException(e);
                return;
            }
            GlobalInfo.setBasicConnection(con);
            // make sure we update state only if statement execution was successful
            GlobalInfo.stateMachine.advanceState(State.CONNECTION_OPENED.nextState());
        }
    },
    CREATE_SCHEMA {
        @Override
        public void invoke() throws Exception {
            Connection con = getAnyConnectionOrThrows();
            String schema = Schema.generateSchemaName(con);
            try {
                GlobalInfo.testUtil.createSchema(con, schema);
            } catch (SQLException e) {
                // jump to EXCEPTION state if exception is caught
                GlobalInfo.stateMachine.advanceState(State.EXCEPTION);
                GlobalInfo.stateMachine.setException(e);
                return;
            }
            System.out.println("\t Create schema: " + schema);
            GlobalInfo.addNewSchema(con, schema);
            GlobalInfo.stateMachine.advanceState(State.STATEMENT_EXECUTED.nextState());
        }
    },
    DROP_SCHEMA {
        @Override
        public void invoke() throws Exception {
            Connection con = getAnyConnectionOrThrows();
            String schema = GlobalInfo.getAnySchema(con);
            if (schema == null) {
                GlobalInfo.stateMachine.advanceState(State.EXCEPTION);
                GlobalInfo.stateMachine.setException(new IllegalStateException("no schema available"));
                return;
            }
            try {
                GlobalInfo.testUtil.dropSchema(con, schema);
            } catch (SQLException e) {
                // jump to EXCEPTION state if exception is caught
                GlobalInfo.stateMachine.advanceState(State.EXCEPTION);
                GlobalInfo.stateMachine.setException(e);
                return;
            }
            System.out.println("\t Drop schema: " + schema);
            GlobalInfo.removeSchema(con, schema);
            GlobalInfo.stateMachine.advanceState(State.STATEMENT_EXECUTED.nextState());
        }
    },
    CREATE_TABLE {
        @Override
        public void invoke() throws Exception {
            Connection con = getAnyConnectionOrThrows();
            Table table = Schema.generateTable(con);
            try {
                GlobalInfo.testUtil.createTable(con, table.getName(),
                        table.columnsToString());
            } catch (SQLException e) {
                // jump to EXCEPTION state if exception is caught
                GlobalInfo.stateMachine.advanceState(State.EXCEPTION);
                GlobalInfo.stateMachine.setException(e);
                return;
            }
            System.out.println("\t Create table: " + table.getName() + " with columns: " + table.columnsToString());
            GlobalInfo.addNewTable(con, table);
            GlobalInfo.stateMachine.advanceState(State.STATEMENT_EXECUTED.nextState());
        }
    },
    DROP_TABLE {
        @Override
        public void invoke() throws Exception {
            Connection con = getAnyConnectionOrThrows();
            Table table = GlobalInfo.getAnyTable(con);
            if (table == null) {
                GlobalInfo.stateMachine.advanceState(State.EXCEPTION);
                GlobalInfo.stateMachine.setException(new IllegalStateException("no table available"));
                return;
            }
            try {
                GlobalInfo.testUtil.dropTable(con, table.getName());
                System.out.println("\t Drop table: " + table.getName());

            } catch (Exception e) {
                // jump to EXCEPTION state if exception is caught
                GlobalInfo.stateMachine.advanceState(State.EXCEPTION);
                GlobalInfo.stateMachine.setException(e);
                return;
            }
            GlobalInfo.removeTable(con, table);
            GlobalInfo.stateMachine.advanceState(State.STATEMENT_EXECUTED.nextState());
        }
    },
    CREATE_VIEW {
        @Override
        public void invoke() throws Exception {
            if (Randomly.getBooleanWithSmallProbability()) {
                GlobalInfo.stateMachine.advanceState(State.EXCEPTION);
                GlobalInfo.stateMachine.setException(new UnsupportedOperationException("not implemented yet"));
                return;
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
                GlobalInfo.stateMachine.setException(new UnsupportedOperationException("not implemented yet"));
                return;
            } else {
                GlobalInfo.stateMachine.advanceState(State.STATEMENT_EXECUTED.nextState());
            }
        }
    },
    CREATE_FUNCTION {
        @Override
        public void invoke() throws Exception {
            Connection con = getAnyConnectionOrThrows();
            try {
                GlobalInfo.testUtil.createFunction(con, "test_blob", "REFCURSOR, int4",
                        "RETURN BOOLEAN AS $$ BEGIN  SELECT * FROM pg_catalog.pg_tables; END; $$ LANGUAGE plpgsql;");
            } catch (Exception e) {
                // jump to EXCEPTION state if exception is caught
                GlobalInfo.stateMachine.advanceState(State.EXCEPTION);
                GlobalInfo.stateMachine.setException(e);
                return;
            }
            GlobalInfo.stateMachine.advanceState(State.STATEMENT_EXECUTED.nextState());
        }
    },
    DROP_FUNCTION {
        @Override
        public void invoke() throws Exception {
            Connection con = getAnyConnectionOrThrows();
            try {
                GlobalInfo.testUtil.dropFunction(con, "test_blob", "REFCURSOR, int4");
            } catch (Exception e) {
                // jump to EXCEPTION state if exception is caught
                GlobalInfo.stateMachine.advanceState(State.EXCEPTION);
                GlobalInfo.stateMachine.setException(e);
                return;
            }

            GlobalInfo.stateMachine.advanceState(State.STATEMENT_EXECUTED.nextState());
        }
    },
    // leaving out CREATE_OBJECT and DROP_OBJECT for now

    CLOSE_CONNECTION {
        @Override
        public void invoke() throws Exception {
            Connection con = getAnyConnectionOrThrows();
            try {
                GlobalInfo.testUtil.closeConnection(con);

            } catch (Exception e) {
                // jump to EXCEPTION state if exception is caught
                GlobalInfo.stateMachine.advanceState(State.EXCEPTION);
                GlobalInfo.stateMachine.setException(e);
                return;
            }
            // maybe close a connection multiple times, do not remove it from the list
            // GlobalInfo.removeConnection(con);

            GlobalInfo.stateMachine.advanceState(State.CONNECTION_CLOSED.nextState());

        }
    },
    HANDLE_EXCEPTION { // quietly handle exception
        @Override
        public void invoke() throws Exception {
            // log exception
            System.out.println("\t Exception: " + GlobalInfo.stateMachine.getException());
            // TODO: compare with expected exception
            GlobalInfo.stateMachine.advanceState(State.EXCEPTION.nextState());
        }
    };

    public abstract void invoke() throws Exception;

    @Nullable
    Connection getAnyConnectionOrThrows() {
        Connection con = GlobalInfo.getAnyConnection();
        if (con == null) {
            GlobalInfo.stateMachine.advanceState(State.EXCEPTION);
            GlobalInfo.stateMachine.setException(new Exception(
                    GlobalInfo.stateMachine.getCurrentState() + " getAnyConnectionOrThrows: no connection available"));
            return null;
        }
        return con;
    }
}
