package demo.state;

import java.sql.Connection;

import demo.GlobalInfo;
import demo.util.TestDbms;

public enum Action {
    OPEN_PRIVILIGED_CONNECTION {
        @Override
        public void invoke() throws Exception {
            GlobalInfo.setPrivilegedConnection(GlobalInfo.testUtil.openPriviligedConnection());
        }
    },
    OPEN_REPLICATION_CONNECTION {
        @Override
        public void invoke() throws Exception {
            Connection con = GlobalInfo.getAnyConnectionForReplication();
            GlobalInfo.setReplicationConnections(con, GlobalInfo.testUtil.openReplicationConnection(con));
        }
    },
    OPEN_READ_ONLY_CONNECTION {
        @Override
        public void invoke() throws Exception {
            // only postgres has options for read-only connections
            String option = null;
            if (GlobalInfo.testDbms == TestDbms.POSTGRES) {
                option = Randomly.fromOptions("ignore", "transaction", "always");
            }
            GlobalInfo.setReadOnlyConnections(GlobalInfo.testUtil.openReadOnlyConnection(option));
        }
    },
    OPEN_CONNECTION {
        @Override
        public void invoke() throws Exception {
            GlobalInfo.setBaseConnection(GlobalInfo.testUtil.openConnection());
        }
    },
    CREATE_SCHEMA {
        @Override
        public void invoke() throws Exception {
            GlobalInfo.testUtil.createSchema(...);
        }
    },
    DROP_SCHEMA {
        @Override
        public void invoke() throws Exception {
            GlobalInfo.testUtil.dropSchema(...);
        }
    },
    CREATE_TABLE {
        @Override
        public void invoke() throws Exception {
            GlobalInfo.testUtil.createTable(...);
        }
    },
    DROP_TABLE {
        @Override
        public void invoke() throws Exception {
            GlobalInfo.testUtil.dropTable(...);
        }
    },
    CREATE_VIEW {
        @Override
        public void invoke() throws Exception {
            GlobalInfo.testUtil.createView(...);
        }
    },
    DROP_VIEW {
        @Override
        public void invoke() throws Exception {
            GlobalInfo.testUtil.dropView(...);
        }
    },
    CREATE_FUNCTION {
        @Override
        public void invoke() throws Exception {
            GlobalInfo.testUtil.createFunction(...);
        }
    },
    DROP_FUNCTION {
        @Override
        public void invoke() throws Exception {
            GlobalInfo.testUtil.dropFunction(...);
        }
    },
    // leaving out CREATE_OBJECT and DROP_OBJECT for now
    
    CLOSE_CONNECTION {
        @Override
        public void invoke() throws Exception {
            GlobalInfo.testUtil.closeConnection(..);
        }
    };

    public abstract void invoke() throws Exception;
}
