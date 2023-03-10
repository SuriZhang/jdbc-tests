package demo.state;

import demo.Randomly;

public enum State {
    INITIAL {
        @Override
        State[] nextStateCandidates() {
            return new State[] { CONNECTION_OPENED };
        }

        @Override
        public Action[] actionCandidates() {
            return new Action[] { Action.OPEN_PRIVILEGED_CONNECTION, Action.OPEN_REPLICATION_CONNECTION,
                    Action.OPEN_READ_ONLY_CONNECTION, Action.OPEN_CONNECTION };
        }
    },
    CONNECTION_OPENED {
        @Override
        State[] nextStateCandidates() {
            return new State[] { CONNECTION_OPENED, CONNECTION_CLOSED, STATEMENT_EXECUTED };
        }

        @Override
        public Action[] actionCandidates() {
            return new Action[] { Action.OPEN_PRIVILEGED_CONNECTION, Action.OPEN_REPLICATION_CONNECTION,
                    Action.OPEN_READ_ONLY_CONNECTION, Action.OPEN_CONNECTION };
        }
    },
    STATEMENT_EXECUTED { // TODO: maybe break this down to more states: table created, table dropped,
                         // select, insert, update, delete?
        @Override
        State[] nextStateCandidates() {
            return new State[] { CONNECTION_OPENED, CONNECTION_CLOSED, STATEMENT_EXECUTED };
        }

        @Override
        public Action[] actionCandidates() {
            return new Action[] { Action.CREATE_SCHEMA, Action.DROP_SCHEMA, Action.CREATE_TABLE, Action.DROP_TABLE,
                    Action.CREATE_VIEW, Action.DROP_VIEW, Action.CREATE_FUNCTION, Action.DROP_FUNCTION };
        }
    },
    CONNECTION_CLOSED {
        @Override
        State[] nextStateCandidates() {
            return new State[] { CONNECTION_OPENED, CONNECTION_CLOSED, FINAL };
        }

        @Override
        public Action[] actionCandidates() {
            return new Action[] { Action.CLOSE_CONNECTION };
        }
    },
    EXCEPTION {
        @Override
        State[] nextStateCandidates() {
            return new State[] { FINAL };
        }

        @Override
        public Action[] actionCandidates() {
            return new Action[] { Action.HANDLE_EXCEPTION };
        }
    },
    FINAL {
        @Override
        State[] nextStateCandidates() {
            return null;
        }

        @Override
        public Action[] actionCandidates() {
            return null;
        }
    };

    // states that can be reached from this state
    abstract State[] nextStateCandidates();

    // actions that can be performed in this state
    public abstract Action[] actionCandidates();

    State nextState() {
        State[] candidates = nextStateCandidates();
        return candidates != null ? Randomly.fromOptions(candidates) : null;
    }
}