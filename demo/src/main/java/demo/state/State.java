package demo.state;

public enum State {
    INTIAL {
        @Override
        public State[] nextStateCandidates() {
            return new State[] { CONNECTION_OPENED };
        }

        @Override
        public Action[] actionCandidates() {
            return new Action[] {};
        }
    },
    CONNECTION_OPENED {
        @Override
        public State[] nextStateCandidates() {
            return new State[] { CONNECTION_OPENED, CONNECTION_CLOSED, STATEMENT_EXECUTED };
        }

        @Override
        public Action[] actionCandidates() {
            return new Action[] { Action.OPEN_PRIVILIGED_CONNECTION, Action.OPEN_REPLICATION_CONNECTION,
                    Action.OPEN_READ_ONLY_CONNECTION, Action.OPEN_CONNECTION };
        }
    },
    STATEMENT_EXECUTED { // TODO: maybe break this down to more states: table created, table dropped,
                         // select, insert, update, delete?
        @Override
        public State[] nextStateCandidates() {
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
        public State[] nextStateCandidates() {
            return new State[] { CONNECTION_OPENED, CONNECTION_CLOSED, FINAL };
        }

        @Override
        public Action[] actionCandidates() {
            return new Action[] { Action.CLOSE_CONNECTION };
        }
    },

    FINAL {
        @Override
        public State[] nextStateCandidates() {
            return new State[] {};
        }

        @Override
        public Action[] actionCandidates() {
            return new Action[] {};
        }
    };

    // states that can be reached from this state
    public abstract State[] nextStateCandidates();

    // actions that can be performed in this state
    public abstract Action[] actionCandidates();

    // returns a random next state from nextStateCandidates()
    public State nextState() {
        return Randomly.fromOptions(nextStateCandidates());
    }

    // returns a random action from actionCandidates()
    public Action action() {
        return Randomly.fromOptions(actionCandidates());
    }
}