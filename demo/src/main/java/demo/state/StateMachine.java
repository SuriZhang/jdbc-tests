package demo.state;

import demo.Randomly;

public final class StateMachine {
    private static State currentState = State.INITIAL;
    private static Action currentAction = null;
    
    private Exception exception = null;

    public State getCurrentState() {
        return currentState;
    }

    public Action getCurrentAction() {
        return currentAction;
    }

    public Exception getException() {
        return exception;
    }

    public void setException(Exception exception) {
        this.exception = exception;
    }

    public State nextState() {
        return currentState = currentState.nextState();
    }

    public Action selectAction() {
        Action[] candidates = currentState.actionCandidates();
        currentAction = candidates != null ? Randomly.fromOptions(candidates) : null;
        return currentAction;
    }

    public void showActionCandidates() {
        Action[] actions = currentState.actionCandidates();
        if (actions == null) {
            return;
        }
        System.out.println(actions.length + " actions are available in state " + currentState);
        for (Action a : actions) {
            System.out.println("\t" + a.toString());
        }
    }

    public void showCurrentState() {
        System.out.println("Current State: " + currentState);
    }

    public void advanceState(State targetState) {
        StateMachine.currentState = targetState;
    }

    public void reset() {
        currentState = State.INITIAL;
        currentAction = null;
    }
}
