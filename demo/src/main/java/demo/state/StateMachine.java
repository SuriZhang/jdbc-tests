package demo.state;

import demo.Randomly;

public final class StateMachine {
    private static State currentState = State.INTIAL;

    public State getCurrentState() {
        return currentState;
    }

    public State nextState() {
        return currentState = currentState.nextState();
    }

    public Action action() {
        Action[] candidates = currentState.actionCandidates();
        return candidates != null ? Randomly.fromOptions(candidates) : null;
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
}
