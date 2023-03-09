package demo.test;

import org.junit.Test;

import demo.state.Action;
import demo.state.State;
import demo.state.StateMachine;

public class StateMachineTest {
    @Test
    public void test() throws Exception {
        StateMachine stateMachine = new StateMachine();
        stateMachine.showCurrentState();
        stateMachine.nextState();
        stateMachine.showCurrentState();
        do {
            Action a = stateMachine.action();
            System.out.println("\t Perform Action: " + stateMachine.action());
            a.invoke();
            stateMachine.showCurrentState();
        } while (stateMachine.getCurrentState() != State.FINAL && stateMachine.getCurrentState() != State.EXCEPTION);

    }
}
