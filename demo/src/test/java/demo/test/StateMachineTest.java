package demo.test;

import org.junit.Test;

import demo.GlobalInfo;
import demo.state.Action;
import demo.state.State;
import demo.util.TestDbms;

public class StateMachineTest {
    @Test
    public void test_1() throws Exception {
        for (int i = 0; i < 100; i++) {

            GlobalInfo.setTestDbms(TestDbms.POSTGRES);
            GlobalInfo.stateMachine.showCurrentState();
            while (GlobalInfo.stateMachine.getCurrentState() != State.FINAL) {
                GlobalInfo.stateMachine.selectAction();
                System.out.println("\t Perform Action: " + GlobalInfo.stateMachine.getCurrentAction());
                GlobalInfo.stateMachine.getCurrentAction().invoke();
                GlobalInfo.stateMachine.showCurrentState();
            }
        }
    }

    @Test
    public void test_2() throws Exception {
        GlobalInfo.setTestDbms(TestDbms.POSTGRES);
        Action.OPEN_READ_ONLY_CONNECTION.invoke();
    }

    @Test
    public void test_3() throws Exception {
        GlobalInfo.setTestDbms(TestDbms.POSTGRES);
        Action.OPEN_CONNECTION.invoke();
        Action.CREATE_TABLE.invoke();
        Action.CREATE_TABLE.invoke();

    }
}
