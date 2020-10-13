package executor;
import py4j.GatewayServer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class PyRheemExecutorTest {

    List<PythonExecutor> listeners = new ArrayList<PythonExecutor>();

    public void registerListener(PythonExecutor listener) {
        listeners.add(listener);
    }

    public void requestExecution(byte[] udf) {
        for (PythonExecutor listener: listeners) {
            Object returnValue = listener.execute(udf);
            System.out.println(returnValue);
        }
    }

    public void solution_provider(int solution){
        System.out.println("The solution was " + solution);
    }

    public static void execute(byte[] udf){
        PyRheemExecutorTest application = new PyRheemExecutorTest();
        GatewayServer server = new GatewayServer(application);
        server.start(true);
        System.out.println("Py4J Java Server on Execution...");

        try {
            TimeUnit.SECONDS.sleep(10);
            application.requestExecution(udf);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}

