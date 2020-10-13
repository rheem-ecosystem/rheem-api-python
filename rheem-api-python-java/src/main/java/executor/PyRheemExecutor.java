package executor;
import io.rheem.core.api.exception.RheemException;
import py4j.GatewayServer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class PyRheemExecutor {

    List<PythonExecutor> listeners = new ArrayList<PythonExecutor>();
    static PyRheemExecutor application = null;

    public void registerListener(PythonExecutor listener) {
        listeners.add(listener);
    }

    public void requestExecution(byte[] udf) {

        if (listeners.size() == 0){
            throw new RheemException("There is not Python Executioners to process");
        }

        for (PythonExecutor listener: listeners) {
            listener.execute(udf);
        }
    }

    public void solution_provider(int solution){
        System.out.println("The solution was " + solution);
    }

    public static void execute(byte[] udf){

        if(application == null){

            /* Start service before with PyRheemExecutor.startExecService*/
            throw new RheemException("PyRheemExecutor not functional, start service before");
        }

        application.requestExecution(udf);

    }

    public Object requestApplication(Object t) {

        if (listeners.size() == 0){
            throw new RheemException("There is not Python Executioners to process");
        }

        for (PythonExecutor listener: listeners) {
            return listener.apply(t);
        }

        return null;
    }

    public static Object apply(Object t){
        return application.requestApplication(t);
    }

    public static void startExecService(){
        application = new PyRheemExecutor();
        GatewayServer server = new GatewayServer(application);
        server.start(true);
        System.out.println("Py4J Java Server on Execution...");

        ProcessBuilder pb =
                new ProcessBuilder(
                        "/Users/rodrigopardomeza/rheem/PyExecutor/executor.py"
                );

        try{
            Process p = pb.start();
            BufferedReader stdError = new BufferedReader(new
                    InputStreamReader(p.getErrorStream()));

            stdError.lines()
                    .forEach(
                            line -> {
                                System.out.println(line);
                            }
                    );

            BufferedReader stdInput = new BufferedReader(new
                    InputStreamReader(p.getInputStream()));

            stdInput.lines()
                    .forEach(
                            line -> {
                                System.out.println(line);
                            }
                    );

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}

