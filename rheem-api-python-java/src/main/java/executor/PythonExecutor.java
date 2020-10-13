package executor;

public interface PythonExecutor {

    Object execute(byte[] obj);
    Object apply(Object elem);
}
