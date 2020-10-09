package pyfunction;

import io.rheem.core.function.ExecutionContext;
import io.rheem.core.function.FunctionDescriptor;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class BytePredicateDescriptor<T> implements FunctionDescriptor.ExtendedSerializablePredicate<T> {

    byte[] udf_in_python;
    public BytePredicateDescriptor(byte[] udf_in_python) {
        this.udf_in_python = udf_in_python;
    }
    @Override
    public void open(ExecutionContext ctx) {
        ProcessBuilder builder = new ProcessBuilder("/Users/rodrigopardomeza/scalytics/rheem-api-python/pyrheem/executor/execute.py");
        //Create the Python to Py4j
        //Envio el UDF to python
        //call the unserilized in python
    }
    @Override
    public boolean test(T t) {
        //Execute function with the value in the Py4j
        return false;
    }

    public static void main(String[] args) {

        List<String> logs = new ArrayList<>();
        try {
            ProcessBuilder builder = new ProcessBuilder("/Users/rodrigopardomeza/scalytics/rheem-api-python/pyrheem/executor/execute.py");
            Process p = builder.start();
            BufferedReader stdInput = new BufferedReader(new
                    InputStreamReader(p.getInputStream()));

            stdInput.lines()
                    .forEach(
                            logs::add
                    );
        }catch (Exception e){

        }
    }
}
