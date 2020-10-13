package pyfunction;

import executor.PyRheemExecutor;
import io.rheem.core.function.ExecutionContext;
import io.rheem.core.function.FunctionDescriptor;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import com.sun.tools.javac.util.List;

public class BytePredicateDescriptor<T> implements FunctionDescriptor.ExtendedSerializablePredicate<T> {

    byte[] udf_in_python;
    public BytePredicateDescriptor(byte[] udf_in_python) {
        this.udf_in_python = udf_in_python;
    }

    @Override
    public void open(ExecutionContext ctx) {
        PyRheemExecutor.startExecService();
        PyRheemExecutor.execute(this.udf_in_python);
    }

    @Override
    public boolean test(T t) {
        //Execute function with the value in the Py4j
        Object o = PyRheemExecutor.apply(List.of(t));
        List oo = List.of(o);
        return (boolean) oo.head;
    }

}
