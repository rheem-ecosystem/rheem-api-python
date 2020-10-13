package pyfunction;

import com.sun.tools.javac.util.List;
import executor.PyRheemExecutor;
import io.rheem.core.function.ExecutionContext;
import io.rheem.core.function.FunctionDescriptor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.function.Function;

public class ByteTransformationDescriptor<T, U> implements FunctionDescriptor.ExtendedSerializableFunction<T, U> {

    byte[] udf_in_python;
    public ByteTransformationDescriptor(byte[] udf_in_python) {
        this.udf_in_python = udf_in_python;
    }

    @Override
    public void open(ExecutionContext executionContext) {
        PyRheemExecutor.startExecService();
        PyRheemExecutor.execute(this.udf_in_python);
    }

    @Override
    public U apply(T t) {
        return (U) List.of(t);
        //return (U) PyRheemExecutor.apply(new ArrayList(Arrays.asList(1,2,3)));
        //return null;
        //return PyRheemExecutor.apply(t);
    }

}
