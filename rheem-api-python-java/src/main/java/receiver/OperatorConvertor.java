package receiver;

import io.rheem.basic.operators.FilterOperator;
import io.rheem.basic.operators.MapOperator;
import io.rheem.basic.operators.TextFileSink;
import io.rheem.basic.operators.TextFileSource;
import io.rheem.core.types.DataSetType;
import org.apache.http.conn.routing.RouteInfo;

public class OperatorConvertor {

    public static void operatorCreator(){

    }

    public static MapOperator<?,?> createMapOperator(byte[] udf, String wrapper, Class<Object> inputtype, Class<Object> outputtype){
        MapOperator<?, ?> m = new MapOperator<>(
                UdfConvertor.transformationConvertor(wrapper, udf, inputtype, outputtype),
                DataSetType.createDefault(inputtype),
                DataSetType.createDefault(outputtype)
        );

        return m;
    }

    public static FilterOperator<?> createFilterOperator(byte[] udf, String wrapper, Class<Object> type){
        FilterOperator<?> f = new FilterOperator<Object>(
                UdfConvertor.predicateConvertor(wrapper, udf, type),
                DataSetType.createDefault(type)
        );

        return f;
    }

    public static TextFileSource createTextFileSourceOperator(byte[] udf, String wrapper){
        TextFileSource tso = new TextFileSource("file:///Users/rodrigopardomeza/scalytics/rheem-api-python/rheem-api-python-java/src/main/java/test/input.txt");
        return tso;
    }

    public static TextFileSink<?> createTextFileSinkOperator(byte[] udf, String wrapper, Class<Object> type){
        TextFileSink<?> tsi = new TextFileSink<>("file:///Users/rodrigopardomeza/scalytics/rheem-api-python/rheem-api-python-java/src/main/java/test/results.txt", Object.class);
        return tsi;
    }
}
