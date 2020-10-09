package receiver;

import io.rheem.basic.data.Tuple2;
import io.rheem.core.api.exception.RheemException;
import io.rheem.core.function.PredicateDescriptor;
import io.rheem.core.function.TransformationDescriptor;

public class UdfConvertor {

    private enum wrapperTypes{
        predicate,
        transform,
        url,
        urltype
    }

    public static PredicateDescriptor<Object> predicateConvertor(String wrapper, byte[] udf, Class<Object> type){
        if(wrapper.equals(wrapperTypes.predicate.toString())){
            return new PredicateDescriptor<>(
                    t -> true,
                    type
            );
        }

        throw new RheemException("Wrong type called");
    }

    public static TransformationDescriptor<Object, Object> transformationConvertor(String wrapper, byte[] udf, Class<Object> inputtype, Class<Object> outputtype){
        if(wrapper.equals(wrapperTypes.transform.toString())){
            return new TransformationDescriptor<>(
                    //t -> new Tuple2<>(t, 1),
                    t -> "lala",
                    inputtype,
                    outputtype
            );
        }

        throw new RheemException("Wrong type called");
    }


}
