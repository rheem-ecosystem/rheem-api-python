package test;

import io.rheem.basic.data.Tuple2;
import io.rheem.basic.operators.*;
import io.rheem.core.api.RheemContext;
import io.rheem.core.function.FlatMapDescriptor;
import io.rheem.core.function.ReduceDescriptor;
import io.rheem.core.function.TransformationDescriptor;
import io.rheem.core.optimizer.costs.DefaultLoadEstimator;
import io.rheem.core.optimizer.costs.LoadEstimator;
import io.rheem.core.plan.rheemplan.RheemPlan;
import io.rheem.core.types.DataSetType;
import io.rheem.core.types.DataUnitType;
import io.rheem.core.util.Counter;
import io.rheem.java.Java;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Test {

    public static void main(String[] args) throws IOException {
        RheemContext rheemContext = new RheemContext().with(Java.basicPlugin());
        TextFileSource textFileSource = new TextFileSource(
                "file:///Users/rodrigopardomeza/scalytics/rheem-api-python/rheem-api-python-java/src/main/java/test/input.txt"
        );

        // for each line (input) output an iterator of the words
        FlatMapOperator<String, String> flatMapOperator = new FlatMapOperator<>(
                new FlatMapDescriptor<>(line -> Arrays.asList((String[]) line.split(" ")),
                        String.class,
                        String.class
                )
        );
        flatMapOperator.getFunctionDescriptor().setLoadEstimators(
                new DefaultLoadEstimator(1, 1, 0.9d, (inCards, outCards) -> inCards[0] * 670),
                LoadEstimator.createFallback(1, 1)
        );


        // for each word transform it to lowercase and output a key-value pair (word, 1)
        MapOperator<String, Tuple2<String, Integer>> mapOperator = new MapOperator<>(
                new TransformationDescriptor<>(word -> new Tuple2<String, Integer>(word.toLowerCase(), 1),
                        DataUnitType.<String>createBasic(String.class),
                        DataUnitType.<Tuple2<String, Integer>>createBasicUnchecked(Tuple2.class)
                ), DataSetType.createDefault(String.class),
                DataSetType.createDefaultUnchecked(Tuple2.class)
        );
        mapOperator.getFunctionDescriptor().setLoadEstimators(
                new DefaultLoadEstimator(1, 1, 0.9d, (inCards, outCards) -> inCards[0] * 245),
                LoadEstimator.createFallback(1, 1)
        );


        // groupby the key (word) and add up the values (frequency)
        ReduceByOperator<Tuple2<String, Integer>, String> reduceByOperator = new ReduceByOperator<>(
                new TransformationDescriptor<>(pair -> pair.field0,
                        DataUnitType.createBasicUnchecked(Tuple2.class),
                        DataUnitType.createBasic(String.class)), new ReduceDescriptor<>(
                ((a, b) -> {
                    a.field1 += b.field1;
                    return a;
                }), DataUnitType.createGroupedUnchecked(Tuple2.class),
                DataUnitType.createBasicUnchecked(Tuple2.class)
        ), DataSetType.createDefaultUnchecked(Tuple2.class)
        );
        reduceByOperator.getKeyDescriptor().setLoadEstimators(
                new DefaultLoadEstimator(1, 1, 0.9d, (inCards, outCards) -> inCards[0] * 50),
                LoadEstimator.createFallback(1, 1)
        );
        reduceByOperator.getReduceDescriptor().setLoadEstimators(
                new DefaultLoadEstimator(1, 1, 0.9d, (inCards, outCards) -> inCards[0] * 350 + 500000),
                LoadEstimator.createFallback(1, 1)
        );


        // write results to a sink
        List<Tuple2> results = new ArrayList<>();
        LocalCallbackSink<Tuple2> sink = LocalCallbackSink.createCollectingSink(results, DataSetType.createDefault(Tuple2.class));

        // Build Rheem plan by connecting operators
        textFileSource.connectTo(0, flatMapOperator, 0);
        flatMapOperator.connectTo(0, mapOperator, 0);
        mapOperator.connectTo(0, reduceByOperator, 0);
        reduceByOperator.connectTo(0, sink, 0);

        // Have Rheem execute the plan.
        rheemContext.execute(new RheemPlan(sink));

        for (Tuple2 r : results){
            System.out.println(r.getField0() + "= " + r.field1);
        }

        // Verify the plan result.
        Counter<String> counter = new Counter<>();
        List<Tuple2> correctResults = new ArrayList<>();
        final List<String> lines = Files.lines(Paths.get(
                "/Users/rodrigopardomeza/scalytics/rheem-api-python/rheem-api-python-java/src/main/java/test/results.txt"
        )).collect(Collectors.toList());
        lines.stream()
                .flatMap(line -> Arrays.stream(line.split("\\s+")))
                .map(String::toLowerCase)
                .forEach(counter::increment);

        for (Map.Entry<String, Integer> countEntry : counter) {
            correctResults.add(new Tuple2<>(countEntry.getKey(), countEntry.getValue()));
        }
    }
}
