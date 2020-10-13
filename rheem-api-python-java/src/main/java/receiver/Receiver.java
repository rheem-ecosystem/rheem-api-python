package receiver;
import descriptor.OperatorSeedRef;
import descriptor.Rheemintegration.RheemPlan;
import descriptor.Rheemintegration.OperatorSeed;
import io.rheem.basic.operators.FilterOperator;
import io.rheem.basic.operators.MapOperator;
import io.rheem.basic.operators.TextFileSink;
import io.rheem.basic.operators.TextFileSource;
import io.rheem.core.api.RheemContext;
import io.rheem.core.api.exception.RheemException;
import io.rheem.core.plan.rheemplan.OperatorBase;
import io.rheem.core.types.DataSetType;
import io.rheem.java.Java;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Receiver {

    public static void main(String[] args) throws Exception {
        System.out.println("java");
        if(args.length != 1){
            System.err.println("Mete la wea bien po ql");
            System.exit(-1);
        }

        RheemPlan plan = RheemPlan.parseFrom(new FileInputStream(args[0]));

        print(plan);
        //createExecutablePlan(plan);

        /*RheemContext rheemContext = new RheemContext().with(Java.basicPlugin());
        TextFileSource op1 = OperatorConvertor.createTextFileSourceOperator(new byte[0], "url");
        FilterOperator<?> op2 = OperatorConvertor.createFilterOperator(new byte[0], "predicate", Object.class);
        MapOperator<?, ?> op3 = OperatorConvertor.createMapOperator(new byte[0], "transform", Object.class, Object.class);
        TextFileSink<?> op4 = OperatorConvertor.createTextFileSinkOperator(new byte[0] , "urltype", Object.class);

        op1.connectTo(0, op2, 0);
        op2.connectTo(0, op3, 0);
        op3.connectTo(0, op4, 0);

        rheemContext.execute(new io.rheem.core.plan.rheemplan.RheemPlan(op4));*/
    }

    private static void createExecutablePlan(RheemPlan plan) {
        RheemContext rheemContext = new RheemContext().with(Java.basicPlugin());

        /**
         * Contains OperatorSeedRef, each includes Slot positions that are being used by another Operators
         * ordinalInputs and ordinalOutputs indicates which position on InputSlots or OutputSlots (Given by value)
         * is used by a related Operator (Given by Key)
         */
        Map<Long, OperatorSeedRef> dict_seed = new HashMap<Long, OperatorSeedRef>();

        /**
         * Contains OperatorBase executable implementations (E.g. FilterOperator)
         */
        Map<Long, OperatorBase> dict_rheem = new HashMap<Long, OperatorBase>();

        //TODO Can this work with multiple sinks?
        List<OperatorSeedRef> sinks = new ArrayList<>();
        OperatorBase sink = null;

        //Setting references to seeds
        for (OperatorSeed seed : plan.getOperatorsList()) {
            dict_seed.put(
                    seed.getId(),
                    new OperatorSeedRef(
                            seed.getId(),
                            seed.getKind(),
                            seed.getUdf().getFunction(),
                            seed.getUdf().getWrapper(),
                            seed.getInputSlotsList(),
                            seed.getOutputSlotsList()
                    )
            );

            // Creating RheemOperators, at this step they are not connected
            OperatorBase rheemOp;
            switch (seed.getKind()) {
                case "source":
                    rheemOp = OperatorConvertor.createTextFileSourceOperator(seed.getUdf().getFunction().toByteArray(), seed.getUdf().getWrapper());
                    break;
                case "map":
                    rheemOp = OperatorConvertor.createMapOperator(seed.getUdf().getFunction().toByteArray(), seed.getUdf().getWrapper(), Object.class, Object.class);
                    break;
                case "filter":
                    rheemOp = OperatorConvertor.createFilterOperator(seed.getUdf().getFunction().toByteArray(), seed.getUdf().getWrapper(), Object.class);
                    break;
                case "sink":
                    rheemOp = OperatorConvertor.createTextFileSinkOperator(seed.getUdf().getFunction().toByteArray(), seed.getUdf().getWrapper(), Object.class);
                    sink = rheemOp;
                    break;
                default:
                    throw new RheemException("Operator " + seed.getKind() + " not supported by now");
            }

            dict_rheem.put(seed.getId(), rheemOp);


        }

        for (Map.Entry<Long, OperatorSeedRef> opref : dict_seed.entrySet()){
            OperatorSeedRef op = opref.getValue();
            if(op.getNumOutputs() > 0){
                for(Map.Entry<Long, Integer> suc : op.getOrdinalOutputs().entrySet()){
                    Long suc_id = suc.getKey();
                    Integer op_output_pos = suc.getValue();

                    //The successor has op.id as predecessor, fetch inputslot ordinal position
                    OperatorSeedRef suc_obj = dict_seed.get(suc_id);
                    Integer suc_input_pos = suc_obj.getOrdinalInput(opref.getKey());

                    OperatorBase rheem_op = dict_rheem.get(op.getId());
                    OperatorBase rheem_suc = dict_rheem.get(suc_id);

                    rheem_op.connectTo(op_output_pos, rheem_suc, suc_input_pos);
                }
            }
        }

        if(sink == null){
            throw new RheemException("Inconsistent Rheem Plan");
        }
        rheemContext.execute(new io.rheem.core.plan.rheemplan.RheemPlan(sink));

    }


    private static Map<Long, OperatorSeedRef> createExecutablePlanOLD(RheemPlan plan) {
        RheemContext rheemContext = new RheemContext().with(Java.basicPlugin());

        Map<Long, OperatorSeedRef> dict_seed = new HashMap<Long, OperatorSeedRef>();
        Map<Long, OperatorBase> dict_rheem = new HashMap<Long, OperatorBase>();
        List<OperatorSeedRef> sinks = new ArrayList<>();

        //Setting references to seeds
        for (OperatorSeed seed : plan.getOperatorsList()) {
            dict_seed.put(
                    seed.getId(),
                    new OperatorSeedRef(
                            seed.getId(),
                            seed.getKind(),
                            seed.getUdf().getFunction(),
                            seed.getUdf().getWrapper(),
                            seed.getInputSlotsList(),
                            seed.getOutputSlotsList()
                    )
            );

            if(seed.getOutputSlotsCount() == 0){
                sinks.add(dict_seed.get(seed.getId()));
            }
        }

        for (OperatorSeedRef sink: sinks) {
            OperatorSeedRef op = sink;

            while (op != null) {

                OperatorBase rheemOp;
                if(dict_rheem.containsKey(op.getId()))
                    rheemOp = dict_rheem.get(op.getId());
                else {
                    switch (op.getKind()) {
                        case "source":
                            rheemOp = OperatorConvertor.createTextFileSourceOperator(op.getUdf().toByteArray(), op.getWrapper());
                            break;
                        case "map":
                            rheemOp = OperatorConvertor.createMapOperator(op.getUdf().toByteArray(), op.getWrapper(), Object.class, Object.class);
                            break;
                        case "filter":
                            rheemOp = OperatorConvertor.createFilterOperator(op.getUdf().toByteArray(), op.getWrapper(), Object.class);
                            break;
                        case "sink":
                            rheemOp = OperatorConvertor.createTextFileSinkOperator(op.getUdf().toByteArray(), op.getWrapper(), Object.class);
                            break;
                        default:
                            throw new RheemException("Operator not supported by now");
                    }
                }

                dict_rheem.put(op.getId(), rheemOp);

                //Crear operador del tipo OP
                if(op.getNumOutputs() > 0) {
                    for (Map.Entry<Long, Integer> refToChild : op.getOrdinalOutputs().entrySet()) {
                        Long child_id = refToChild.getKey();
                        Integer op_output_slot = refToChild.getValue();

                        //TODO crear referencia en padre e hijo
                    }
                }
            }
        }

        return null;
    }


    public static void old_main(String[] args) throws Exception{
        System.out.println("java");
        if(args.length != 1){
            System.err.println("Mete la wea bien po ql");
            System.exit(-1);
        }

        RheemPlan plan = RheemPlan.parseFrom(new FileInputStream(args[0]));

        print(plan);

        Map<Long, OperatorSeedRef> dict_op = createReferences(plan);
    }

    // Iterates though all people in the AddressBook and prints info about them.
    static void print(RheemPlan plan) {
        for (OperatorSeed op: plan.getOperatorsList() ) {
            System.out.println("Obj ID: " + op.getId());
            System.out.println("  KIND: " + op.getKind());
            System.out.println("  UDF: " + op.getUdf().getFunction());
            System.out.println("  Wrap: " + op.getUdf().getWrapper());

            for (OperatorSeed.InputSlot in : op.getInputSlotsList()) {

                System.out.println("My owner is: " + in.getOwnerId());

                if(in.hasOccupant()){
                    System.out.println("My daddy is: " + in.getOccupant().getOccupantId());
                }
            }

            for (OperatorSeed.OutputSlot in : op.getOutputSlotsList()) {

                System.out.println("My owner is: " + in.getOwnerId());

                if(in.hasOccupant()){
                    System.out.println("My baby is: " + in.getOccupant().getOccupantId());
                }
            }
        }
    }

    static Map<Long, OperatorSeedRef> createReferences(RheemPlan rp){

        Map<Long, OperatorSeedRef> dict_op = new HashMap<Long, OperatorSeedRef>();

        for (OperatorSeed op: rp.getOperatorsList() ) {
            dict_op.put(
                    op.getId(),
                    new OperatorSeedRef(
                            op.getId(),
                            op.getKind(),
                            op.getUdf().getFunction(),
                            op.getUdf().getWrapper(),
                            op.getInputSlotsList(),
                            op.getOutputSlotsList()
                    )
            );

        }

        for (Map.Entry<Long, OperatorSeedRef> opref : dict_op.entrySet()){
            OperatorSeedRef op = opref.getValue();
            if(op.getNumOutputs() > 0){
                for(Map.Entry<Long, Integer> suc : op.getOrdinalOutputs().entrySet()){
                    Long suc_id = suc.getKey();
                    Integer op_output_pos = suc.getValue();

                    //The successor has op.id as predecessor, fetch inputslot ordinal position
                    OperatorSeedRef suc_obj = dict_op.get(suc_id);
                    Integer suc_input_pos = suc_obj.getOrdinalInput(opref.getKey());

                    op.setOutput(op_output_pos, new OutputSlot("to_" + suc_id.toString(), op));
                    suc_obj.setInput(suc_input_pos, new InputSlot("from_" + opref.getKey().toString(), suc_obj));
                    op.getOutput(op_output_pos).connectTo(suc_obj.getInput(suc_input_pos));
                }
            }
        }

        System.out.println(dict_op.toString());
        return dict_op;
    }

    public static void fakeExecution(OperatorSeedRef op){

        RheemContext rheemContext = new RheemContext().with(Java.basicPlugin());
        //rheemContext.execute(new RheemPlan(sink));
    }
}
