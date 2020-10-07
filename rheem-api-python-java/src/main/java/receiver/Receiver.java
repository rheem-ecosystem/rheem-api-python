package receiver;
import descriptor.OperatorSeedRef;
import descriptor.Rheemintegration.RheemPlan;
import descriptor.Rheemintegration.OperatorSeed;

import java.io.FileInputStream;
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

        createReferences(plan);
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

    static void createReferences(RheemPlan rp){

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
    }
}
