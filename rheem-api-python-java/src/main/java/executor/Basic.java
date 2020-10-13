package executor;

import descriptor.Rheemintegration;
import io.rheem.core.api.exception.RheemException;

import java.io.FileInputStream;
import java.io.IOException;

public class Basic {

    public static void main(String[] args) throws IOException {
        Rheemintegration.RheemPlan plan = Rheemintegration.RheemPlan.parseFrom(new FileInputStream(args[0]));

        print(plan);

        byte[] targetUdf = null;
        for (Rheemintegration.OperatorSeed seed : plan.getOperatorsList()) {
            if(seed.getKind().equals("map")){
                targetUdf = seed.getUdf().getFunction().toByteArray();
                break;
            }
        }

        if (targetUdf == null){
            throw new RheemException("Nothing to do, do you think i have your time?");
        }

        System.out.println("targetUdf");
        System.out.println(targetUdf);

        PyRheemExecutorTest.execute(targetUdf);

    }

    // Iterates though all people in the AddressBook and prints info about them.
    static void print(Rheemintegration.RheemPlan plan) {
        for (Rheemintegration.OperatorSeed op: plan.getOperatorsList() ) {
            System.out.println("Obj ID: " + op.getId());
            System.out.println("  KIND: " + op.getKind());
            System.out.println("  UDF: " + op.getUdf().getFunction());
            System.out.println("  Wrap: " + op.getUdf().getWrapper());

            for (Rheemintegration.OperatorSeed.InputSlot in : op.getInputSlotsList()) {

                System.out.println("My owner is: " + in.getOwnerId());

                if(in.hasOccupant()){
                    System.out.println("My daddy is: " + in.getOccupant().getOccupantId());
                }
            }

            for (Rheemintegration.OperatorSeed.OutputSlot in : op.getOutputSlotsList()) {

                System.out.println("My owner is: " + in.getOwnerId());

                if(in.hasOccupant()){
                    System.out.println("My baby is: " + in.getOccupant().getOccupantId());
                }
            }
        }
    }
}
