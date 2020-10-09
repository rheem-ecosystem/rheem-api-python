package receiver;

import descriptor.OperatorSeedRef;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class OutputSlot<T> extends Slot<T> {

    private final List<InputSlot<T>> occupiedSlots = new LinkedList<>();

    public OutputSlot(String name, OperatorSeedRef owner) {
        super(name, owner);
    }

    /**
     * Connect this output slot to an input slot. The input slot must not be occupied already.
     *
     * @param inputSlot the input slot to connect to
     */
    public void connectTo(InputSlot<T> inputSlot) {
        if (inputSlot.getOccupant() != null) {
            throw new IllegalStateException("Cannot connect: input slot is already occupied");
        }

        this.occupiedSlots.add(inputSlot);
        inputSlot.setOccupant(this);
    }

    public void disconnectFrom(InputSlot<T> inputSlot) {
        if (inputSlot.getOccupant() != this) {
            throw new IllegalStateException("Cannot disconnect: input slot is not occupied by this output slot");
        }

        this.occupiedSlots.remove(inputSlot);
        inputSlot.setOccupant(null);
        //inputSlot.notifyDetached();  //TODO what was this doing
    }

    public List<InputSlot<T>> getOccupiedSlots() {
        return this.occupiedSlots;
    }

    /**
     * Take the output connections away from one operator and give them to another one.
     */

    public static void stealConnections(OperatorSeedRef victim, OperatorSeedRef thief) {
        if (victim.getNumOutputs() != thief.getNumOutputs()) {
            throw new IllegalArgumentException("Cannot steal outputs: Mismatching number of outputs.");
        }

        for (int i = 0; i < victim.getNumOutputs(); i++) {
            thief.getOutput(i).unchecked().stealOccupiedSlots(victim.getOutput(i).unchecked());
        }
    }

    public OutputSlot<Object> unchecked() {
        return (OutputSlot<Object>) this;
    }

    /**
     * Takes away the occupied {@link InputSlot}s of the {@code victim} and connects it to this instance.
     */
    public void stealOccupiedSlots(OutputSlot<T> victim) {
        final List<InputSlot<T>> occupiedSlots = new ArrayList<>(victim.getOccupiedSlots());
        for (InputSlot<T> occupiedSlot : occupiedSlots) {
            victim.disconnectFrom(occupiedSlot);
            this.connectTo(occupiedSlot);
        }
    }
}
