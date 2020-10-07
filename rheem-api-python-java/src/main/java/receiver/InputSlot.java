package receiver;

import descriptor.OperatorSeedRef;

import java.util.List;

public class InputSlot<T> extends Slot<T> {

    /**
     * Output slot of another operator that is connected to this input slot.
     */
    private OutputSlot<T> occupant;

    /**
     * Creates a new instance.
     */
    public InputSlot(String name, OperatorSeedRef owner) {
        super(name, owner);
    }

    InputSlot<T> setOccupant(OutputSlot<T> outputSlot) {
        this.occupant = outputSlot;
        return this;
    }

    public OutputSlot<T> getOccupant() {
        return this.occupant;
    }
}
