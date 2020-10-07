package receiver;

import descriptor.OperatorSeedRef;

abstract public class Slot<T> {

    /**
     * Identifies this slot within its operator.
     */
    private final String name;

    /**
     * The operator that is being decorated by this slot.
     */
    private final OperatorSeedRef owner;


    protected Slot(String name, OperatorSeedRef owner) {
        assert owner != null;
        this.name = name;
        this.owner = owner;
    }

    public String getName() {
        return this.name;
    }

    public OperatorSeedRef getOwner() {
        return this.owner;
    }

    /**
     * @return whether this is an {@link OutputSlot}
     */
    public boolean isOutputSlot() {
        return this instanceof OutputSlot;
    }

    /**
     * @return whether this is an input slot
     */
    public boolean isInputSlot() {
        return this instanceof InputSlot;
    }

    public boolean isCompatibleWith(Slot<?> that) {
        return true;
    }

    @Override
    public String toString() {
        return String.format("%s@%s", this.name, this.owner == null ? "no owner" : this.owner.toString());
    }

}
