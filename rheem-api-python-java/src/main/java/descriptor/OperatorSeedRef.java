package descriptor;

import com.google.protobuf.ByteString;
import receiver.InputSlot;
import receiver.OutputSlot;
import org.apache.commons.lang3.Validate;

import java.util.*;

public class OperatorSeedRef {
    private int numInputSlots;
    private int numOutputSlots;
    private long id;
    private String kind;
    private ByteString udf;
    private String wrapper;
    protected InputSlot[] inputSlots;
    protected OutputSlot[] outputSlots;

    //Map that stores the position of each id/pos
    private Map<Long, Integer> ordinalInputs = new HashMap<>();
    private Map<Long, Integer> ordinalOutputs = new HashMap<>();
    //TODO fill platform from rheem context
    // private final Set<Platform> targetPlatforms = new HashSet<>(0);

    public OperatorSeedRef(long id, String kind,
                           ByteString udf, String wrapper,
                           List<Rheemintegration.OperatorSeed.InputSlot> is,
                           List<Rheemintegration.OperatorSeed.OutputSlot> os)
    {
        this.id = id;
        this.kind = kind;
        this.udf = udf;
        this.wrapper = wrapper;

        this.numInputSlots = is.size();
        this.numOutputSlots = os.size();

        this.inputSlots = new InputSlot[numInputSlots];
        this.outputSlots = new OutputSlot[numOutputSlots];

        System.out.println("op: " + this.id + " - " + this.kind);
        for(Rheemintegration.OperatorSeed.InputSlot op : is){
            ordinalInputs.put(op.getOccupant().getOccupantId(), is.indexOf(op));
            System.out.println("input: " + op.getOccupant().getOccupantId() + " - " + is.indexOf(op));
        }
        for(Rheemintegration.OperatorSeed.OutputSlot op : os){
            ordinalOutputs.put(op.getOccupant().getOccupantId(), os.indexOf(op));
            System.out.println("output: " + op.getOccupant().getOccupantId() + " - " + os.indexOf(op));
        }

    }

    public long getId() {
        return id;
    }

    public String getKind() {
        return kind;
    }

    public ByteString getUdf() {
        return udf;
    }

    public String getWrapper() {
        return wrapper;
    }

    public InputSlot[] getAllInputs() {
        return this.inputSlots;
    }

    public OutputSlot[] getAllOutputs() {
        return this.outputSlots;
    }

    public int getNumInputs() {
        return this.getAllInputs().length;
    }

    public int getNumOutputs() {
        return this.getAllOutputs().length;
    }

    /**
     * Sets the {@link InputSlot} of this instance. This method must only be invoked, when the input index is not
     * yet filled.
     *
     * @param index at which the {@link InputSlot} should be placed
     * @param input the new {@link InputSlot}
     */
    public void setInput(int index, InputSlot input) {
        assert index < this.getNumInputs() && this.getInput(index) == null;
        assert input.getOwner() == this;
        ((InputSlot[]) this.getAllInputs())[index] = input;
    }

    /**
     * Sets the {@link OutputSlot} of this instance. This method must only be invoked, when the output index is not
     * yet filled.
     *
     * @param index  at which the {@link OutputSlot} should be placed
     * @param output the new {@link OutputSlot}
     */
    public void setOutput(int index, OutputSlot output) {
        assert index < this.getNumOutputs() && this.getOutput(index) == null;
        assert output.getOwner() == this;
        ((OutputSlot[]) this.getAllOutputs())[index] = output;
    }

    /**
     * Retrieve an {@link InputSlot} of this instance using its index.
     *
     * @param index of the {@link InputSlot}
     * @return the requested {@link InputSlot}
     */
    public InputSlot getInput(int index) {
        final InputSlot[] allInputs = this.getAllInputs();
        Validate.inclusiveBetween(0, allInputs.length - 1, index, "Illegal input index %d for %s.", index, this);
        return allInputs[index];
    }

    /**
     * Retrieve an {@link OutputSlot} of this instance using its index.
     *
     * @param index of the {@link OutputSlot}
     * @return the requested {@link OutputSlot}
     */
    public OutputSlot getOutput(int index) {
        final OutputSlot[] allOutputs = this.getAllOutputs();
        if (index < 0 || index >= allOutputs.length) {
            throw new IllegalArgumentException(String.format("Illegal output index: %d.", index));
        }
        return allOutputs[index];
    }

    /**
     * Retrieve an {@link InputSlot} of this instance by its name.
     *
     * @param name of the {@link InputSlot}
     * @return the requested {@link InputSlot}
     */
    public InputSlot getInput(String name) {
        for (InputSlot inputSlot : this.getAllInputs()) {
            if (inputSlot.getName().equals(name)) return inputSlot;
        }
        throw new IllegalArgumentException(String.format("No slot with such name: %s", name));
    }

    public Map<Long, Integer> getOrdinalInputs() {
        return ordinalInputs;
    }

    public Integer getOrdinalInput(Long id) {
        return ordinalInputs.get(id);
    }

    public Map<Long, Integer> getOrdinalOutputs() {
        return ordinalOutputs;
    }

}
