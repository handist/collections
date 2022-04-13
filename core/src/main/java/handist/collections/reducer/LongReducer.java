package handist.collections.reducer;

import java.io.Serializable;
import java.util.function.Consumer;

import mpi.MPI;

public class LongReducer implements Serializable {

    /**
     * The reduction operations provided by default, <em>SUM</em>, <em>PROD</em>,
     * <em>MAX</em>, <em>MIN</em>
     */
    public static enum Op {
        SUM, PROD, MAX, MIN
    }

    private static final long serialVersionUID = -648909085372911699L;

    public static mpi.Op getMPIOp(Op op) {
        switch (op) {
        case SUM:
            return MPI.SUM;
        case PROD:
            return MPI.PROD;
        case MAX:
            return MPI.MAX;
        case MIN:
            return MPI.MIN;
        }
        throw new IllegalArgumentException();
    }

    private long value;
    private final Consumer<Long> reduceFunc;

    public LongReducer(Op op) {
        switch (op) {
        case SUM:
            value = 0l;
            reduceFunc = (input) -> value += input;
            break;
        case PROD:
            value = 1l;
            reduceFunc = (input) -> value *= input;
            break;
        case MAX:
            value = Long.MIN_VALUE;
            reduceFunc = (input) -> {
                if (input > value) {
                    value = input;
                }
            };
            break;
        case MIN:
            value = Long.MAX_VALUE;
            reduceFunc = (input) -> {
                if (input < value) {
                    value = input;
                }
            };
            break;
        default:
            throw new UnsupportedOperationException("LongReducer does not support the operation :" + op);
        }
    }

    public void reduce(long input) {
        reduceFunc.accept(input);
    }

    public long value() {
        return value;
    }
}
