package handist.collections.reducer;

import java.io.Serializable;
import java.util.function.Consumer;

import mpi.MPI;

public class IntReducer implements Serializable {

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

    private int value;
    private final Consumer<Integer> reduceFunc;

    public IntReducer(Op op) {
        switch (op) {
        case SUM:
            value = 0;
            reduceFunc = (input) -> value += input;
            break;
        case PROD:
            value = 1;
            reduceFunc = (input) -> value *= input;
            break;
        case MAX:
            value = Integer.MIN_VALUE;
            reduceFunc = (input) -> {
                if (input > value) {
                    value = input;
                }
            };
            break;
        case MIN:
            value = Integer.MAX_VALUE;
            reduceFunc = (input) -> {
                if (input < value) {
                    value = input;
                }
            };
            break;
        default:
            throw new UnsupportedOperationException("IntReducer does not support the operation :" + op);
        }
    }

    public void reduce(int input) {
        reduceFunc.accept(input);
    }

    public int value() {
        return value;
    }
}
