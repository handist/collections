package handist.collections.reducer;

import java.io.Serializable;

import mpi.MPI;

public class BoolReducer implements Serializable {

    /**
     * The reduction operations provided by default, <em>AND</em>, <em>OR</em>,
     */
    public static enum Op {
        AND, OR
    }

    private static final long serialVersionUID = -648909085372911699L;

    public static mpi.Op getMPIOp(Op op) {
        switch (op) {
        case AND:
            return MPI.LAND;
        case OR:
            return MPI.LOR;
        }
        throw new IllegalArgumentException();
    }

    private boolean value;
    private final Op op;

    public BoolReducer(Op op) {
        this.op = op;
        switch (op) {
        case AND:
            value = true;
            break;
        case OR:
            value = false;
            break;
        }
    }

    public boolean reduce(boolean input) {
        if (op == Op.AND && !input) {
            value = false;
            return true;
        } else if (op == Op.OR && input) {
            value = true;
            return true;
        }
        return false;
    }

    public boolean value() {
        return value;
    }
}
