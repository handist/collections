package handist.collections;

import java.io.PrintStream;
import java.io.PrintWriter;

public class ParallelExecutionException extends RuntimeException {
    public final Throwable cause;
    public final String msg;
    public ParallelExecutionException(String msg, Throwable cause) {
        this.msg = msg;
        this.cause = cause;
    }
    public ParallelExecutionException(String msg) {
        this(msg,null);
    }
    public ParallelExecutionException(Throwable cause) {
        this(null,cause);
    }
    public ParallelExecutionException() {
        this(null,null);
    }
    
    @Override 
    public String toString() {
        return "[ParallelExecutionException,  msg: " + msg + ", cause: " + cause + ".";
    }
    
    @Override
    public void printStackTrace(PrintStream out) {
        out.print("[ParallelExecutionException] " );
        if(msg!=null) out.println(msg);
        out.println();
        // TODO message for cause
        super.printStackTrace(out);
    }
    @Override
    public void printStackTrace(PrintWriter out) {
        out.print("[ParallelExecutionException] " );
        if(msg!=null) out.println(msg);
        out.println();
        // TODO message for cause        
        super.printStackTrace(out);
    }
}
