/*******************************************************************************
 * Copyright (c) 2020 Handy Tools for Distributed Computing (HanDist) project.
 *
 * This program and the accompanying materials are made available to you under 
 * the terms of the Eclipse Public License 1.0 which accompanies this 
 * distribution, and is available at https://www.eclipse.org/legal/epl-v10.html
 *
 * SPDX-License-Identifier: EPL-1.0
 *******************************************************************************/
package handist.collections;

import java.io.PrintStream;
import java.io.PrintWriter;

public class ParallelExecutionException extends RuntimeException {
	/** Serial Version UID */
	private static final long serialVersionUID = 3091764613344405477L;
	public final Throwable cause;
	public final String msg;
	public ParallelExecutionException() {
		this(null,null);
	}
	public ParallelExecutionException(String msg) {
		this(msg,null);
	}
	public ParallelExecutionException(String msg, Throwable cause) {
		this.msg = msg;
		this.cause = cause;
	}
	public ParallelExecutionException(Throwable cause) {
		this(null,cause);
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
	@Override 
	public String toString() {
		return "[ParallelExecutionException,  msg: " + msg + ", cause: " + cause + ".";
	}
}
