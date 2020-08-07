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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * {@link FutureN} receives a list of {@link Future} and waits for the 
 * termination of all the futures. 
 * There are four variations of futureN.
 * <ul>
 * 	<li>When a list of <code>Future&lt;?&gt;</code> is given
 *     <ul> 
 *     	<li>{@link FutureN.OnlyWait} waits for the termination of the given futures.
 *       <li>{@link FutureN.ReturnGivenResult} receives the result data structure `R result` and returns the result
 *                 when all the futures are finished.
 *     </ul>
 *   <li> When a list of <code>Future&lt;R&gt;</code> is given
 *     <ul>
 *       <li>{@link FutureN.ListResults} receives a <code>List&lt;Future&lt;R&gt;&gt;</code> and returns a <code>List&lt;R&gt;</code>. 
 *       <li>{@link FutureN.ConsumeResults} receives a <code>List&lt;Future&lt;R&gt;&gt;</code> and a <code>Consumer&lt;R&gt;</code>. 
 *       	It waits for the termination of the given futures and processes the result using the provided consumer.
 *     </ul>
 *  </ul>
 *  
 *  @param <S> generic type of the group of {@link Future} this class will 
 *   handle
 */
public abstract class FutureN<S>  {

	/**
	 * {@link ConsumeResults} receives a <code>List&lt;Future&lt;R&gt;&gt;</code>
	 * and a <code>Consumer&lt;R&gt;</code>. It waits for the termination of 
	 * the given futures before transmitting the individual results of type <code>R</code>
	 * to the consumer.
	 *
	 * @param <R> type of the result produced by the individual futures
	 */
	public static class ConsumeResults<R> extends FutureN<R> implements Future<Void> {

		/** Consumer provided during construction */
		private Consumer<R> consumer;
		/** Flag used to indicate if {@link #processResult()} was previously called */ 
		private boolean processed = false;

		/**
		 * Constructor taking a list of futures and a Consumer as parameter
		 * @param futures the list of futures this instance will wait on
		 * @param consumer the consumer which will receive the results produced 
		 * by every future
		 */
		public ConsumeResults(List<Future<R>> futures, Consumer<R> consumer) {
			super(futures);
			this.consumer = consumer;
		}

		/**
		 * Waits for every future to terminate and gives their individual results to the
		 * consumer provided during construction
		 * @return nothing
		 * @throws InterruptedException if such an exception is thrown by one of the futures
		 * @throws ExecutionException if such an exception is thrown by one of the futures 
		 */
		@Override
		public Void get() throws InterruptedException, ExecutionException {
			get0();
			processResult();
			return null;
		}

		/**
		 * Waits for every future to terminate and gives their individual results to the
		 * consumer provided during construction
		 * @return nothing
		 * @throws InterruptedException if such an exception is thrown by one of the futures
		 * @throws ExecutionException if such an exception is thrown by one of the futures 
		 * @throws TimeoutException if one of the futures handled by this class exhausts the timeout
		 */
		@Override
		public Void get(long timeout, TimeUnit unit)
				throws InterruptedException, ExecutionException, TimeoutException {
			get0(timeout, unit);
			processResult();
			return null;
		}

		/**
		 * Helper method which takes the results produced by each future and gives them to
		 * the consumer
		 */
		private void processResult() {
			synchronized(this) {
				if(processed) return;
				for(Future<R> f: futures) {
					try {
						consumer.accept(f.get());
					} catch (InterruptedException|ExecutionException e) {
						throw new Error("This should not occur!");
					}
				}
				processed = true;
			}
		}
	}

	/**
	 * {@link ListResults} receives a <code>List&lt;Future&lt;R&gt;&gt;</code> and 
	 * returns a <code>List&lt;R&gt;</code> containing the result of each of the
	 * individual futures.
	 * 
	 * @param <R> type of the result of each future
	 */
	public static class ListResults<R> extends FutureN<R> implements Future<List<R>> {
		/** List which is going to contain the result of each future */
		List<R> result = null;

		/**
		 * Constructor with a list of futures.
		 * @param futures list of futures to wait on
		 */
		public ListResults(List<Future<R>> futures) {
			super(futures);
		}

		/**
		 * Waits for all futures to terminate and return the individual result of each future
		 * in a list.
		 * @return the result produced by each future, in a list 
		 * @throws InterruptedException if such an exception is thrown by one of the futures
		 * @throws ExecutionException if such an exception is thrown by one of the futures 
		 */
		@Override
		public List<R> get() throws InterruptedException, ExecutionException {
			get0();
			return processResult();
		}

		/**
		 * Waits for all futures to terminate within the provided timeout and return the 
		 * individual result of each future in a list.
		 * @param timeout the total time allowed for all the futures to complete
		 * @param unit time unit of the timeout
		 * @return the result produced by each future, in a list
		 * @throws InterruptedException if such an exception is thrown by one of the futures
		 * @throws ExecutionException if such an exception is thrown by one of the futures 
		 * @throws TimeoutException if one of the futures handled by this class exhausts the timeout
		 */
		@Override
		public List<R> get(long timeout, TimeUnit unit)
				throws InterruptedException, ExecutionException, TimeoutException {
			get0(timeout, unit);
			return processResult();
		}

		/**
		 * Helper method which initializes {@link #result}
		 * @return the freshly initialized {@link #result}
		 */
		List<R> processResult() {
			synchronized(this) {
				if(result!=null) return result;
				result = new ArrayList<>();
				for(Future<R> f: futures) {
					try {
						result.add(f.get());
					} catch (InterruptedException|ExecutionException e) {
						throw new Error("This should not occur!");
					}
				}
			}
			return result;
		}
	}
	/**
	 * {@link OnlyWait} waits for the termination of all its given futures.
	 */
	public static class OnlyWait extends ReturnGivenResult<Void> {
		/**
		 * Constructor with a list of futures.
		 * @param futures futures on which to wait for completion
		 */
		public OnlyWait(List<Future<?>> futures) {
			super(futures, null);
		}
	}

	/**
	 * {@link ReturnGivenResult} receives a list
	 * of futures and an instance of the generic type R. This instance is 
	 * returned in method {@link #get()} after all the futures have completed.
	 * <p>
	 * This nested class is typically used when the futures operate on the elements contained in
	 * an object and the "result"  of this operation is actually the same object with its contents
	 * potentially transformed. Presumably, the object can be safely manipulated again once all 
	 * the futures have completed.
	 * 
	 * @param <R> the type of the result type (This class implements <code>Future&lt;R&gt;</code>)
	 */
	@SuppressWarnings("rawtypes")
	public static class ReturnGivenResult<R> extends FutureN implements Future<R> {
		/** Object to return when all the futures have completed */
		R result;

		/**
		 * Constructor which takes a list of futures and an object 
		 * @param futures the list of futures whose completion will be waited on
		 * @param r object to return when all the futures have completed
		 */
		@SuppressWarnings("unchecked")
		public ReturnGivenResult(List<Future<?>> futures, R r) {
			super(futures);
			result = r;
		}

		/**
		 * Waits for the termination of all the futures and returns the object with which this {@link FutureN}
		 * was constructed.
		 */
		@Override
		public R get() throws InterruptedException, ExecutionException {
			get0();
			return result;
		}

		/**
		 * Waits for the termination of all the futures within the specified timeout and returns the object this
		 * instance was initialized with. 
		 * @param timeout total timeout allowed for all the futures to finish
		 * @param unit time unit of the timeout
		 * @return the object this class was constructed with
		 * @throws InterruptedException if such an exception is thrown by one of the futures
		 * @throws ExecutionException if such an exception is thrown by one of the futures 
		 * @throws TimeoutException if one of the futures handled by this class exhausts the timeout
		 */
		@Override
		public R get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
			get0(timeout, unit);
			return result;
		}
	}

	/** List of futures handled by this class */
	protected final List<Future<S>> futures;

	boolean isCanceled = false;

	boolean isDone = false;

	/**
	 * Constructor taking a list of futures as argument. 
	 * @param futures the futures which this instance aggreagates
	 */
	public FutureN(List<Future<S>> futures) {
		this.futures = futures;
	}

	/**
	 * Attempts to cancel execution of all the contained tasks. This attempt will fail if one of 
	 * the task has already completed, has already been cancelled, or could not be cancelled for
	 * some other reason. If successful, and this task has not started when cancel is called, 
	 * none of the contained tasks should never run. If the task has already started, then the 
	 * mayInterruptIfRunning parameter determines whether the thread executing this task should 
	 * be interrupted in an attempt to stop the task. After this method returns, subsequent 
	 * calls to isDone() will always return true. Subsequent calls to isCancelled() will always 
	 * return true if this method returned true.
	 * 
	 * @param mayInterruptIfRunning true if the thread executing the tasks should be interrupted; 
	 * otherwise, in-progress tasks are allowed to complete
	 * @return false if at least one of the tasks could not be cancelled (usually because it has
	 * already completed normally, true otherwise
	 */
	public boolean cancel(boolean mayInterruptIfRunning) {
		boolean result = true;
		for (Future<?> f : futures)
			result &= f.cancel(mayInterruptIfRunning);
		return result;
	}

	/** 
	 * Helper method for child classes, calls method {@link Future#get()} on each future handled
	 * @throws InterruptedException if such an exception is thrown by one of the futures
	 * @throws ExecutionException if such an exception is thrown by one of the futures  
	 */ 
	protected void get0() throws InterruptedException, ExecutionException {
		synchronized (this) {
			if (isDone) return;
		}
		for (Future<?> f : futures)
			f.get();
		return;
	}
	/** 
	 * Helper method for child classes, calls method {@link Future#get(long,TimeUnit)} on each future 
	 * handled by this class
	 * @param timeout total timeout allowed for all the futures
	 * @param unit unit of the timeout
	 * @throws InterruptedException if such an exception is thrown by one of the futures
	 * @throws ExecutionException if such an exception is thrown by one of the futures 
	 * @throws TimeoutException if one of the futures handled by this class exhausts the timeout
	 */ 
	protected void get0(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		long start = System.currentTimeMillis();
		synchronized (this) {
			if (isDone)
				return;
		}
		for (Future<?> f : futures) {
			long now = System.currentTimeMillis();
			f.get(timeout - unit.convert(now - start, TimeUnit.MILLISECONDS), unit);
		}
		return;
	}

	/**
	 * Indicates if all the tasks this instnace contains were cancelled before they could complete normally
	 * @return true if all the tasks were successfully canceled before they could complete
	 */
	public boolean isCancelled() {
		boolean result = true;
		for (Future<S> f : futures)
			result &= f.isCancelled();
		return result;
	}

	/**
	 * Returns true is all the individual tasks this instance contains completed. 
	 * @return true if all the tasks completed
	 */
	public boolean isDone() {
		synchronized (this) {
			if (isDone)
				return isDone;
		}
		for (Future<S> f : futures) {
			if (!f.isDone())
				return false;
		}
		synchronized (this) {
			isDone = true;
		}
		return true;
	}
}
