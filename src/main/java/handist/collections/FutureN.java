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
	protected final List<Future<S>> futures;
	boolean isCanceled = false;
	boolean isDone = false;

	public FutureN(List<Future<S>> futures) {
		this.futures = futures;
	}

	public boolean cancel(boolean mayInterruptIfRunning) {
		boolean result = true;
		for (Future<?> f : futures)
			result &= f.cancel(mayInterruptIfRunning);
		return result;
	}

	public boolean isCancelled() {
		boolean result = true;
		for (Future<S> f : futures)
			result &= f.isCancelled();
		return result;
	}

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

	protected void get0() throws InterruptedException, ExecutionException {
		synchronized (this) {
			if (isDone) return;
		}
		for (Future<?> f : futures)
			f.get();
		return;
	}

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
	 * {@link ReturnGivenResult} receives a list
	 * of futures and an instance of the generic type R. This instance is 
	 * returned in method {@link #get()} when all the futures have completed.
	 * <p>
	 * TODO explain a typical use-case.
	 * 
	 * @param <R> the type of the result type (This class implements <code>Future&lt;R&gt;</code>)
	 */
	@SuppressWarnings("rawtypes")
	public static class ReturnGivenResult<R> extends FutureN implements Future<R> {
		R result;

		@SuppressWarnings("unchecked")
		public ReturnGivenResult(List<Future<?>> futures, R r) {
			super(futures);
			result = r;
		}

		@Override
		public R get() throws InterruptedException, ExecutionException {
			get0();
			return result;
		}

		@Override
		public R get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
			get0(timeout, unit);
			return result;
		}
	}
	/**
	 * {@link OnlyWait} waits for the termination of all its given futures.
	 */
	public static class OnlyWait extends ReturnGivenResult<Void> {
		public OnlyWait(List<Future<?>> futures) {
			super(futures, null);
		}
	}

	/**
	 * {@link ListResults} receives a <code>List&lt;Future&lt;R&gt;&gt;</code> and 
	 * returns a <code>List&lt;R&gt;</code> containing the result of each of the
	 * given futures.
	 * 
	 * @param <R> type of the result of each parallel future. 
	 */
	public static class ListResults<R> extends FutureN<R> implements Future<List<R>> {
		public ListResults(List<Future<R>> futures) {
			super(futures);
		}
		@Override
		public List<R> get() throws InterruptedException, ExecutionException {
			get0();
			return processResult();
		}
		@Override
		public List<R> get(long timeout, TimeUnit unit)
				throws InterruptedException, ExecutionException, TimeoutException {
			get0(timeout, unit);
			return processResult();
		}
		List<R> result = null;
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
	 * {@link ConsumeResults} receives a <code>List&lt;Future&lt;R&gt;&gt;</code>
	 * and a <code>Consumer&lt;R&gt;</code>. It waits for the termination of 
	 * the given futures before giving the individual results of type <code>R</code>
	 * to the consumer.
	 *
	 * @param <R> type of the result produced by the individual futures
	 */
	public static class ConsumeResults<R> extends FutureN<R> implements Future<Void> {

		private Consumer<R> consumer;
		public ConsumeResults(List<Future<R>> futures, Consumer<R> consumer) {
			super(futures);
			this.consumer = consumer;
		}
		@Override
		public Void get() throws InterruptedException, ExecutionException {
			get0();
			processResult();
			return null;
		}
		@Override
		public Void get(long timeout, TimeUnit unit)
				throws InterruptedException, ExecutionException, TimeoutException {
			get0(timeout, unit);
			processResult();
			return null;
		}
		boolean processed = false;
		void processResult() {
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
}
