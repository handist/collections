package handist.collections;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

public class FutureN<S>  {
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

    public void get0() throws InterruptedException, ExecutionException {
        synchronized (this) {
            if (isDone) return;
        }
        for (Future<?> f : futures)
            f.get();
        return;
    }

    public void get0(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
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

    public static class ReturnGivenResult<R> extends FutureN implements Future<R> {
    	R result;
		public ReturnGivenResult(List<Future<?>> futures, R result) {
			super(futures);
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
    public static class OnlyWait extends ReturnGivenResult<Void> {
		public OnlyWait(List<Future<?>> futures) {
			super(futures, null);
		}
    }
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
