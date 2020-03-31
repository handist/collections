package handist.util;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class FutureN<S, R> implements Future<R> {
    private final R result;
    private final List<Future<?>> futures;
    boolean isCanceled = false;
    boolean isDone = false;

    public FutureN(List<Future<?>> futures, R result) {
        this.result = result;
        this.futures = futures;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        boolean result = true;
        for (Future<?> f : futures)
            result &= f.cancel(mayInterruptIfRunning);
        synchronized (this) {
            isCanceled = true;
        }
        return result;
    }

    @Override
    public boolean isCancelled() {
        synchronized (this) {
            return isCanceled;
        }
    }

    @Override
    public boolean isDone() {
        synchronized (this) {
            if (isDone)
                return isDone;
        }
        for (Future<?> f : futures) {
            if (!f.isDone())
                return false;
        }
        synchronized (this) {
            isDone = true;
        }
        return true;
    }

    @Override
    public R get() throws InterruptedException, ExecutionException {
        synchronized (this) {
            if (isDone)
                return result;
        }
        for (Future<?> f : futures)
            f.get();
        return result;
    }

    @Override
    public R get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        long start = System.currentTimeMillis();
        synchronized (this) {
            if (isDone)
                return result;
        }
        for (Future<?> f : futures) {
            long now = System.currentTimeMillis();
            f.get(timeout - unit.convert(now - start, TimeUnit.MILLISECONDS), unit);
        }
        return result;
    }
}