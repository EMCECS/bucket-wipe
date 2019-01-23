/**
 * Copyright 2016-2019 Dell Inc. or its subsidiaries.  All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.emc.ecs.tool;

import org.apache.log4j.Logger;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class EnhancedThreadPoolExecutor extends ThreadPoolExecutor {
    private static final Logger log = Logger.getLogger(EnhancedThreadPoolExecutor.class);

    private BlockingDeque<Runnable> workDeque;
    private Semaphore threadsToKill = new Semaphore(0);
    private final Object pauseLock = new Object();
    private boolean paused = false;
    private final Object submitLock = new Object();
    private AtomicLong unfinishedTasks = new AtomicLong();
    private AtomicInteger activeTasks = new AtomicInteger();

    public EnhancedThreadPoolExecutor(int poolSize, BlockingDeque<Runnable> workDeque) {
        super(poolSize, poolSize, 0L, TimeUnit.MILLISECONDS, workDeque);
        this.workDeque = workDeque;
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        if (threadsToKill.tryAcquire()) {

            // we need a Deque here so the job order isn't disturbed
            workDeque.addFirst(r);

            t.setUncaughtExceptionHandler(new PoolShrinkingHandler(t.getUncaughtExceptionHandler()));

            // throwing an exception is the only way to immediately kill a thread in the pool. otherwise, the entire
            // work queue must be empty before the pool will start to shrink
            throw new PoolTooLargeException("killing thread to shrink pool");
        }

        // a new task started, so the queue should be smaller.
        synchronized (submitLock) {
            submitLock.notify();
        }

        synchronized (pauseLock) {
            while (paused) {
                try {
                    pauseLock.wait();
                } catch (InterruptedException e) {
                    log.warn("interrupted while paused", e);
                }
            }
        }

        activeTasks.incrementAndGet();

        super.beforeExecute(t, r);
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        activeTasks.decrementAndGet();
        unfinishedTasks.decrementAndGet();
        super.afterExecute(r, t);
    }

    public <T> Future<T> blockingSubmit(Callable<T> task) {
        FutureTask<T> futureTask = new FutureTask<>(task);
        blockingSubmit(futureTask);
        return futureTask;
    }

    /**
     * This will attempt to submit the task to the pool and, in the case where the queue is full, block until space is
     * available
     *
     * @throws IllegalStateException if the executor is shutting down or terminated
     */
    public Future blockingSubmit(Runnable task) {
        while (true) {
            if (this.isShutdown()) throw new IllegalStateException("executor is shut down");

            synchronized (submitLock) {
                try {
                    Future future = this.submit(task);
                    unfinishedTasks.incrementAndGet();
                    return future;
                } catch (RejectedExecutionException e) {
                    // ignore
                }
                if (this.isShutdown()) throw new IllegalStateException("executor is shut down");
                try {
                    submitLock.wait();
                } catch (InterruptedException e) {
                    log.warn("interrupted while waiting to submit a task", e);
                }
            }
        }
    }

    /**
     * This will set both the core and max pool size and kill any excess threads as their tasks complete.
     */
    public void resizeThreadPool(int newPoolSize) {

        // negate any last resize attempts
        threadsToKill.drainPermits();
        int diff = getActiveCount() - newPoolSize;
        super.setCorePoolSize(newPoolSize);
        super.setMaximumPoolSize(newPoolSize);
        if (diff > 0) threadsToKill.release(diff);
    }

    /**
     * If possible, pauses the executor so that active threads will complete their current task and then wait to execute
     * new tasks from the queue until unpaused.
     *
     * @return true if the state of the executor was changed from running to paused, false if already paused
     * @throws IllegalStateException if the executor is shutting down or terminated
     */
    public boolean pause() {
        synchronized (pauseLock) {
            if (isShutdown()) throw new IllegalStateException("executor is shut down");
            boolean wasPaused = paused;
            paused = true;
            return !wasPaused;
        }
    }

    /**
     * If possible, resumes the executor so that tasks will continue to be executed from the queue.
     *
     * @return true if the state of the executor was changed from paused to running, false if already running
     * @throws IllegalStateException if the executor is shutting down or terminated
     * @see #pause()
     */
    public boolean resume() {
        synchronized (pauseLock) {
            if (isShutdown()) throw new IllegalStateException("executor is shut down");
            boolean wasPaused = paused;
            paused = false;
            pauseLock.notifyAll();
            return wasPaused;
        }
    }

    @Override
    public int getActiveCount() {
        return activeTasks.get();
    }

    public long getUnfinishedTasks() {
        return unfinishedTasks.get();
    }

    static class PoolTooLargeException extends RuntimeException {
        public PoolTooLargeException(String message) {
            super(message);
        }
    }

    static class PoolShrinkingHandler implements Thread.UncaughtExceptionHandler {
        private Thread.UncaughtExceptionHandler handler;

        public PoolShrinkingHandler(Thread.UncaughtExceptionHandler handler) {
            this.handler = handler;
        }

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            if (!(e instanceof PoolTooLargeException)) {
                if (handler != null) handler.uncaughtException(t, e);
            }
        }
    }
}
