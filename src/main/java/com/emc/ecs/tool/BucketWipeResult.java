package com.emc.ecs.tool;

import org.apache.http.annotation.GuardedBy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An Async Result of a BucketWipe Operation
 *
 * Clients can use {@link #getCompletedFuture()} which will be completed when the bucket wipe has completed
 */
public class BucketWipeResult {
    private final AtomicLong actionsOutstanding = new AtomicLong();
    private final AtomicLong numberObjectsDeleted = new AtomicLong();
    private final AtomicBoolean allActionsSubmitted = new AtomicBoolean();
    private final CompletableFuture<Boolean> completedFuture = new CompletableFuture<>();
    private final List<String> errors = Collections.synchronizedList(new ArrayList<>());

    @GuardedBy("this")
    private String lastKey;

    public CompletableFuture<Boolean> getCompletedFuture() {
        return completedFuture;
    }

    public String getLastKey() {
        synchronized (this) {
            return lastKey;
        }
    }

    public long getObjectsDeleted() {
        return numberObjectsDeleted.get();
    }


    public void addError(String error) {
        errors.add(error);
    }

    public List<String> getErrors() {
        return errors;
    }

    public void actionOutstanding() {
        actionsOutstanding.incrementAndGet();
    }

    public void actionComplete() {
        numberObjectsDeleted.incrementAndGet();
        actionsOutstanding.decrementAndGet();

        completeFutureIfComplete();
    }

    public void allActionsSubmitted() {
        allActionsSubmitted.set(true);

        completeFutureIfComplete();
    }


    protected void setLastKey(String lastKey) {
        synchronized (this) {
            this.lastKey = lastKey;
        }
    }

    /**
     * The operation is complete when all actions have been submitted and there are no outstanding issues
     */
    private void completeFutureIfComplete() {
        if (allActionsSubmitted.get() && actionsOutstanding.get() == 0) {
            completedFuture.complete(errors.isEmpty());
        }
    }
}
