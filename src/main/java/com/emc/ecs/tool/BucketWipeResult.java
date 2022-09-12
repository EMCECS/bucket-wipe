/*
 * Copyright (c) 2016-2021 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.ecs.tool;

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
    private final AtomicLong deletedObjects = new AtomicLong();
    private final AtomicBoolean allActionsSubmitted = new AtomicBoolean();
    private final CompletableFuture<Boolean> completedFuture = new CompletableFuture<>();
    private final List<String> errors = Collections.synchronizedList(new ArrayList<>());
    private String lastKey;

    public CompletableFuture<Boolean> getCompletedFuture() {
        return completedFuture;
    }

    /**
     * GuardedBy "this"
     */
    public String getLastKey() {
        synchronized (this) {
            return lastKey;
        }
    }

    public long getDeletedObjects() {
        return deletedObjects.get();
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
        deletedObjects.incrementAndGet();
        actionsOutstanding.decrementAndGet();

        completeFutureIfComplete();
    }

    public void allActionsSubmitted() {
        allActionsSubmitted.set(true);

        completeFutureIfComplete();
    }


    /**
     * GuardedBy "this"
     */
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
