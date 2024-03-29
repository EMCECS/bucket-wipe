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

import com.emc.object.s3.S3Client;
import com.emc.object.s3.bean.*;
import com.emc.object.s3.request.AbortMultipartUploadRequest;
import com.emc.object.s3.request.ListMultipartUploadsRequest;
import com.emc.object.s3.request.ListObjectsRequest;
import com.emc.object.s3.request.ListVersionsRequest;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

/**
 * Provides multiple Asynchronous operations for deleting objects from buckets
 * <p>
 * An instance of the class can be used multiple times for different buckets as long as the S3Client configuration remains the same.
 * <p>
 * Operations are async, users should pass in an instance of {@link BucketWipeResult} into which the result of the bucket
 * wipe operation (number of objects deleted etc) will be placed.  The operation is complete when the {@link BucketWipeResult#getCompletedFuture()}
 * completes.
 * <p>
 * MaxConcurrent controls how many concurrent delete actions can be submitted at once, this allows an instance to handle deletion of millions of
 * objects from a bucket without causing a runaway OOM condition.  Note that the maxConcurrent applies to ALL actions within an instance, for example
 * if there are two callers they will both be competing to add new delete actions within the maxConcurrent limit.
 */
public class BucketWipeOperations {
    public static final int DEFAULT_THREADS = 32;
    public static final int DEFAULT_MAX_CONCURRENT = 2000;

    private final S3Client client;
    private final ExecutorService executor;
    private final Semaphore submissionSemaphore;

    /**
     * Default constructor that uses the {@link #DEFAULT_THREADS} number of threads and {@link #DEFAULT_MAX_CONCURRENT} max concurrency
     *
     * @param client The pre-configured S3 client to use when contacting the S3 server
     */
    public BucketWipeOperations(S3Client client) {
        this(client, DEFAULT_THREADS, DEFAULT_MAX_CONCURRENT);
    }

    /**
     * Create an instance of the {@link BucketWipeOperations} class.  Instances are thread safe and can be used by multiple callers simultaneously
     *
     * @param client        The pre-configured S3 client to use when contacting the S3 server
     * @param numThreads    Number of threads to use in the thread pool for  simultaneous actions
     * @param maxConcurrent Total number of actions to be queued up at once.  New delete actions will be blocked until there is "space" in the queue
     */
    public BucketWipeOperations(S3Client client, int numThreads, int maxConcurrent) {
        this.client = client;
        this.executor = Executors.newFixedThreadPool(numThreads);
        this.submissionSemaphore = new Semaphore(maxConcurrent);
    }

    public void shutdown() {
        executor.shutdown();
    }

    /**
     * Deletes all objects from the specified bucket that have keys specified in the sourceListFile
     *
     * @param bucket         the target S3 Bucket
     * @param sourceListFile filepath of the containing the object keys.  Each line in the file represents an object key
     * @param result         asynchronous result of the operation
     * @throws InterruptedException if interrupted waiting for submission semaphore
     */
    public void deleteAllObjectsWithList(String bucket, String sourceListFile, BucketWipeResult result) throws InterruptedException {
        try {
            try (BufferedReader reader = new BufferedReader(new FileReader(sourceListFile))) {
                String key = reader.readLine();
                while (key != null) {
                    submitTask(new DeleteObjectTask(client, bucket, key), result);
                    key = reader.readLine();
                }
            } finally {
                result.allActionsSubmitted();
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException("File not found", e);
        } catch (IOException e) {
            throw new RuntimeException("Error reading key list line", e);
        }
    }

    /**
     * Deletes all Keys from the specified bucket that start with the given prefix
     *
     * @param bucket the target bucket
     * @param prefix key prefix of objects to be deleted
     * @param result the asynchronous result of the operation
     * @throws InterruptedException if interrupted waiting for submission semaphore
     */
    protected void deleteAllObjectsHierarchical(String bucket, String prefix, BucketWipeResult result) throws InterruptedException {
        ListObjectsResult listing = null;
        ListObjectsRequest request = new ListObjectsRequest(bucket).withPrefix(prefix)
                .withEncodingType(EncodingType.url).withDelimiter("/");
        List<String> subPrefixes = new ArrayList<>();

        do {
            if (listing == null) {
                listing = client.listObjects(request);
            } else {
                listing = client.listMoreObjects(listing);
            }

            for (S3Object object : listing.getObjects()) {
                result.setLastKey(object.getKey());
                submitTask(new DeleteObjectTask(client, bucket, object.getKey()), result);
            }

            subPrefixes.addAll(listing.getCommonPrefixes());
        } while (listing.isTruncated());

        for (String subPrefix : subPrefixes) {
            deleteAllObjectsHierarchical(bucket, subPrefix, result);
        }
    }

    /**
     * Deletes all Keys from the specified bucket that start with the given prefix
     *
     * @param bucket the target bucket
     * @param prefix key prefix of objects to be deleted
     * @param result the asynchronous result of the operation
     * @throws InterruptedException if interrupted waiting for submission semaphore
     */
    public void deleteAllObjects(String bucket, String prefix, BucketWipeResult result) throws InterruptedException {
        ListObjectsResult listing = null;
        ListObjectsRequest request = new ListObjectsRequest(bucket).withPrefix(prefix).withEncodingType(EncodingType.url);
        do {
            if (listing == null) listing = client.listObjects(request);
            else listing = client.listMoreObjects(listing);

            for (S3Object object : listing.getObjects()) {
                result.setLastKey(object.getKey());
                submitTask(new DeleteObjectTask(client, bucket, object.getKey()), result);
            }

        } while (listing.isTruncated());
    }

    /**
     * Deletes all Object versions from the specified bucket that start with the given prefix
     *
     * @param bucket the target bucket
     * @param prefix key prefix of object versions to be deleted
     * @param result the asynchronous result of the operation
     * @throws InterruptedException if interrupted waiting for submission semaphore
     */
    public void deleteAllVersions(String bucket, String prefix, BucketWipeResult result) throws InterruptedException {
        ListVersionsResult listing = null;
        ListVersionsRequest request = new ListVersionsRequest(bucket).withPrefix(prefix).withEncodingType(EncodingType.url);
        do {
            if (listing != null) {
                request.setKeyMarker(listing.getNextKeyMarker());
                request.setVersionIdMarker(listing.getNextVersionIdMarker());
            }
            listing = client.listVersions(request);

            for (AbstractVersion version : listing.getVersions()) {
                result.setLastKey(version.getKey() + " (version " + version.getVersionId() + ")");
                submitTask(new DeleteVersionTask(client, bucket, version.getKey(), version.getVersionId()), result);
            }

        } while (listing.isTruncated());
    }

    public void deleteAllMpus(String bucket, BucketWipeResult result) throws InterruptedException {
        ListMultipartUploadsResult listing = null;
        ListMultipartUploadsRequest request = new ListMultipartUploadsRequest(bucket).withEncodingType(EncodingType.url);
        do {
            if (listing != null) {
                request.setKeyMarker(listing.getNextKeyMarker());
                request.setUploadIdMarker(listing.getNextUploadIdMarker());
            }
            listing = client.listMultipartUploads(request);

            for (Upload upload : listing.getUploads()) {
                result.setLastKey(upload.getKey() + " (uploadId " + upload.getUploadId() + ")");
                submitTask(new DeleteMpuTask(client, bucket, upload.getKey(), upload.getUploadId()), result);
            }

        } while (listing.isTruncated());
    }

    /**
     * Submits a task to be executed recording the fact in the result.  The result is updated as the task completes
     */
    private void submitTask(Runnable task, BucketWipeResult result) throws InterruptedException {
        submissionSemaphore.acquire();

        result.actionOutstanding();
        CompletableFuture.runAsync(task, executor)
                .exceptionally((e) -> {
                    result.addError(e.getMessage());
                    return null;
                })
                .thenRun(() -> {
                    result.actionComplete();
                    submissionSemaphore.release();
                });
    }

    protected static class DeleteObjectTask implements Runnable {
        private final S3Client client;
        private final String bucket;
        private final String key;

        public DeleteObjectTask(S3Client client, String bucket, String key) {
            this.client = client;
            this.bucket = bucket;
            this.key = key;
        }

        @Override
        public void run() {
            client.deleteObject(bucket, key);
        }
    }

    protected static class DeleteVersionTask implements Runnable {
        private final S3Client client;
        private final String bucket;
        private final String key;
        private final String versionId;

        public DeleteVersionTask(S3Client client, String bucket, String key, String versionId) {
            this.client = client;
            this.bucket = bucket;
            this.key = key;
            this.versionId = versionId;
        }

        @Override
        public void run() {
            client.deleteVersion(bucket, key, versionId);
        }
    }

    protected static class DeleteMpuTask implements Runnable {
        private final S3Client client;
        private final String bucket;
        private final String key;
        private final String uploadId;

        public DeleteMpuTask(S3Client client, String bucket, String key, String uploadId) {
            this.client = client;
            this.bucket = bucket;
            this.key = key;
            this.uploadId = uploadId;
        }

        @Override
        public void run() {
            client.abortMultipartUpload(new AbortMultipartUploadRequest(bucket, key, uploadId));
        }
    }
}
