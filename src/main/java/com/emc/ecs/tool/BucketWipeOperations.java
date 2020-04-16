package com.emc.ecs.tool;

import com.emc.object.s3.S3Client;
import com.emc.object.s3.bean.AbstractVersion;
import com.emc.object.s3.bean.DeleteObjectsResult;
import com.emc.object.s3.bean.EncodingType;
import com.emc.object.s3.bean.ListObjectsResult;
import com.emc.object.s3.bean.ListVersionsResult;
import com.emc.object.s3.bean.S3Object;
import com.emc.object.s3.request.DeleteObjectsRequest;
import com.emc.object.s3.request.ListObjectsRequest;
import com.emc.object.s3.request.ListVersionsRequest;
import com.emc.object.util.RestUtil;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

/**
 * Provides multiple Asynchronous operations for deleting objects from buckets
 *
 * An instance of the class can be used multiple times for different buckets as long as the S3Client configuration remains the same.
 *
 * Operations are async, users should pass in an instance of {@link BucketWipeResult} into which the result of the bucket
 * wipe operation (number of objects deleted etc) will be placed.  The operation is complete when the {@link BucketWipeResult#getCompletedFuture()}
 * completes.
 *
 * MaxConcurrent controls how many concurrent delete actions can be submitted at once, this allows an instance to handle deletion of millions of
 * objects from a bucket without causing a runaway OOM condition.  Note that the maxConcurrent applies to ALL actions within an instance, for example
 * if there are two callers they will both be competing to add new delete actions within the maxConcurrent limit.
 */
public class BucketWipeOperations {
    public static final int DEFAULT_THREADS = 32;
    public static final int DEFAULT_MAX_CONCURRENT = 2000;

    private S3Client client;
    private ExecutorService executor;
    private Semaphore submissionSemaphore;

    /**
     * Default constructor that uses the {@link #DEFAULT_THREADS} number of threads and {@link #DEFAULT_MAX_CONCURRENT} max concurrency
     */
    public BucketWipeOperations(S3Client client) {
        this(client, DEFAULT_THREADS, DEFAULT_MAX_CONCURRENT);
    }

    /**
     * Create an instance of the {@link BucketWipeOperations} class.  Instances are thread safe and can be used by multiple callers simultaneously
     * @param client The pre-configured S3 client to use when contacting the S3 server
     * @param numThreads Number of threads to use in the thread pool for  simultaneous actions
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
     * @param bucket the target S3 Bucket
     * @param sourceListFile filepath of the containing the object keys.  Each line in the file represents an object key
     * @param result asynchronous result of the operation
     * @throws InterruptedException if interrupted waiting for submission semaphore
     */
    public void deleteAllObjectsWithList(String bucket, String sourceListFile, BucketWipeResult result) throws InterruptedException {
        try {
            BufferedReader reader = new BufferedReader(new FileReader(sourceListFile));
            try {
                String key = reader.readLine();
                while (key != null) {
                    submitTask(new DeleteObjectTask(client, bucket, RestUtil.urlDecode(key)), result);
                    key = reader.readLine();
                }
            } finally {
                reader.close();
                result.allActionsSubmitted();
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException("File not found", e);
        } catch (IOException e) {
            throw new RuntimeException("Error reading key list line", e);
        }

        result.allActionsSubmitted();
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
                submitTask(new DeleteObjectTask(client, bucket, RestUtil.urlDecode(object.getKey())), result);
            }

            subPrefixes.addAll(listing.getCommonPrefixes());
        } while (listing.isTruncated());

        for(String subPrefix : subPrefixes) {
            deleteAllObjectsHierarchical(bucket, subPrefix, result);
        }

        result.allActionsSubmitted();
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

        result.allActionsSubmitted();
    }

    /**
     * Deletes all Object versions from the specified bucket that start with the given prefix
     *
     * @param bucket the target bucket
     * @param prefix key prefix of object versions to be deleted
     * @param result the asynchronous result of the operation
     * @throws InterruptedException if interrupted waiting for submission semaphore
     */
    public void deleteAllVersions(S3Client client, String bucket, String prefix, BucketWipeResult result) throws InterruptedException {
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
                submitTask(new DeleteVersionTask(client, bucket, RestUtil.urlDecode(version.getKey()), version.getVersionId()), result);
            }

        } while (listing.isTruncated());

        result.allActionsSubmitted();
    }

    /** Submits a task to be executed recording the fact in the result.  The result is updated as the task completes */
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

    protected class DeleteObjectTask implements Runnable {
        private S3Client client;
        private String bucket;
        private String key;

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

    protected class DeleteVersionTask implements Runnable {
        private S3Client client;
        private String bucket;
        private String key;
        private String versionId;

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

    protected class DeleteBatchObjectsTask implements Callable<DeleteObjectsResult> {
        private S3Client client;
        private DeleteObjectsRequest request;

        public DeleteBatchObjectsTask(S3Client client, DeleteObjectsRequest request) {
            this.client = client;
            this.request = request;
        }

        @Override
        public DeleteObjectsResult call() {
            return client.deleteObjects(request);
        }
    }
}
