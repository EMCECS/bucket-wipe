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

import com.emc.object.Protocol;
import com.emc.object.s3.S3Client;
import com.emc.object.s3.S3Config;
import com.emc.object.s3.S3Exception;
import com.emc.object.s3.bean.*;
import com.emc.object.s3.jersey.S3JerseyClient;
import com.emc.object.s3.request.DeleteObjectsRequest;
import com.emc.object.s3.request.ListObjectsRequest;
import com.emc.object.s3.request.ListVersionsRequest;
import com.emc.object.util.RestUtil;
import org.apache.commons.cli.*;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class BucketWipe implements Runnable {
    public static final int DEFAULT_THREADS = 32;
    public static final int QUEUE_SIZE = 2000;

    public static void main(String[] args) throws Exception {
        boolean debug = false;
        BucketWipe bucketWipe = null;
        try {
            CommandLine line = new DefaultParser().parse(options(), args, true);

            debug = line.hasOption("stacktrace");

            if (line.hasOption("h")) {
                printHelp();
                System.exit(1);
            }

            if (line.getArgs().length == 0) throw new IllegalArgumentException("must specify a bucket");

            bucketWipe = new BucketWipe();
            bucketWipe.setEndpoint(new URI(line.getOptionValue("e")));
            bucketWipe.setVhost(line.hasOption("vhost"));
            bucketWipe.setSmartClient(!line.hasOption("no-smart-client"));
            bucketWipe.setKeepBucket(line.hasOption("keep-bucket"));
            bucketWipe.setAccessKey(line.getOptionValue("a"));
            bucketWipe.setSecretKey(line.getOptionValue("s"));
            bucketWipe.setSourceListFile(line.getOptionValue("l"));
            if (line.hasOption("p")) bucketWipe.setPrefix(line.getOptionValue("p"));
            if (line.hasOption("t")) bucketWipe.setThreads(Integer.parseInt(line.getOptionValue("t")));
            if(line.hasOption("hier")) bucketWipe.setHierarchical(true);
            bucketWipe.setBucket(line.getArgs()[0]);

            // update the user
            final AtomicBoolean monitorRunning = new AtomicBoolean(true);
            final BucketWipe fBucketWipe = bucketWipe;
            Thread statusThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    while (monitorRunning.get()) {
                        try {
                            System.out.print("Objects deleted: " + fBucketWipe.getDeletedObjects() + "\r");
                            Thread.sleep(500);
                        } catch (InterruptedException e) {
                            // ignore
                        }
                    }
                }
            });
            statusThread.setDaemon(true);
            statusThread.start();

            long startTime = System.currentTimeMillis();
            bucketWipe.run();
            long duration = System.currentTimeMillis() - startTime;
            double xput = (double) bucketWipe.getDeletedObjects() / duration * 1000;

            monitorRunning.set(false);
            System.out.print("Objects deleted: " + fBucketWipe.getDeletedObjects() + "\r");
            System.out.println();

            System.out.println(String.format("Duration: %d secs (%.2f/s)", duration / 1000, xput));

            for (String error : bucketWipe.getErrors()) {
                System.out.println("Error: " + error);
            }

        } catch (Throwable t) {
            System.out.println("Error: " + t.getMessage());
            if (debug) t.printStackTrace();
            if (bucketWipe != null) System.out.println("Last key before error: " + bucketWipe.getLastKey());
            printHelp();
            System.exit(2);
        }
    }

    private static void printHelp() {
        new HelpFormatter().printHelp("java -jar bucket-wipe.jar [options] <bucket-name>", options());
    }

    private static Options options() {
        Options options = new Options();
        options.addOption(Option.builder().longOpt("stacktrace").desc("displays full stack trace of errors").build());
        options.addOption(Option.builder("h").longOpt("help").desc("displays this help text").build());
        options.addOption(Option.builder("e").longOpt("endpoint").hasArg().argName("URI").required()
                .desc("the endpoint to connect to, including protocol, host, and port").build());
        options.addOption(Option.builder().longOpt("vhost")
                .desc("enables DNS buckets and turns off load balancer").build());
        options.addOption(Option.builder().longOpt("no-smart-client")
                .desc("disables the ECS smart-client. use this option with an external load balancer").build());
        options.addOption(Option.builder("a").longOpt("access-key").hasArg().argName("access-key").required()
                .desc("the S3 access key").build());
        options.addOption(Option.builder("s").longOpt("secret-key").hasArg().argName("secret-key").required()
                .desc("the secret key").build());
        options.addOption(Option.builder("p").longOpt("prefix").hasArg().argName("prefix")
                .desc("deletes only objects under the specified prefix").build());
        options.addOption(Option.builder("t").longOpt("threads").hasArg().argName("threads")
                .desc("number of threads to use").build());
        options.addOption(Option.builder("hier").longOpt("hierarchical").desc("Enumerate the bucket hierarchically.  " +
                "This is recommended for ECS's filesystem-enabled buckets.").build());
        options.addOption(Option.builder().longOpt("keep-bucket").desc("do not delete the bucket when done").build());
        options.addOption(Option.builder("l").longOpt("key-list").hasArg().argName("file")
                .desc("instead of listing bucket, delete objects matched in source file key list").build());
        return options;
    }

    private URI endpoint;
    private boolean vhost;
    private boolean smartClient = true;
    private String accessKey;
    private String secretKey;
    private String bucket;
    private String prefix;
    private int threads = DEFAULT_THREADS;
    private boolean keepBucket;
    private EnhancedThreadPoolExecutor executor;
    private AtomicLong deletedObjects = new AtomicLong(0);
    private List<String> errors = Collections.synchronizedList(new ArrayList<String>());
    private String lastKey;
    private boolean hierarchical;
    private String sourceListFile;

    @Override
    public void run() {
        S3Config config;
        if (vhost || !smartClient) {
            config = new S3Config(endpoint);
            config.setUseVHost(vhost);
        } else {
            Protocol protocol = Protocol.valueOf(endpoint.getScheme().toUpperCase());
            config = new S3Config(protocol, endpoint.getHost()).withPort(endpoint.getPort());
        }
        config.withIdentity(accessKey).withSecretKey(secretKey);
        S3Client client = new S3JerseyClient(config);

        executor = new EnhancedThreadPoolExecutor(threads, new LinkedBlockingDeque<Runnable>(QUEUE_SIZE));

        if (sourceListFile != null) {
            deleteAllObjectsWithList(client, bucket, sourceListFile);
        } else if (hierarchical) {
            deleteAllObjectsHierarchical(client, bucket, prefix);
        } else if (client.getBucketVersioning(bucket).getStatus() == null) {
            deleteAllObjects(client, bucket, prefix);
        } else {
            deleteAllVersions(client, bucket, prefix);
        }

        executor.shutdown();

        try {
            if (prefix == null && !keepBucket) client.deleteBucket(bucket);
        } catch (S3Exception e) {
            errors.add(e.getMessage());
        }
    }

    protected void deleteAllObjectsWithList(S3Client client, String bucket, String sourceListFile) {
        List<Future> futures = new ArrayList<>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(sourceListFile));
            try {
                String key = reader.readLine();
                while (key != null) {
                    futures.add(executor.blockingSubmit(new DeleteObjectTask(client, bucket, RestUtil.urlDecode(key))));
                    while (futures.size() > QUEUE_SIZE) {
                        handleSingleFutures(futures, QUEUE_SIZE / 2);
                    }
                    key = reader.readLine();
                }
            } finally {
                reader.close();
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException("File not found", e);
        } catch (IOException e) {
            throw new RuntimeException("Error reading key list line", e);
        }
        handleSingleFutures(futures, futures.size());
    }

    protected void deleteAllObjectsHierarchical(S3Client client, String bucket, String prefix) {
        List<Future> futures = new ArrayList<>();
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
                lastKey = object.getKey();
                futures.add(executor.blockingSubmit(new DeleteObjectTask(client, bucket, RestUtil.urlDecode(object.getKey()))));
            }

            while (futures.size() > QUEUE_SIZE) {
                handleSingleFutures(futures, QUEUE_SIZE / 2);
            }

            subPrefixes.addAll(listing.getCommonPrefixes());
        } while (listing.isTruncated());

        handleSingleFutures(futures, futures.size());

        for(String subPrefix : subPrefixes) {
            deleteAllObjectsHierarchical(client, bucket, subPrefix);
        }
    }

    protected void deleteAllObjects(S3Client client, String bucket, String prefix) {
        List<Future> futures = new ArrayList<>();
        ListObjectsResult listing = null;
        ListObjectsRequest request = new ListObjectsRequest(bucket).withPrefix(prefix).withEncodingType(EncodingType.url);
        do {
            if (listing == null) listing = client.listObjects(request);
            else listing = client.listMoreObjects(listing);

            for (S3Object object : listing.getObjects()) {
                lastKey = object.getKey();
                futures.add(executor.blockingSubmit(new DeleteObjectTask(client, bucket, object.getKey())));
            }

            while (futures.size() > QUEUE_SIZE) {
                handleSingleFutures(futures, QUEUE_SIZE / 2);
            }
        } while (listing.isTruncated());

        handleSingleFutures(futures, futures.size());

    }

    protected void deleteAllVersions(S3Client client, String bucket, String prefix) {
        List<Future> futures = new ArrayList<>();
        ListVersionsResult listing = null;
        ListVersionsRequest request = new ListVersionsRequest(bucket).withPrefix(prefix).withEncodingType(EncodingType.url);
        do {
            if (listing != null) {
                request.setKeyMarker(listing.getNextKeyMarker());
                request.setVersionIdMarker(listing.getNextVersionIdMarker());
            }
            listing = client.listVersions(request);

            for (AbstractVersion version : listing.getVersions()) {
                lastKey = version.getKey() + " (version " + version.getVersionId() + ")";
                futures.add(executor.blockingSubmit(new DeleteVersionTask(client, bucket,
                        RestUtil.urlDecode(version.getKey()), version.getVersionId())));
            }

            while (futures.size() > QUEUE_SIZE) {
                handleSingleFutures(futures, QUEUE_SIZE / 2);
            }
        } while (listing.isTruncated());

        handleSingleFutures(futures, futures.size());
    }

    protected void handleSingleFutures(List<Future> futures, int num) {
        for (Iterator<Future> i = futures.iterator(); i.hasNext() && num-- > 0; ) {
            Future future = i.next();
            i.remove();
            try {
                future.get();
                deletedObjects.incrementAndGet();
            } catch (InterruptedException e) {
                errors.add(e.getMessage());
            } catch (ExecutionException e) {
                errors.add(e.getCause().getMessage());
            }
        }
    }

    public long getDeletedObjects() {
        return deletedObjects.get();
    }

    public List<String> getErrors() {
        return errors;
    }

    public URI getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(URI endpoint) {
        this.endpoint = endpoint;
    }

    public boolean isVhost() {
        return vhost;
    }

    public void setVhost(boolean vhost) {
        this.vhost = vhost;
    }

    public boolean isSmartClient() {
        return smartClient;
    }

    public void setSmartClient(boolean smartClient) {
        this.smartClient = smartClient;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getSourceListFile() { return sourceListFile;}

    public void setSourceListFile(String sourceListFile) { this.sourceListFile = sourceListFile;}

    public String getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public int getThreads() {
        return threads;
    }

    public void setThreads(int threads) {
        this.threads = threads;
    }

    public boolean isKeepBucket() {
        return keepBucket;
    }

    public void setKeepBucket(boolean keepBucket) {
        this.keepBucket = keepBucket;
    }

    public boolean isHierarchical() {
        return hierarchical;
    }

    public void setHierarchical(boolean hierarchical) {
        this.hierarchical = hierarchical;
    }

    public String getLastKey() {
        return lastKey;
    }

    public BucketWipe withEndpoint(URI endpoint) {
        setEndpoint(endpoint);
        return this;
    }

    public BucketWipe withVhost(boolean vhost) {
        setVhost(vhost);
        return this;
    }

    public BucketWipe withSmartClient(boolean smartClient) {
        setSmartClient(smartClient);
        return this;
    }

    public BucketWipe withAccessKey(String accessKey) {
        setAccessKey(accessKey);
        return this;
    }

    public BucketWipe withSecretKey(String secretKey) {
        setSecretKey(secretKey);
        return this;
    }

    public BucketWipe withBucket(String bucket) {
        setBucket(bucket);
        return this;
    }

    public BucketWipe withPrefix(String prefix) {
        setPrefix(prefix);
        return this;
    }

    public BucketWipe withThreads(int threads) {
        setThreads(threads);
        return this;
    }

    public BucketWipe withKeepBucket(boolean keepBucket) {
        setKeepBucket(keepBucket);
        return this;
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
}
