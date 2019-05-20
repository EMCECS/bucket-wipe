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
// Atmos imports
import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.emc.atmos.api.AtmosApi;
import com.emc.atmos.api.jersey.AtmosApiClient;
import com.emc.atmos.AtmosException;
import com.emc.atmos.api.ObjectPath;
import com.emc.atmos.api.*;
import com.emc.atmos.api.bean.ListDirectoryResponse;
import com.emc.atmos.api.bean.DirectoryEntry;
import com.emc.atmos.api.request.ListDirectoryRequest;
// atmos graph related
import java.util.concurrent.Callable;

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.jgrapht.traverse.BreadthFirstIterator;

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
            if (line.hasOption("hier")) bucketWipe.setHierarchical(true);
            bucketWipe.setBucket(line.getArgs()[0]);
            //*********start Atmos related*******************************
            bucketWipe.setAtmosOption(line.hasOption("atmos"));
            bucketWipe.setPort(Integer.parseInt(line.getOptionValue("port", "80")));
            if (line.hasOption("o")) {
                bucketWipe.setAtmosObjectSpace(true);
            } else {
                bucketWipe.setAtmosObjectSpace(false);
            }
            bucketWipe.setRemoteRoot(line.getArgs()[0]);  // remoteroot
            // ****************end atmos related
            // update the user
            final AtomicBoolean monitorRunning = new AtomicBoolean(true);
            final BucketWipe fBucketWipe = bucketWipe;
            Thread statusThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    while (monitorRunning.get()) {
                        try {
                            // add atmos stats
                            if (fBucketWipe.getAtmosOption()) {
                                System.out.print("Objects deleted: " + (fBucketWipe.getAtmosDeletedDirs() + fBucketWipe.getAtmosDeletedFiles()) + "\r");
                            } else {
                                System.out.print("Objects deleted: " + fBucketWipe.getDeletedObjects() + "\r");
                            }
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
            double xput;

            // atmos related
            if (fBucketWipe.getAtmosOption()) {
                xput = (double) (bucketWipe.getAtmosDeletedFiles() + bucketWipe.getAtmosDeletedDirs()) / duration * 1000;
            } else {
                xput = (double) bucketWipe.getDeletedObjects() / duration * 1000;
            }
            monitorRunning.set(false);
            // atmos related
            if (fBucketWipe.getAtmosOption()) {
                System.out.println("Deleted " + (fBucketWipe.getAtmosDeletedDirs() + fBucketWipe.getAtmosDeletedFiles()) + " atmos objects");
                System.out.println("Files: " + fBucketWipe.getAtmosDeletedFiles() + " Directories: " + fBucketWipe.getAtmosDeletedDirs() + " Failed Objects: " + fBucketWipe.getFailedCount());
                System.out.println("Failed Files: " + fBucketWipe.getFailedItems());
            } else {
                System.out.print("Objects deleted: " + fBucketWipe.getDeletedObjects() + "\r");
                System.out.println();
            }
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
        new HelpFormatter().printHelp("java -jar bucket-wipe.jar [options] <bucket-name or atmos directory path>", options());
    }

    // options to use for both atmos and EMC
    private static Options options() {
        Options options = new Options();
        options.addOption(Option.builder().longOpt("stacktrace").desc("displays full stack trace of errors").build());
        options.addOption(Option.builder("h").longOpt("help").desc("displays this help text").build());
        options.addOption(Option.builder("atmos").longOpt("atmos")
                .desc("the tool is used to delete Atmos namespace").build());
        options.addOption(Option.builder("e").longOpt("endpoint").hasArg().argName("URI").required()
                .desc("the endpoint to connect to, including protocol, host, and port or Atmos access point host").build());
        options.addOption(Option.builder().longOpt("vhost")
                .desc("enables DNS buckets and turns off load balancer").build());
        options.addOption(Option.builder().longOpt("no-smart-client")
                .desc("disables the ECS smart-client. use this option with an external load balancer").build());
        options.addOption(Option.builder("a").longOpt("access-key").hasArg().argName("access-key").required()
                .desc("the S3 access key or Atmos UID in the form of subtenantid/uid, e.g. 640f9a5cc636423fbc748566b397d1e1/uid1").build());
        options.addOption(Option.builder("s").longOpt("secret-key").hasArg().argName("secret-key").required()
                .desc("the secret key or Atmos Shared secret if option -atmos is used").build());
        options.addOption(Option.builder("p").longOpt("prefix").hasArg().argName("prefix")
                .desc("deletes only objects under the specified prefix").build());
        options.addOption(Option.builder("o").longOpt("atmos-object-space").desc("atmos only: use atmos object space  " +
                "If this option is not set namespace object structure will used by default").build());
        options.addOption(Option.builder("port").longOpt("atmosport").hasArg().argName("port")
                .desc("Atmos access point port (default 80)").build());
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

    // *********Atmos related
    private boolean atmosOption;
    private String remoteroot; // remoteroot is Atmos' bucket name
    private AtmosApi atmosClient;
    private boolean atmosObjectSpace;
    private long dirCount;
    private long fileCount;
    private SimpleDirectedGraph<TaskNode, DefaultEdge> graph;
    private Set<TaskNode> failedItems;
    private BlockingQueue<Runnable> queue;
    private ThreadPoolExecutor pool;
    private int port;
    private int failedCount;
    private int completedCount;

    public void AtmosInit() {
        dirCount = 0;
        fileCount = 0;
        failedCount = 0;
        completedCount = 0;
    }


    @Override
    public void run() {
        // Atmos related
        if (getAtmosOption()) {
            // Atmos initialize delete counters
            AtmosInit();
            // Make sure remote path is in the correct format
            if (!remoteroot.startsWith("/")) {
                remoteroot = "/" + remoteroot;
            }
            if (!remoteroot.endsWith("/")) {
                // Must be a dir (ends with /)
                remoteroot = remoteroot + "/";
            }

            AtmosConfig atmosConfiguration = new AtmosConfig(accessKey, secretKey, endpoint);

            atmosClient = new AtmosApiClient(atmosConfiguration);
            // add executor new
            executor = new EnhancedThreadPoolExecutor(threads, new LinkedBlockingDeque<Runnable>(QUEUE_SIZE));

            if (sourceListFile != null) {
                deleteAtmosObjectsWithList(atmosClient, sourceListFile);
            } else {
                deleteAtmosObjectsHierarchical(atmosClient, remoteroot);
            }
            // shutdown executor
            executor.shutdown();

            // end atmos option
        } else {

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
        } // end atmos if
    }

//**************ATMOS DELETE  *********************

    protected void deleteAtmosObjectsHierarchical(AtmosApi client, String path) {
        List<Future> futures = new ArrayList<>();
        ObjectPath objPath = new ObjectPath(path);

        failedItems = Collections.synchronizedSet(new HashSet<TaskNode>());

        graph = new SimpleDirectedGraph<TaskNode, DefaultEdge>(DefaultEdge.class);

        DeleteAtmosDirTask ddt = new DeleteAtmosDirTask();
        ddt.setDirPath(objPath);
        ddt.setAtmosDelete(this);
        ddt.addToGraph(graph);

        while (true) {
            synchronized (graph) {  // graph start
                if (graph.vertexSet().size() == 0) {
                    // We're done
                    break;
                }

                // Look for available unsubmitted tasks
                BreadthFirstIterator<TaskNode, DefaultEdge> iterator = new BreadthFirstIterator<TaskNode, DefaultEdge>(graph);
                while (iterator.hasNext()) {  // while start
                    TaskNode tskNode = iterator.next();
                    if (graph.inDegreeOf(tskNode) == 0 && !tskNode.isQueued()) { // if start
                        tskNode.setQueued(true);
                        futures.add(executor.blockingSubmit(tskNode));
                    } // if end
                } // while end
            } //graph end
            while (futures.size() > QUEUE_SIZE) {
                handleSingleFutures(futures, QUEUE_SIZE / 2);
            }
        } // while end
        handleSingleFutures(futures, futures.size());
    }  // class end


    protected void deleteAtmosObjectsWithList(AtmosApi client, String sourceListFile) {
        List<Future> futures = new ArrayList<>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(sourceListFile));
            try {
                String oid = reader.readLine();
                while (oid != null) {
                    ObjectIdentifier id = atmosObjectSpace ? new ObjectId(oid) : new ObjectPath(oid);
                    futures.add(executor.blockingSubmit(new DeleteAtmosObjectTask(client, id)));
                    while (futures.size() > QUEUE_SIZE) {
                        handleSingleFutures(futures, QUEUE_SIZE / 2);
                    }
                    oid = reader.readLine();
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

        for (String subPrefix : subPrefixes) {
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

    public String getSourceListFile() {
        return sourceListFile;
    }

    public void setSourceListFile(String sourceListFile) {
        this.sourceListFile = sourceListFile;
    }

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

    // ************ start  Atmos related*******************
    public boolean getAtmosOption() {
        return atmosOption;
    }

    public void setAtmosOption(boolean option) {
        this.atmosOption = option;
    }

    public BucketWipe withAtmos() {
        setAtmosOption(true);
        return this;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getRemoteRoot() {
        return remoteroot;
    }

    public void setRemoteRoot(String remoteroot) {
        this.remoteroot = remoteroot;
    }

    public BucketWipe withRemoteRoot(String remoteroot) {
        setRemoteRoot(remoteroot);
        return this;
    }

    public long getAtmosDeletedDirs() {
        return dirCount;
    }

    public long getAtmosDeletedFiles() {
        return fileCount;
    }

    public AtmosApi getAtmosClient() {
        return atmosClient;
    }

    public int getFailedCount() {
        return failedCount;
    }

    public Set<TaskNode> getFailedItems() {
        return failedItems;
    }

    public void setAtmosObjectSpace(boolean atmosObjectSpace) {
        this.atmosObjectSpace = atmosObjectSpace;
    }

    public synchronized void increment(ObjectPath objectPath) {
        if (objectPath.isDirectory()) {
            dirCount++;
        } else {
            fileCount++;
        }
    }

    public synchronized void success(TaskNode task, ObjectPath objectPath) {

        completedCount++;
        long pct = completedCount * 100 / (fileCount + dirCount);
    }

    public synchronized void failure(TaskNode task, ObjectPath objectPath,
                                     Exception e) {

        failedCount++;
        failedItems.add(task);
    }

    public SimpleDirectedGraph<TaskNode, DefaultEdge> getGraph() {
        return graph;
    }

//***************** end Atmos related

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
    // *************** atmos related start

    protected class DeleteAtmosObjectTask implements Runnable {
        private AtmosApi client;
        private ObjectIdentifier id;

        public DeleteAtmosObjectTask(AtmosApi client, ObjectIdentifier id) {
            this.client = client;
            this.id = id;
        }

        @Override
        public void run() {
            client.delete(id);
        }
    }


    //*********************  DELETE ATMOS FILE TASK *************************************
    protected class DeleteAtmosFileTask extends TaskNode {

        private ObjectPath filePath;
        private BucketWipe atmosWipe;

        @Override
        protected TaskResult execute() throws Exception {
            try {
                atmosWipe.getAtmosClient().delete(filePath);
            } catch (AtmosException e) {
                atmosWipe.failure(this, filePath, e);
                return new TaskResult(false);
            }

            atmosWipe.success(this, filePath);
            return new TaskResult(true);
        }

        public ObjectPath getFilePath() {
            return filePath;
        }

        public void setFilePath(ObjectPath filePath) {
            this.filePath = filePath;
        }

        public void setAtmosDelete(BucketWipe atmosWipe) {
            this.atmosWipe = atmosWipe;
        }
    }

    //***********END DELETE ATMOS FILE TASK **********************************
    // ********* ATMOS DELETE DIR TASK ************************************
    protected class DeleteAtmosDirTask extends TaskNode {

        private ObjectPath dirPath;
        private BucketWipe atmosWipe;
        private Set<DeleteAtmosDirChild> parentDirs = new HashSet<DeleteAtmosDirChild>();

        @Override
        protected TaskResult execute() throws Exception {

            try {

                // All entries in this directory will become parents of the child task
                // that deletes the current directory after it's empty.
                DeleteAtmosDirChild child = new DeleteAtmosDirChild();
                child.addParent(this);
                child.addToGraph(atmosWipe.getGraph());
                for (DeleteAtmosDirChild ch : parentDirs) {
                    ch.addParent(child);
                }

                ListDirectoryRequest request = new ListDirectoryRequest().path(dirPath);
                List<DirectoryEntry> dirEntries = atmosWipe.getAtmosClient().listDirectory(request).getEntries();

                while (request.getToken() != null) {
                    dirEntries.addAll(atmosWipe.getAtmosClient().listDirectory(request).getEntries());
                }


                for (DirectoryEntry entry : dirEntries) {
                    // get path for the current entry
                    ObjectPath entryPath = new ObjectPath(dirPath, entry);

                    if (entry.isDirectory()) {
                        DeleteAtmosDirTask deleteDirTask = new DeleteAtmosDirTask();

                        deleteDirTask.setDirPath(entryPath);
                        deleteDirTask.setAtmosDelete(atmosWipe);
                        deleteDirTask.addParent(this);

                        deleteDirTask.addToGraph(atmosWipe.getGraph());
                        child.addParent(deleteDirTask);
                        // If we have any other children to depend on, add them
                        for (DeleteAtmosDirChild ch : parentDirs) {
                            ch.addParent(deleteDirTask);
                            deleteDirTask.parentDirs.add(ch);
                        }
                        deleteDirTask.parentDirs.add(child);
                    } else {
                        DeleteAtmosFileTask deleteFileTask = new DeleteAtmosFileTask();
                        deleteFileTask.setFilePath(entryPath);
                        deleteFileTask.setAtmosDelete(atmosWipe);
                        deleteFileTask.addParent(this);
                        atmosWipe.increment(entryPath);
                        deleteFileTask.addToGraph(atmosWipe.getGraph());
                        child.addParent(deleteFileTask);
                    }
                }

            } catch (Exception e) {
                atmosWipe.failure(this, dirPath, e);
                return new TaskResult(false);
            }
            return new TaskResult(true);
        }

        public ObjectPath getDirPath() {
            return dirPath;
        }

        public void setDirPath(ObjectPath dirPath) {
            this.dirPath = dirPath;
        }

        public void setAtmosDelete(BucketWipe atmosWipe) {
            this.atmosWipe = atmosWipe;
        }

        /**
         * You can't delete the directory until all the children are deleted, so
         * this task will depend on all the children before delete.
         */
        public class DeleteAtmosDirChild extends TaskNode {

            @Override
            protected TaskResult execute() throws Exception {
                if (!dirPath.toString().equals("/")) {
                    // Delete directory
                    try {
                        //*skip delete* directory if keep-bucket option is set
                        if (!(dirPath.isDirectory() && remoteroot.equals(dirPath.toString()) && keepBucket)) {
                            atmosWipe.getAtmosClient().delete(dirPath);
                            atmosWipe.increment(dirPath);
                        }
                    } catch (AtmosException e) {
                        atmosWipe.failure(this, dirPath, e);
                        return new TaskResult(false);
                    }
                }
                atmosWipe.success(this, dirPath);

                return new TaskResult(true);
            }
        }
    }

    //*****  ATMOS DELETE DIR END

    //         end ATMOS related
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

    //************************************* ATMOS related tasks**********************
    protected abstract class TaskNode implements Callable<TaskResult> {

        protected Set<TaskNode> parentTasks;
        protected SimpleDirectedGraph<TaskNode, DefaultEdge> graph;
        private boolean queued;

        public TaskNode(Set<TaskNode> parentTasks) {
            this.parentTasks = parentTasks;
            if (parentTasks == null) {
                this.parentTasks = new HashSet<TaskNode>();
            }
        }

        public TaskNode() {
            this.parentTasks = new HashSet<TaskNode>();
        }

        public void addParent(TaskNode parent) {
            parentTasks.add(parent);
            if (graph != null) {
                synchronized (graph) {
                    graph.addEdge(parent, this);
                }
            }
        }

        public void addToGraph(SimpleDirectedGraph<TaskNode, DefaultEdge> graph) {
            this.graph = graph;

            synchronized (graph) {
                graph.addVertex(this);
                for (TaskNode parent : parentTasks) {
                    try {
                        graph.addEdge(parent, this);
                    } catch (IllegalArgumentException e) {
                        // The parent task probably already completed.
                    }
                }
            }
        }

        @Override
        public TaskResult call() throws Exception {
            if (graph == null) {
                throw new IllegalStateException("Task not in graph?");
            }

            TaskResult result = null;
            try {
                result = execute();
            } catch (Exception e) {
                result = new TaskResult(false);
            }

            // Completed.  Remove from graph.
            removeFromGraph();

            return result;
        }

        private void removeFromGraph() {
            if (graph == null) {
                throw new IllegalStateException("Task not in graph?");
            }
            synchronized (graph) {
                graph.removeVertex(this);
            }
        }

        protected abstract TaskResult execute() throws Exception;

        public void setQueued(boolean queued) {
            this.queued = queued;
        }

        public boolean isQueued() {
            return queued;
        }
    }

    protected class TaskResult {
        private boolean successful;

        public TaskResult(boolean successful) {
            this.setSuccessful(successful);
        }

        public void setSuccessful(boolean successful) {
            this.successful = successful;
        }

        public boolean isSuccessful() {
            return successful;
        }
    }
    //*********************** END Atmos related
}
