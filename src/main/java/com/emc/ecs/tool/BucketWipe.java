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

import com.emc.object.Protocol;
import com.emc.object.s3.S3Client;
import com.emc.object.s3.S3Config;
import com.emc.object.s3.S3Exception;
import com.emc.object.s3.jersey.S3JerseyClient;
import org.apache.commons.cli.*;

import java.net.URI;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.emc.ecs.tool.BucketWipeOperations.DEFAULT_MAX_CONCURRENT;
import static com.emc.ecs.tool.BucketWipeOperations.DEFAULT_THREADS;

public class BucketWipe implements Runnable {
    public static void main(String[] args) {
        boolean debug = false;
        final String DISCLAIMER = "WARNING: THIS TOOL PERMANENTLY DELETES ALL DATA IN A BUCKET, INCLUDING ALL VERSIONS!!\n" +
                "Even if you have versioning enabled, the tool will destroy all data in the bucket and (by default) " +
                "the bucket itself.\nIf this is not what you want, please use a different tool.";
        final BucketWipe bucketWipe = new BucketWipe();
        try {
            CommandLine line = new DefaultParser().parse(options(), args, true);

            debug = line.hasOption("stacktrace");

            if (line.hasOption("h")) {
                printHelp();
                System.exit(1);
            }

            if (line.getArgs().length == 0) throw new IllegalArgumentException("must specify a bucket");

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
            if (line.hasOption("delete-mpus")) bucketWipe.setDeleteMpus(true);
            bucketWipe.setBucket(line.getArgs()[0]);

            if (!line.hasOption("y")) {
                System.out.println(DISCLAIMER);
                System.out.print("Are you sure to continue(Y/N):");
                Scanner scan = new Scanner(System.in);
                String s = scan.next();
                if (!s.equals("y") && !s.equals("Y"))
                    throw new Exception("Disclaimer is not accepted.");
            }else {
                System.out.println(DISCLAIMER);
                System.out.println("User acknowledged and accepted the above disclaimer with option -y/--accept-disclaimer.");
            }

            // update the user
            final AtomicBoolean monitorRunning = new AtomicBoolean(true);
            Thread statusThread = new Thread(() -> {
                while (monitorRunning.get()) {
                    try {
                        System.out.print("Objects deleted: " + bucketWipe.getResult().getDeletedObjects() + "\r");
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                }
            });
            statusThread.setDaemon(true);
            statusThread.start();

            long startTime = System.currentTimeMillis();
            bucketWipe.run();
            long duration = System.currentTimeMillis() - startTime;
            double xput = (double) bucketWipe.getResult().getDeletedObjects() / duration * 1000;

            monitorRunning.set(false);
            System.out.print("Objects deleted: " + bucketWipe.getResult().getDeletedObjects() + "\r");
            System.out.println();

            System.out.printf("Duration: %d secs (%.2f/s)%n", duration / 1000, xput);

            for (String error : bucketWipe.getResult().getErrors()) {
                System.out.println("Error: " + error);
            }

        } catch (Throwable t) {
            System.out.println("Error: " + t.getMessage());
            if (debug) t.printStackTrace();
            System.out.println("Last key before error: " + bucketWipe.getResult().getLastKey());
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
        options.addOption(Option.builder().longOpt("delete-mpus").desc("incomplete MPUs will prevent the " +
                "bucket from being deleted. use this option to clean up all incomplete MPUs").build());
        options.addOption(Option.builder("y").longOpt("accept-disclaimer")
                .desc("Acknowledge and accept that you understand \"THIS TOOL PERMANENTLY DELETES ALL DATA IN A BUCKET, INCLUDING ALL VERSIONS.\"")
                .build());
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
    private boolean hierarchical;
    private String sourceListFile;
    private boolean deleteMpus;
    private final BucketWipeResult result = new BucketWipeResult();

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

        BucketWipeOperations bucketWipeOperations = new BucketWipeOperations(client, threads, DEFAULT_MAX_CONCURRENT);
        try {
            if (sourceListFile != null) {
                bucketWipeOperations.deleteAllObjectsWithList(bucket, sourceListFile, result);
            } else if (hierarchical) {
                bucketWipeOperations.deleteAllObjectsHierarchical(bucket, prefix, result);
            } else if (client.getBucketVersioning(bucket).getStatus() == null) {
                bucketWipeOperations.deleteAllObjects(bucket, prefix, result);
            } else {
                bucketWipeOperations.deleteAllVersions(bucket, prefix, result);
            }

            if (deleteMpus) bucketWipeOperations.deleteAllMpus(bucket, result);

            result.allActionsSubmitted();

            // Wait for operation to complete
            result.getCompletedFuture().get();

            if (prefix == null && !keepBucket) {
                client.deleteBucket(bucket);
            }
        } catch (InterruptedException | ExecutionException | S3Exception e) {
            result.addError(e.getMessage());
        } finally {
            bucketWipeOperations.shutdown();
        }
    }

    public BucketWipeResult getResult() {
        return result;
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

    public boolean isDeleteMpus() {
        return deleteMpus;
    }

    public void setDeleteMpus(boolean deleteMpus) {
        this.deleteMpus = deleteMpus;
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

    public BucketWipe withSourceListFile(String sourceListFile) {
        setSourceListFile(sourceListFile);
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

    public BucketWipe withHierarchical(boolean hierarchical) {
        setHierarchical(hierarchical);
        return this;
    }

    public BucketWipe withDeleteMpus(boolean deleteMpus) {
        setDeleteMpus(deleteMpus);
        return this;
    }
}
