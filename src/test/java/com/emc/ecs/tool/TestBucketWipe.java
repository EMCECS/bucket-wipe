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
import com.emc.object.s3.bean.*;
import com.emc.object.s3.jersey.S3JerseyClient;
import com.emc.object.s3.request.AbortMultipartUploadRequest;
import com.emc.object.s3.request.ListMultipartUploadsRequest;
import com.emc.object.s3.request.ListObjectsRequest;
import com.emc.object.s3.request.ListVersionsRequest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.net.URI;

public class TestBucketWipe {
    private static final Logger log = LoggerFactory.getLogger(TestBucketWipe.class);

    private String accessKey;
    private String secretKey;
    private URI endpoint;

    @Before
    public void setup() throws Exception {
        accessKey = TestConfig.getPropertyNotEmpty(TestProperties.S3_ACCESS_KEY);
        secretKey = TestConfig.getPropertyNotEmpty(TestProperties.S3_SECRET_KEY);
        endpoint = new URI(TestConfig.getPropertyNotEmpty(TestProperties.S3_ENDPOINT));
    }

    S3Config getS3Config() {
        S3Config s3Config = new S3Config(Protocol.valueOf(endpoint.getScheme().toUpperCase()), endpoint.getHost())
                .withIdentity(accessKey).withSecretKey(secretKey)
                .withSmartClient(false);
        if (endpoint.getPort() > 0) s3Config.setPort(endpoint.getPort());
        return s3Config;
    }

    void createBucketAndKeys(S3Client client, String bucketName, String... keys) {
        client.createBucket(bucketName);
        for (String key : keys) {
            client.putObject(bucketName, key, new byte[]{}, null);
        }
    }

    void deleteBucket(S3Client client, String bucketName) {
        try {
            if (client.bucketExists(bucketName)) {
                if (client.getBucketVersioning(bucketName).getStatus() != null) {
                    ListVersionsResult listing = null;
                    ListVersionsRequest request = new ListVersionsRequest(bucketName).withEncodingType(EncodingType.url);
                    do {
                        if (listing != null) {
                            request.setKeyMarker(listing.getNextKeyMarker());
                            request.setVersionIdMarker(listing.getNextVersionIdMarker());
                        }
                        listing = client.listVersions(request);

                        for (AbstractVersion version : listing.getVersions()) {
                            client.deleteVersion(bucketName, version.getKey(), version.getVersionId());
                        }

                    } while (listing.isTruncated());
                } else {
                    ListObjectsResult listing = null;
                    ListObjectsRequest request = new ListObjectsRequest(bucketName).withEncodingType(EncodingType.url);
                    do {
                        if (listing == null) listing = client.listObjects(request);
                        else listing = client.listMoreObjects(listing);

                        for (S3Object object : listing.getObjects()) {
                            client.deleteObject(bucketName, object.getKey());
                        }

                    } while (listing.isTruncated());
                }

                ListMultipartUploadsResult listing = null;
                ListMultipartUploadsRequest request = new ListMultipartUploadsRequest(bucketName).withEncodingType(EncodingType.url);
                do {
                    if (listing != null) {
                        request.setKeyMarker(listing.getNextKeyMarker());
                        request.setUploadIdMarker(listing.getNextUploadIdMarker());
                    }
                    listing = client.listMultipartUploads(request);

                    for (Upload upload : listing.getUploads()) {
                        client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, upload.getKey(), upload.getUploadId()));
                    }

                } while (listing.isTruncated());

                client.deleteBucket(bucketName);
            }
        } catch (S3Exception e) {
            log.error("could not clean up bucket", e);
        }
    }

    @Test
    public void testUrlEncoding() {
        S3Client client = new S3JerseyClient(getS3Config());
        String bucketName = "test-bucket-wipe";
        String[] keys = {
                "foo!@#$%^&*()-_=+",
                "bar\\u00a1\\u00bfbar",
                "查找的unicode",
                "baz\\u0007bim"
        };

        // create bucket and keys
        createBucketAndKeys(client, bucketName, keys);

        try {
            // delete the bucket with bucket-wipe
            BucketWipe bucketWipe = new BucketWipe().withEndpoint(endpoint).withSmartClient(false)
                    .withAccessKey(accessKey).withSecretKey(secretKey);
            bucketWipe.withBucket(bucketName).run();

            if (bucketWipe.getResult().getErrors().size() > 0) {
                for (String error : bucketWipe.getResult().getErrors()) {
                    System.err.println(error);
                }
            }

            Assert.assertEquals(keys.length, bucketWipe.getResult().getDeletedObjects());
            Assert.assertEquals(0, bucketWipe.getResult().getErrors().size());

            try {
                client.listObjects(bucketName);
                Assert.fail("bucket still exists");
            } catch (S3Exception e) {
                if (e.getHttpCode() != 404) log.error("unexpected error listing bucket", e);
                Assert.assertEquals(e.getHttpCode(), 404);
            }
        } finally {
            // clean up bucket and keys if necessary
            deleteBucket(client, bucketName);
        }
    }

    @Test
    public void testWithSourceList() throws Exception {
        S3Client client = new S3JerseyClient(getS3Config());
        String bucketName = "test-bucket-wipe";
        String[] keys = {
                "key-1",
                "key-2",
                "key-3",
                "key-4",
                "key-5"
        };

        // create bucket and keys
        createBucketAndKeys(client, bucketName, keys);

        // write source list file
        File file = File.createTempFile("source-file-list-test", null);
        file.deleteOnExit();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(file, true))) {
            for (int i = 0; i < keys.length; ++i) {
                bw.write(keys[i]);
                if (i < keys.length - 1)
                    bw.write("\n");
            }
        }

        try {
            // delete the bucket with bucket-wipe
            BucketWipe bucketWipe = new BucketWipe().withEndpoint(endpoint).withSmartClient(false)
                    .withAccessKey(accessKey).withSecretKey(secretKey);
            bucketWipe.setSourceListFile(file.getAbsolutePath());
            bucketWipe.withBucket(bucketName).run();

            if (bucketWipe.getResult().getErrors().size() > 0) {
                for (String error : bucketWipe.getResult().getErrors()) {
                    System.err.println(error);
                }
            }

            Assert.assertEquals(keys.length, bucketWipe.getResult().getDeletedObjects());
            Assert.assertEquals(0, bucketWipe.getResult().getErrors().size());

            try {
                client.listObjects(bucketName);
                Assert.fail("bucket still exists");
            } catch (S3Exception e) {
                if (e.getHttpCode() != 404) log.error("unexpected error listing bucket", e);
                Assert.assertEquals(e.getHttpCode(), 404);
            }
        } finally {
            // clean up bucket and keys if necessary
            deleteBucket(client, bucketName);
        }
    }

    @Test
    public void testKeepBucket() {
        // create bucket
        S3Client client = new S3JerseyClient(getS3Config());
        String bucketName = "test-bucket-wipe";
        String[] keys = {
                "key-1",
                "key-2",
                "key-3",
                "key-4",
                "key-5"
        };

        // create bucket and keys
        createBucketAndKeys(client, bucketName, keys);

        try {
            // wipe bucket, but don't delete
            BucketWipe bucketWipe = new BucketWipe().withEndpoint(endpoint).withSmartClient(false)
                    .withAccessKey(accessKey).withSecretKey(secretKey);
            bucketWipe.setKeepBucket(true);
            bucketWipe.withBucket(bucketName).run();

            if (bucketWipe.getResult().getErrors().size() > 0) {
                for (String error : bucketWipe.getResult().getErrors()) {
                    System.err.println(error);
                }
            }

            Assert.assertEquals(keys.length, bucketWipe.getResult().getDeletedObjects());
            Assert.assertEquals(0, bucketWipe.getResult().getErrors().size());

            try {
                client.listObjects(bucketName);
            } catch (S3Exception e) {
                if (e.getHttpCode() != 404) log.error("unexpected error listing bucket", e);
                Assert.fail("bucket should still exist, but was deleted");
            }
        } finally {
            // clean up bucket and keys if necessary
            deleteBucket(client, bucketName);
        }
    }

    @Test
    public void testDeleteMpu() {
        S3Client client = new S3JerseyClient(getS3Config());
        String bucketName = "test-bucket-wipe";
        String[] keys = {
                "key-1",
                "key-2",
                "key-3",
                "key-4",
                "key-5"
        };

        // create bucket and keys
        createBucketAndKeys(client, bucketName, keys);

        // create a couple MPUs
        client.initiateMultipartUpload(bucketName, "mpu-1");
        client.initiateMultipartUpload(bucketName, "mpu-2");

        try {
            // wipe bucket
            BucketWipe bucketWipe = new BucketWipe().withEndpoint(endpoint).withSmartClient(false)
                    .withAccessKey(accessKey).withSecretKey(secretKey);
            bucketWipe.setDeleteMpus(true);
            bucketWipe.withBucket(bucketName).run();

            if (bucketWipe.getResult().getErrors().size() > 0) {
                for (String error : bucketWipe.getResult().getErrors()) {
                    System.err.println(error);
                }
            }

            Assert.assertEquals(keys.length + 2, bucketWipe.getResult().getDeletedObjects());
            Assert.assertEquals(0, bucketWipe.getResult().getErrors().size());

            try {
                client.listObjects(bucketName);
                Assert.fail("bucket still exists");
            } catch (S3Exception e) {
                if (e.getHttpCode() != 404) log.error("unexpected error listing bucket", e);
                Assert.assertEquals(e.getHttpCode(), 404);
            }
        } finally {
            // clean up bucket and keys if necessary
            deleteBucket(client, bucketName);
        }
    }
}
