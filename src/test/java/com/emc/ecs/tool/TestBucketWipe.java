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
import com.emc.object.s3.jersey.S3JerseyClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.net.URI;

public class TestBucketWipe {
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

    @Test
    public void testUrlEncoding() {

        // create bucket
        String bucketName = "test-bucket-wipe";
        S3Client client = new S3JerseyClient(getS3Config());
        client.createBucket(bucketName);
        boolean bucketExists = true;

        String[] keys = {
                "foo!@#$%^&*()-_=+",
                "bar\\u00a1\\u00bfbar",
                "查找的unicode",
                "baz\\u0007bim"
        };

        try {
            // create some keys with weird characters
            for (String key : keys) {
                client.putObject(bucketName, key, new byte[]{}, null);
            }

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
                if (e.getHttpCode() == 404) bucketExists = false;
                else throw e;
            }
        } finally {
            try {
                if (bucketExists) {
                    for (String key : keys) {
                        client.deleteObject(bucketName, key);
                    }
                    client.deleteBucket(bucketName);
                }
            } catch (S3Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testWithSourceList() throws Exception {
        String bucketName = "test-bucket-wipe";
        S3Client client = new S3JerseyClient(getS3Config());
        client.createBucket(bucketName);

        boolean bucketExists = true;

        String[] keys = {
                "key-1",
                "key-2",
                "key-3",
                "key-4",
                "key-5"
        };

        File file = File.createTempFile("source-file-list-test", null);
        file.deleteOnExit();
        FileWriter fw = new FileWriter(file, true);
        BufferedWriter bw = new BufferedWriter(fw);

        try {
            try {
                for (int i = 0; i < keys.length; ++i) {
                    String key = keys[i];
                    client.putObject(bucketName, key, new byte[]{}, null);
                    bw.write(key);
                    if (i < keys.length - 1)
                        bw.write("\n");
                }
            } finally {
                bw.close();
                fw.close();
            }

            // wipe bucket, but don't delete
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
                if (e.getHttpCode() == 404) bucketExists = false;
                else throw e;
            }
        } finally {
            try {
                if (bucketExists) {
                    for (String key : keys) {
                        client.deleteObject(bucketName, key);
                    }
                    client.deleteBucket(bucketName);
                }
            } catch (S3Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testKeepBucket() {
        // create bucket
        String bucketName = "test-bucket-wipe";
        S3Client client = new S3JerseyClient(getS3Config());
        client.createBucket(bucketName);

        boolean bucketExists = true;

        String[] keys = {
                "key-1",
                "key-2",
                "key-3",
                "key-4",
                "key-5"
        };

        try {
            // create some keys with weird characters
            for (String key : keys) {
                client.putObject(bucketName, key, new byte[]{}, null);
            }

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
                if (e.getHttpCode() == 404) bucketExists = false;
                else throw e;
            }
        } finally {
            try {
                if (bucketExists) {
                    client.deleteBucket(bucketName);
                } else {
                    Assert.fail("bucket no longer exists.");
                }
            } catch (S3Exception e) {
                e.printStackTrace();
            }
        }
    }
}
