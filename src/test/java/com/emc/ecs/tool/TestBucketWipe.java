/**
 * Copyright 2016-2019 Dell Inc. or its subsidiaries.  All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 * <p>
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.emc.ecs.tool;

import com.emc.atmos.AtmosException;
import com.emc.atmos.StickyThreadAlgorithm;
import com.emc.atmos.api.AtmosApi;
import com.emc.atmos.api.AtmosConfig;
import com.emc.atmos.api.ObjectId;
import com.emc.atmos.api.ObjectPath;
import com.emc.atmos.api.bean.DirectoryEntry;
import com.emc.atmos.api.jersey.AtmosApiClient;
import com.emc.atmos.api.request.CreateObjectRequest;
import com.emc.atmos.api.request.ListDirectoryRequest;
import com.emc.object.Protocol;
import com.emc.object.s3.S3Client;
import com.emc.object.s3.S3Config;
import com.emc.object.s3.S3Exception;
import com.emc.object.s3.jersey.S3JerseyClient;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.net.URI;
import java.util.List;
import java.util.Random;


public class TestBucketWipe {
    private String accessKey;
    private String secretKey;
    private URI endpoint;

    // Atmos test related
    private String atmosUID;
    private String atmosSecret;
    private URI atmosEndpoint;

    private static final String ATMOS_TEST_DIR_PREFIX = "atmos_wipe_";

    private AtmosConfig config;
    protected AtmosApi api;

    @Before
    public void setup() throws Exception {
        accessKey = TestConfig.getPropertyNotEmpty(TestProperties.S3_ACCESS_KEY);
        secretKey = TestConfig.getPropertyNotEmpty(TestProperties.S3_SECRET_KEY);
        endpoint = new URI(TestConfig.getPropertyNotEmpty(TestProperties.S3_ENDPOINT));

        // Atmos related setAtmosatmosWipe
        atmosSecret = TestConfig.getPropertyNotEmpty(TestProperties.ATMOS_SECRET);
        atmosEndpoint = new URI(TestConfig.getPropertyNotEmpty(TestProperties.ATMOS_ENDPOINT));
        atmosUID = TestConfig.getPropertyNotEmpty(TestProperties.ATMOS_UID);


        AtmosConfig config = new AtmosConfig(atmosUID, atmosSecret, atmosEndpoint);

        Assume.assumeTrue("Could not load Atmos configuration", config != null);
        config.setDisableSslValidation(false);
        config.setEnableExpect100Continue(false);
        config.setEnableRetry(false);
        config.setLoadBalancingAlgorithm(new StickyThreadAlgorithm());
        api = new AtmosApiClient(config);
    }

    protected int AtmosCreateObjectTreeRecursively(ObjectPath path, int maxFileNums, int depth) {

        String fileContent = "test atmos bucket wipe tool file contents bla-bla-bla";
        int depthCounter = depth;
        ObjectPath dirPath;

        if (path.isDirectory()) { //if
            dirPath = path;
            do {
                // create maxNumFiles in the current directory
                for (int i = 0; i < maxFileNums; i++) {
                    // create file name
                    ObjectPath filePath = new ObjectPath(dirPath + rand8char() + "_file_" + i + ".tmp");
                    CreateObjectRequest request = new CreateObjectRequest().identifier(filePath);
                    ObjectId id = this.api.createObject(request).getObjectId();
                    Assert.assertNotNull("null file ID returned", id);
                }
                // if this is not the last subdirectory create subdirectory
                if (depthCounter > 0) {
                    dirPath = new ObjectPath(dirPath + rand8char() + "_dir_" + depthCounter + "/");
                    ObjectId id = this.api.createDirectory(dirPath);
                    Assert.assertNotNull("null directory ID returned", id);
                    depthCounter--;
                    depthCounter = AtmosCreateObjectTreeRecursively(dirPath, maxFileNums, depthCounter);
                } else {
                    return depthCounter;
                }
            } while (depthCounter > 0);
        } // end if
        return depthCounter;
    }

    private String rand8char() {
        Random r = new Random();
        StringBuilder sb = new StringBuilder(8);
        for (int i = 0; i < 8; i++) {
            sb.append((char) ('a' + r.nextInt(26)));
        }
        return sb.toString();
    }


    // Test connection to Atmos server
    @Test
    public void testAtmosConnect() {

        try {
            String version = api.getServiceInformation().getAtmosVersion();
            //    Assert.fail("- Atmos version-  : " + version);
            Assert.assertFalse("- Error -1- error - version is null but expected not", version == null);

        } catch (AtmosException e) {
            Assert.fail("- Error -2- connection to Atmos server failed with error code : " + e.getErrorCode());
            System.exit(3);
        }
    }

    @Test
    public void testAtmosCreateDeleteDirStructure() {

        int numberFiles = 3; // number of files in each directory
        int depth = 3; //depth of the directory tree
        int width = 3; // number of sub-directories in the root layer
        String rootDirName = "/test-dir_root/";
        // create root directory in tenat space
        ObjectPath path = new ObjectPath(rootDirName);
        ObjectId id = this.api.createDirectory(path);
        Assert.assertNotNull("- Error -1- null root directory ID returned", id);

        try {
            for (int i = 0; i < width; i++) {
                AtmosCreateObjectTreeRecursively(path, numberFiles, depth);
            }
        } catch (Exception e) {
            Assert.fail("- Error -2- directory stucture creation failed with error message : " + e.getMessage());
        }
        // now delete directory created structure with bucket-wipe Atmos option call
        try {
            ListDirectoryRequest request = new ListDirectoryRequest().path(path);
            List<DirectoryEntry> ents = this.api.listDirectory(request).getEntries();
            if (!ents.isEmpty()) {
                BucketWipe bucketWipe = new BucketWipe().withEndpoint(atmosEndpoint).withAccessKey(atmosUID).withSecretKey(atmosSecret);
                String atmSecretKey = bucketWipe.getSecretKey();
                Assert.assertEquals("- Error -1- wrong secret key returned : ", atmosSecret, atmSecretKey);
                bucketWipe.withAtmos().withPrefix(rootDirName).run();
            }
            try {
                // verify that rootDirName was deleted - the call below should result in exception
                this.api.listDirectory(request).getEntries();
                Assert.fail("- Error -2- top directory was not deleted : " + rootDirName);

            } catch (AtmosException e) {
                // expected exception  - rootDirName does not exists
                Assert.assertEquals("- Error -2- wrong error code returned : ", 404, e.getHttpCode());
            }
        } catch (Exception e) {
            Assert.fail("- Error -3- BucketWipe operation failed with error message : " + e.getMessage());
        }

    }


    @Test
    public void testUrlEncoding() {

        // create bucket
        String bucketName = "test-bucket-wipe";
        S3Client client = new S3JerseyClient(new S3Config(Protocol.valueOf(endpoint.getScheme().toUpperCase()),
                // disable smart-client to work around STORAGE-3299
                endpoint.getHost()).withIdentity(accessKey).withSecretKey(secretKey).withSmartClient(false));
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
            BucketWipe bucketWipe = new BucketWipe().withEndpoint(endpoint).withAccessKey(accessKey).withSecretKey(secretKey);
            bucketWipe.withBucket(bucketName).run();

            if (bucketWipe.getErrors().size() > 0) {
                for (String error : bucketWipe.getErrors()) {
                    System.err.println(error);
                }
            }

            Assert.assertEquals(keys.length, bucketWipe.getDeletedObjects());
            Assert.assertEquals(0, bucketWipe.getErrors().size());

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
        S3Client client = new S3JerseyClient(new S3Config(Protocol.valueOf(endpoint.getScheme().toUpperCase()),
                // disable smart-client to work around STORAGE-3299
                endpoint.getHost()).withIdentity(accessKey).withSecretKey(secretKey).withSmartClient(false));
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
                    if (i < keys.length -1)
                        bw.write("\n");
                }
            } finally {
                bw.close();
                fw.close();
            }

            // wipe bucket, but don't delete
            BucketWipe bucketWipe = new BucketWipe().withEndpoint(endpoint).withAccessKey(accessKey).withSecretKey(secretKey);
            bucketWipe.setSourceListFile(file.getAbsolutePath());
            bucketWipe.withBucket(bucketName).run();

            if (bucketWipe.getErrors().size() > 0) {
                for (String error : bucketWipe.getErrors()) {
                    System.err.println(error);
                }
            }

            Assert.assertEquals(keys.length, bucketWipe.getDeletedObjects());
            Assert.assertEquals(0, bucketWipe.getErrors().size());

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
        S3Client client = new S3JerseyClient(new S3Config(Protocol.valueOf(endpoint.getScheme().toUpperCase()),
                // disable smart-client to work around STORAGE-3299
                endpoint.getHost()).withIdentity(accessKey).withSecretKey(secretKey).withSmartClient(false));
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
            BucketWipe bucketWipe = new BucketWipe().withEndpoint(endpoint).withAccessKey(accessKey).withSecretKey(secretKey);
            bucketWipe.setKeepBucket(true);
            bucketWipe.withBucket(bucketName).run();

            if (bucketWipe.getErrors().size() > 0) {
                for (String error : bucketWipe.getErrors()) {
                    System.err.println(error);
                }
            }

            Assert.assertEquals(keys.length, bucketWipe.getDeletedObjects());
            Assert.assertEquals(0, bucketWipe.getErrors().size());

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
