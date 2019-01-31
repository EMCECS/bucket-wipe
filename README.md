# Bucket Wipe 
The purpose of the bucket-wipe tool is to enumerate a bucket and delete its contents in parallel threads.  At the end the bucket is optionally deleted.  This tool is required because you cannot (for safety reasons) delete a non-empty bucket.

Current Version: 1.10

# Disclaimers about deleting data

THIS TOOL PERMANENTLY DELETES ALL DATA IN A BUCKET

# How to Build 

1. Clone bucket-wipe tool from githib 
```
$ git clone https://github.com/EMCECS/bucket-wipe
```
2. cd to the bucket-wipe directory
```
$ cd bucket-wipe
```
3. Build jar file
```
$ ./gradlew jar
```
4. The build will be located in `<path_to_bucket_wipe>bucket_wipe/build/libs/bucket-wipe-1.10.jar`

# Usage
Bucket Wipe is a Java application and can be run using the `java -jar` command followed by the location of the JAR file.  Running without any other arguments will produce command-line help.

 

``` 
C:\Users\cwikj\Downloads>java -jar bucket-wipe-1.10.jar

Error: Missing required options: e, a, s

usage:  java -jar bucket-wipe.jar [options] <bucket-name> 

 -a,--access-key <access-key>   the S3 access key

 -e,--endpoint <URI>            the endpoint to connect to, including
                                protocol, host, and port
                                
 -h,--help                      displays this help text

 -hier,--hierarchical           Enumerate the bucket hierarchically.  This
                                is recommended for ECS's
                                filesystem-enabled buckets.

    --keep-bucket               do not delete the bucket when done

 -l,--key-list <file>           instead of listing bucket, delete objects
                                matched in source file key list

    --no-smart-client           disables the ECS smart-client. use this
                                option with an external load balancer

 -p,--prefix <prefix>           deletes only objects under the specified
                                Prefix

 -s,--secret-key <secret-key>   the secret key

    --stacktrace                displays full stack trace of errors

 -t,--threads <threads>         number of threads to use

    --vhost                     enables DNS buckets and turns off load
                                balancer
```

Note that if you are using an endpoint that is a load balancer, you will want to use --no-smart-client to turn off the built-in software load balancer.
  
