package com.nishant.kafkabr;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.iterable.S3Objects;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;


public class S3Controller {

    private final String PREFIX = "sprinklr-";
    private AmazonS3 s3Client = null;

    public S3Controller() {
        System.out.println("Initialising S3 Client");
        BasicAWSCredentials awsCreds = new BasicAWSCredentials(KafkabrApplication.getCredentials().getACCESS_ID(), KafkabrApplication.getCredentials().getSECURITY_KEY());
        s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(awsCreds)).withRegion(Regions.DEFAULT_REGION)
                .build();
    }

    private void addBucket(String bucketName) {
//        try creating bucket indefinitely
        while (true) {
            try {
                if (!s3Client.doesBucketExistV2(bucketName)) {
                    System.out.println("Connecting to bucket: " + bucketName);
                    s3Client.createBucket(bucketName);
//                    Add configuration rule to auto delete objects after 30 days
                    BucketLifecycleConfiguration.Rule rule = new BucketLifecycleConfiguration.Rule()
                            .withId("Delete rule")
                            .withExpirationInDays(30)
                            .withStatus(BucketLifecycleConfiguration.ENABLED);
                    BucketLifecycleConfiguration configuration = new BucketLifecycleConfiguration()
                            .withRules(Collections.singletonList(rule));
                    s3Client.setBucketLifecycleConfiguration(bucketName, configuration);
                    System.out.println("Connection Successful with bucket: " + bucketName);
                }
                break;
            } catch (Exception e) {
                System.out.println(e.getMessage());
                System.out.println("Retrying..");
                try {
                    Thread.sleep(2000);
                } catch (Exception ignored) {
                }
            }
        }
    }

    public void uploadObject(String bucket, String keyPrefix, ArrayList<String> buffer) {
//        add prefix to bucket name to make it universally unique
        addBucket(PREFIX + bucket);
//        create object key
        String key = keyPrefix + "/" + (new Timestamp(System.currentTimeMillis()).toString());
        StringBuilder file = new StringBuilder();
        System.out.println("Creating file: " + key);
        for (int i = 0; i < buffer.size(); i++) {
            file.append(buffer.get(i));
            file.append(buffer.size() != (i + 1) ? "\n" : "");
        }
//        upload until success
        while (true) {
            try {
                System.out.println("Uploading: " + key);
                s3Client.putObject(PREFIX + bucket, key, file.toString());
                break;
            } catch (Exception e) {
                System.out.println(e.getMessage());
                System.out.println("Retrying..");
                try {
                    Thread.sleep(2000);
                } catch (Exception ignored) {
                }
            }
        }
    }

    public List<String> listKeys(String bucket, String keyPrefix, Date from, Date to) {
        addBucket(PREFIX + bucket);
        System.out.println("Filtering keys for task");
        Iterable<S3ObjectSummary> objectSummaries = S3Objects.withPrefix(s3Client, PREFIX + bucket, keyPrefix);
        Stream<S3ObjectSummary> objectStream = StreamSupport.stream(objectSummaries.spliterator(), true);
//        filter keys for objects created between the two dates specified
        List<String> keyList = objectStream.filter(obj -> (!obj.getLastModified().before(from) && !obj.getLastModified().after(nextDay(to)))).map(S3ObjectSummary::getKey).collect(Collectors.toList());
        return keyList;
    }

    public List<String> downloadObjects(String bucket, String key) {
        System.out.println("Downloading Object: " + key);
        List<String> result = null;
        S3Object fullObject = null;
//        upload until success
        while (true) {
            try {
                fullObject = s3Client.getObject(new GetObjectRequest(PREFIX + bucket, key));
                result = getTextInputStream(fullObject.getObjectContent());
                break;
            } catch (Exception e) {
                System.out.println(e.getMessage());
                System.out.println("Retrying..");
                try {
                    Thread.sleep(2000);
                } catch (Exception ignored) {
                }
            }
        }
        return result;
    }

    //    create a list of records from object downloaded
    private List<String> getTextInputStream(InputStream input) throws IOException {
        List<String> result = new ArrayList<>();
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        String line = null;
        while ((line = reader.readLine()) != null) {
            result.add(line);
        }
        return result;
    }

    private Date nextDay(Date oldDate) {
        Calendar c = Calendar.getInstance();
        c.setTime(oldDate);
        c.add(Calendar.DAY_OF_MONTH, 1);
        return c.getTime();
    }
}
