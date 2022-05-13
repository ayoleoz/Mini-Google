package edu.upenn.cis.cis455.AWS;

import java.util.Arrays;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

import software.amazon.awssdk.transfer.s3.FileUpload;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

public class S3 {
    public static String BUCKET = "3887da3bdd41e38401c47f5bce495707";
    public static S3Client client = getS3Client();
    public static long MB = 1024;

    public static S3Client getS3Client() {
        return S3Client.builder().credentialsProvider(StaticCredentialsProvider.create(Utils.awsCreds))
                .region(Utils.region).build();
    }

    public void uploadObjectTM(S3TransferManager transferManager, String bucketName, String objectKey,
            String objectPath) {
        FileUpload upload = transferManager.uploadFile(
                u -> u.source(Paths.get(objectPath)).putObjectRequest(p -> p.bucket(bucketName).key(objectKey)));
        upload.completionFuture().join();
    }

    public void UploadObject(String bucketName, String objectKey, String objectPath) {
        S3TransferManager transferManager = S3TransferManager.builder().s3ClientConfiguration(
                cfg -> cfg.region(Utils.region).credentialsProvider(StaticCredentialsProvider.create(Utils.awsCreds)))
                .build();
        uploadObjectTM(transferManager, bucketName, objectKey, objectPath);
        System.out.println("Object was successfully uploaded using the Transfer Manager.");
        transferManager.close();
    }

    public String putS3Object(String bucketName, String objectKey, String objectPath) {
        if (bucketName == null || objectKey == null || objectPath == null || objectPath.isBlank()) {
            return null;
        }
        try {
            PutObjectRequest putOb = PutObjectRequest.builder().bucket(bucketName).key(objectKey).build();
            byte[] obj = getObjectFile(objectPath);
            if (obj == null)
                return null;
            PutObjectResponse response = client.putObject(putOb, RequestBody.fromBytes(obj));
            return response.eTag();
        } catch (S3Exception e) {
            System.err.println(e.getMessage());
            return null;
        }
    }

    // Return a byte array
    private byte[] getObjectFile(String filePath) {
        FileInputStream fileInputStream = null;
        byte[] bytesArray = null;
        try {
            File file = new File(filePath);
            if (!file.exists()) {
                return null;
            }
            bytesArray = new byte[(int) file.length()];
            fileInputStream = new FileInputStream(file);
            fileInputStream.read(bytesArray);

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (fileInputStream != null) {
                try {
                    fileInputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return bytesArray;
    }

    public void getObjectAndWriteToLocal(String bucketName, String keyName, String path) throws IOException {
        try {
            GetObjectRequest objectRequest = GetObjectRequest.builder().key(keyName).bucket(bucketName).build();
            ResponseBytes<GetObjectResponse> objectBytes = client.getObjectAsBytes(objectRequest);
            byte[] data = objectBytes.asByteArray();
            // Write the data to a local file
            File myFile = new File(path);
            OutputStream os = new FileOutputStream(myFile);
            os.write(data);
            System.out.println("Successfully obtained bytes from an S3 object");
            os.close();
        } catch (S3Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    public String getObjectBytes(String bucketName, String keyName, long start, long end) throws IOException {
        try {
            GetObjectRequest objectRequest = GetObjectRequest.builder().key(keyName).bucket(bucketName).build();
            ResponseBytes<GetObjectResponse> objectBytes = client.getObjectAsBytes(objectRequest);
            byte[] data = objectBytes.asByteArray();
//            System.out.println(data.length);
            // Write the data to a local file
            if (start >= 0 && start < end && end <= data.length) {
                byte[] interested = Arrays.copyOfRange(data, (int) start, (int) end);
                String string = new String(interested, StandardCharsets.UTF_8);
                return string;
            }
        } catch (S3Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
        return null;
    }

}
