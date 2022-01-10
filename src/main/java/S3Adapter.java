import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class S3Adapter {
  private final S3Client S3;

  public S3Adapter(S3Client S3) {
    this.S3 = S3;
  }

  private void cleanBucketObjects(String bucketName) {
    try {
      ListObjectsRequest listObjects = ListObjectsRequest
        .builder()
        .bucket(bucketName)
        .build();

      ListObjectsResponse res = S3.listObjects(listObjects);
      List<S3Object> objects = res.contents();

      for (S3Object myValue : objects) {
        System.out.print("\n The name of the key is " + myValue.key());
        System.out.print("\n The owner is " + myValue.owner());
        deleteBucketObject(bucketName, myValue.key());
      }

    } catch (S3Exception e) {
      System.err.println(e.awsErrorDetails().errorMessage());
      System.exit(1);
    }
  }

  private void deleteBucketObject(String bucketName, String objectName) {

    ArrayList<ObjectIdentifier> toDelete = new ArrayList<>();
    toDelete.add(ObjectIdentifier.builder().key(objectName).build());

    try {
      DeleteObjectsRequest dor = DeleteObjectsRequest.builder()
        .bucket(bucketName)
        .delete(Delete.builder().objects(toDelete).build())
        .build();
      S3.deleteObjects(dor);
    } catch (S3Exception e) {
      System.err.println(e.awsErrorDetails().errorMessage());
      System.exit(1);
    }
    System.out.println("Done!");
  }


  public void deleteBucket(String bucket) {
    cleanBucketObjects(bucket);
    DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(bucket).build();
    S3.deleteBucket(deleteBucketRequest);
  }

  public void uploadJarsTo(String jarsBucket) {
    for (int i = 1; i <= 5; i++) {
      String currJar = "..//out//artifacts//Step" + i + "_jar//Step" + i + ".jar";
      PutObjectRequest request = PutObjectRequest.builder()
        .bucket(jarsBucket)
        .key("Step" + i + "Jar")
        .build();
      S3.putObject(request, RequestBody.fromFile(new File(currJar)));
    }
  }

  public void deleteAllBuckets(){
    ListBucketsResponse buckets = S3.listBuckets();
    System.out.println("All Your S3 buckets are:");
    int i = 1;
    for (Bucket b : buckets.buckets()) {
      System.out.println("Bucket number"+i+": " + b.name());
      deleteBucket(b.name());
    }
  }
}