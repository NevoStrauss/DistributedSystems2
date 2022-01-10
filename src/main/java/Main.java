import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

public class Main {
  public static S3Client S3;
  public static Ec2Client ec2;
  public static EmrClient emr;
  private static final String outputBucket = "dsp-ass2output";
  private static final String outputBucketBaseUrl = "s3://" + outputBucket + "/";
  private static final String jarsBucket = "dsp-jars-eran-nevo";
  private static final String jarsBucketBaseUrl = "s3://" + jarsBucket + "/";
  private static final Region REGION = Region.US_EAST_1;
  private static String TERMINATE = "TERMINATE_JOB_FLOW";

  public static void main(String[] args) {

    System.out.println("===============================================");
    System.out.println("Connect to aws & S3");
    System.out.println("===============================================\n");

    ec2 = Ec2Client.builder()
      .region(REGION)
      .build();

    S3 = S3Client.builder()
      .region(REGION)
      .build();

    System.out.println("===============================================");
    System.out.println("Clean all S3 buckets");
    System.out.println("===============================================\n");

    S3Adapter s3Adapter = new S3Adapter(S3);
    s3Adapter.deleteAllBuckets();


    System.out.println("===============================================");
    System.out.println("Create new S3 buckets");
    System.out.println("===============================================\n");

    S3.createBucket(CreateBucketRequest.builder().bucket(outputBucket).build());
    S3.createBucket(CreateBucketRequest.builder().bucket(jarsBucket).build());

    System.out.println("===============================================");
    System.out.println("Upload jars to S3");
    System.out.println("===============================================\n");

    s3Adapter.uploadJarsTo(jarsBucket);

    System.out.println("===============================================");
    System.out.println("Create EMR");
    System.out.println("===============================================\n");

    emr = EmrClient.builder()
      .region(REGION)
      .build();

    System.out.println("===============================================");
    System.out.println("EMR clusters:");
    System.out.println("===============================================\n");

    System.out.println(emr.listClusters());


    //step 1 configs
    HadoopJarStepConfig stepOneJarConfig = HadoopJarStepConfig.builder()
      .jar(jarsBucketBaseUrl + "Step1.jar")
      .args("Step1", "s3n://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data", "/output1/")
      .build();

    StepConfig stepOneConfigs = StepConfig.builder()
      .name("Step1")
      .hadoopJarStep(stepOneJarConfig)
      .actionOnFailure(TERMINATE)
      .build();


    //step2 configs
    HadoopJarStepConfig stepTwoJarConfig = HadoopJarStepConfig.builder()
      .jar(jarsBucketBaseUrl + "Step2.jar")
      .args("Step2", "s3n://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data", "/output2/")
      .build();

    StepConfig stepTwoConfigs = StepConfig.builder()
      .name("Step2")
      .hadoopJarStep(stepTwoJarConfig)
      .actionOnFailure(TERMINATE)
      .build();


    //step3 configs
    HadoopJarStepConfig stepThreeJarConfig = HadoopJarStepConfig.builder()
      .jar(jarsBucketBaseUrl + "Step3.jar")
      .args("Step3", "s3n://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data", "/output3/")
      .build();

    StepConfig stepThreeConfigs = StepConfig.builder()
      .name("Step3")
      .hadoopJarStep(stepThreeJarConfig)
      .actionOnFailure(TERMINATE)
      .build();

    //step4 configs
    HadoopJarStepConfig stepFourJarConfig = HadoopJarStepConfig.builder()
      .jar(jarsBucketBaseUrl + "Step4.jar")
      .args("Step4", "/output2/", "/output3/", "/output4/")
      .build();

    StepConfig stepFourConfigs = StepConfig.builder()
      .name("Step4")
      .hadoopJarStep(stepFourJarConfig)
      .actionOnFailure(TERMINATE)
      .build();


    //step5 configs
    HadoopJarStepConfig stepFiveJarConfig = HadoopJarStepConfig.builder()
      .jar(jarsBucketBaseUrl + "Step5.jar")
      .args("Step5", "/output3/", "/output4/", "/output5/")
      .build();


    StepConfig stepFiveConfigs = StepConfig.builder()
      .name("Step5")
      .hadoopJarStep(stepFiveJarConfig)
      .actionOnFailure(TERMINATE)
      .build();


    //step6 configs
    HadoopJarStepConfig stepSixJarConfig = HadoopJarStepConfig.builder()
      .jar(jarsBucketBaseUrl + "Step6.jar")
      .args("Step6", "/output5", outputBucketBaseUrl + "outputAssignment2")
      .build();

    StepConfig stepSixConfig = StepConfig.builder()
      .name("Step6")
      .hadoopJarStep(stepSixJarConfig)
      .actionOnFailure(TERMINATE)
      .build();

    JobFlowInstancesConfig instances = JobFlowInstancesConfig.builder()
      .instanceCount(3)
      .masterInstanceType(InstanceType.M4_2_XLARGE.toString())
      .slaveInstanceType(InstanceType.M4_2_XLARGE.toString())
      .hadoopVersion("3.3.1")
      .ec2KeyName("vockey")
      .placement(PlacementType.builder().build())
      .keepJobFlowAliveWhenNoSteps(false)
      .build();

    System.out.println("give the cluster all our steps");

    RunJobFlowRequest request = RunJobFlowRequest.builder()
      .name("dspAss2EraNevo")
      .instances(instances)
      .steps(stepOneConfigs, stepTwoConfigs, stepThreeConfigs, stepFourConfigs, stepFiveConfigs) //add step 6 when ready
      .logUri(jarsBucketBaseUrl + "logs/")
      .serviceRole("EMR_DefaultRole")
      .jobFlowRole("EMR_EC2_DefaultRole")
      .releaseLabel("emr-5.11.0")
      .build();

    RunJobFlowResponse response = emr.runJobFlow(request);
    String id = response.jobFlowId();
    System.out.println("our cluster id: " + id);
  }


}
