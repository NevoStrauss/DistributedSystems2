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
      .build();

    StepConfig stepOneConfigs = StepConfig.builder()
      .name("Step1")
      .hadoopJarStep(stepOneJarConfig)
      .actionOnFailure(TERMINATE)
      .build();


    //step2 configs
    HadoopJarStepConfig stepTwoJarConfig = HadoopJarStepConfig.builder()
      .jar(jarsBucketBaseUrl + "Step2.jar")
      .build();

    StepConfig stepTwoConfigs = StepConfig.builder()
      .name("Step2")
      .hadoopJarStep(stepTwoJarConfig)
      .actionOnFailure(TERMINATE)
      .build();


//    //step3 configs
    HadoopJarStepConfig stepThreeJarConfig = HadoopJarStepConfig.builder()
      .jar(jarsBucketBaseUrl + "Step3.jar")
      .build();

    StepConfig stepThreeConfigs = StepConfig.builder()
      .name("Step3")
      .hadoopJarStep(stepThreeJarConfig)
      .actionOnFailure(TERMINATE)
      .build();


    //step4 configs
    HadoopJarStepConfig stepFourJarConfig = HadoopJarStepConfig.builder()
      .jar(jarsBucketBaseUrl + "Step4.jar")
      .build();

    StepConfig stepFourConfigs = StepConfig.builder()
      .name("Step4")
      .hadoopJarStep(stepFourJarConfig)
      .actionOnFailure(TERMINATE)
      .build();


    //step5 configs
    HadoopJarStepConfig stepFiveJarConfig = HadoopJarStepConfig.builder()
      .jar(jarsBucketBaseUrl + "Step5.jar")
      .build();

    StepConfig stepFiveConfigs = StepConfig.builder()
      .name("Step5")
      .hadoopJarStep(stepFiveJarConfig)
      .actionOnFailure(TERMINATE)
      .build();

    //step6 configs
    HadoopJarStepConfig stepSixJarConfigs = HadoopJarStepConfig.builder()
      .jar(jarsBucketBaseUrl + "Step6.jar")
      .build();

    StepConfig stepSixConfigs = StepConfig.builder()
      .name("Step6")
      .hadoopJarStep(stepSixJarConfigs)
      .actionOnFailure(TERMINATE)
      .build();


    System.out.println("===============================================");
    System.out.println("Initializing Job");
    System.out.println("===============================================\n");

    // config job
    JobFlowInstancesConfig instances = JobFlowInstancesConfig.builder()
      .instanceCount(3)
      .masterInstanceType(InstanceType.M4_LARGE.toString())
      .slaveInstanceType(InstanceType.M4_LARGE.toString())
      .hadoopVersion("2.7.3")
      .ec2KeyName("vockey")
      .placement(PlacementType.builder().build())
      .keepJobFlowAliveWhenNoSteps(false)
      .build();

    RunJobFlowRequest request = RunJobFlowRequest.builder()
      .name("dspAss2EraNevo")
      .instances(instances)
      .steps(stepOneConfigs, stepTwoConfigs, stepThreeConfigs, stepFourConfigs, stepFiveConfigs, stepSixConfigs)
      .logUri(jarsBucketBaseUrl + "logs/")
      .serviceRole("EMR_DefaultRole")
      .jobFlowRole("EMR_EC2_DefaultRole")
      .releaseLabel("emr-5.11.0")
      .build();

    RunJobFlowResponse response = emr.runJobFlow(request);
    String id = response.jobFlowId();
    System.out.println("cluster id: " + id);
  }

}
