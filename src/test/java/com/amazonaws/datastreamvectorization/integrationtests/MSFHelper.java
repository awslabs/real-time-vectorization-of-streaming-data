package com.amazonaws.datastreamvectorization.integrationtests;

import com.amazonaws.services.kinesisanalyticsv2.AmazonKinesisAnalyticsV2;
import com.amazonaws.services.kinesisanalyticsv2.AmazonKinesisAnalyticsV2ClientBuilder;
import com.amazonaws.services.kinesisanalyticsv2.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CopyObjectRequest;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MSFHelper {

    AmazonKinesisAnalyticsV2 msfClient;
    private final static String MSF_APP_JAR_LOCAL_PATH = "target/data-stream-vectorization-1.0-SNAPSHOT.jar";
    private final static String MSF_APP_JAR_S3_FILE_KEY = "data-stream-vectorization-1.0-SNAPSHOT-integ-test.jar";

    public MSFHelper() {
        this.msfClient = AmazonKinesisAnalyticsV2ClientBuilder.defaultClient();
    }

    public StartApplicationResult startMSFApp(String appName) {
        StartApplicationRequest startApplicationRequest = new StartApplicationRequest().withApplicationName(appName);
        return this.msfClient.startApplication(startApplicationRequest);
    }

    public StopApplicationResult stopMSFApp(String appName, boolean force) {
        StopApplicationRequest stopApplicationRequest = new StopApplicationRequest()
                .withApplicationName(appName)
                .withForce(force);
        return this.msfClient.stopApplication(stopApplicationRequest);
    }

//    public UpdateApplicationResult updateMSFAppDefault(String appName) {
//        ApplicationDetail appDetail = this.describeApplication(appName).getApplicationDetail();
//        this.uploadMSFAppJarToS3(appDetail);
//
//        ApplicationConfigurationUpdate appConfigUpdate = new ApplicationConfigurationUpdate();
//        appConfigUpdate.setApplicationCodeConfigurationUpdate(this.getAppCodeConfigUpdate());
//
//    }
//
//    public UpdateApplicationResult updateMSFAppCrossVPC(String appName, String msfAppJarS3Path, String osClusterName) {
//        // TODO: update MSF app jar location
//        //  update endpoint URL (only for crossVPC)
//    }

    private ApplicationCodeConfigurationUpdate getAppCodeConfigUpdate() {
        ApplicationCodeConfigurationUpdate appCodeConfigUpdate = new ApplicationCodeConfigurationUpdate();
        S3ContentLocationUpdate s3ContentLocationUpdate = new S3ContentLocationUpdate()
                .withFileKeyUpdate(MSF_APP_JAR_S3_FILE_KEY);
        appCodeConfigUpdate.setCodeContentUpdate(new CodeContentUpdate().withS3ContentLocationUpdate(s3ContentLocationUpdate));
        return appCodeConfigUpdate;
    }

    private void uploadMSFAppJarToS3(ApplicationDetail appDetail) {
        try {
            String s3BucketArn = appDetail.getApplicationConfigurationDescription()
                    .getApplicationCodeConfigurationDescription()
                    .getCodeContentDescription()
                    .getS3ApplicationCodeLocationDescription()
                    .getBucketARN();
            String s3BucketName = getBucketNameFromArn(s3BucketArn);

            AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
            CopyObjectRequest copyObjectRequest = new CopyObjectRequest()
                    .withDestinationBucketName(s3BucketName)
                    .withDestinationKey(MSF_APP_JAR_S3_FILE_KEY)
                    .withSourceKey(MSF_APP_JAR_LOCAL_PATH);
            s3Client.copyObject(copyObjectRequest);
        } catch (Exception e) {
            throw new RuntimeException("Failed to upload test MSF app jar to the MSF app bucket: ", e);
        }
    }

    private String getBucketNameFromArn(String s3BucketArn) {
        Pattern s3BucketArnPattern = Pattern.compile("arn:aws:s3:::(?<s3BucketName>[a-z0-9.\\-])");
        Matcher matcher = s3BucketArnPattern.matcher(s3BucketArn);
        if (matcher.find()) {
            return matcher.group("s3BucketName");
        } else {
            throw new RuntimeException("Could not get bucket name from S3 bucket ARN: " + s3BucketArn);
        }
    }

    private DescribeApplicationResult describeApplication(String appName) {
        DescribeApplicationRequest describeApplicationRequest = new DescribeApplicationRequest().withApplicationName(appName);
        return this.msfClient.describeApplication(describeApplicationRequest);
    }

    private UpdateApplicationResult updateApplication(String appName, UpdateApplicationRequest updateAppRequest) {
        DescribeApplicationRequest describeApplicationRequest = new DescribeApplicationRequest().withApplicationName(appName);
        DescribeApplicationResult describeApplicationResult = this.msfClient.describeApplication(describeApplicationRequest);
        String conditionalToken = describeApplicationResult.getApplicationDetail().getConditionalToken();
        describeApplicationResult
                .getApplicationDetail()
                .getApplicationConfigurationDescription()
                .getApplicationCodeConfigurationDescription()
                .getCodeContentDescription()
                .getS3ApplicationCodeLocationDescription()
                .getBucketARN();

        ApplicationConfigurationUpdate appConfigUpdate = new ApplicationConfigurationUpdate();

        UpdateApplicationRequest updateApplicationRequest = new UpdateApplicationRequest()
                .withApplicationName(appName)
                .withConditionalToken(conditionalToken)
                .withApplicationConfigurationUpdate(appConfigUpdate);
        return this.msfClient.updateApplication(updateApplicationRequest);
    }
}
