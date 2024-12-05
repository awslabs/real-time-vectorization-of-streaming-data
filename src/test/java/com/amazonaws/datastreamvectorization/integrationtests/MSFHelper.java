package com.amazonaws.datastreamvectorization.integrationtests;

import com.amazonaws.services.kinesisanalyticsv2.AmazonKinesisAnalyticsV2;
import com.amazonaws.services.kinesisanalyticsv2.AmazonKinesisAnalyticsV2ClientBuilder;
import com.amazonaws.services.kinesisanalyticsv2.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.PutObjectRequest;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.amazonaws.datastreamvectorization.constants.CommonConstants.FlinkApplicationProperties.*;
import static com.amazonaws.datastreamvectorization.wrappers.FlinkSetupProvider.APPLICATION_PROPERTIES_GROUP_NAME;
import static java.util.Map.entry;

/**
 * Helper class to interact with Amazon MSF (Managed Service for Apache Flink)
 */
public class MSFHelper {

    AmazonKinesisAnalyticsV2 msfClient;
    private final static String MSF_APP_JAR_LOCAL_PATH = "target/data-stream-vectorization-1.0-SNAPSHOT.jar";
    private final static String MSF_APP_JAR_S3_FILE_KEY = "data-stream-vectorization-1.0-SNAPSHOT-integ-test.jar";
    private final static int MAX_POLL_STACK_STATUS_RETRIES = 15;
    private final static Long POLL_STACK_STATUS_DELAY = 60000L; // 1 minute
    private final static List<String> TERMINAL_MSF_APP_STATUSES = List.of("READY", "RUNNING", "ROLLED_BACK");

    public MSFHelper() {
        this.msfClient = AmazonKinesisAnalyticsV2ClientBuilder.defaultClient();
    }

    /**
     * Starts the MSF app with given name and waits for the app state to reach a terminal status before returning.
     *
     * @param appName Name of MSF app to start
     */
    public void startMSFApp(String appName) {
        StartApplicationRequest startApplicationRequest = new StartApplicationRequest().withApplicationName(appName);
        this.msfClient.startApplication(startApplicationRequest);
        String appStatus = pollMSFAppStatus(appName);
        if (appStatus.isEmpty()) {
            throw new RuntimeException("Failed to start application " + appName);
        }
        if (appStatus.equals("STARTING")) {
            throw new RuntimeException("MSF app " + appName + " is still in STARTING state and did not complete " +
                    "starting within test timeout");
        }
        if (!appStatus.equals("RUNNING")) {
            throw new RuntimeException("MSF app " + appName + " did not end in RUNNING state after starting: " +
                    appStatus);
        }
    }

    /**
     * Stops the MSF app with given name and waits for the app state to reach a terminal status before returning.
     *
     * @param appName Name of MSF app to stop
     */
    public void stopMSFApp(String appName, boolean force) {
        StopApplicationRequest stopApplicationRequest = new StopApplicationRequest()
                .withApplicationName(appName)
                .withForce(force);
        this.msfClient.stopApplication(stopApplicationRequest);
        String appStatus = pollMSFAppStatus(appName);
        if (appStatus.isEmpty()) {
            throw new RuntimeException("Failed to stop application " + appName);
        }
        if (appStatus.equals("STOPPING") || appStatus.equals("FORCE_STOPPING")) {
            throw new RuntimeException("MSF app " + appName + " is still in " + appStatus +
                    "state and did not complete stopping within test timeout");
        }
        if (!appStatus.equals("READY")) {
            throw new RuntimeException("MSF app " + appName + " did not end in READY state after stopping: " +
                    appStatus);
        }
    }

    /**
     * Updates the MSF app for a typical integration test. Uploads the new MSF app JAR to test to the MSF app's S3
     * bucket and updates the MSF app config with the new JAR location.
     *
     * @param appName Name of MSF app to update
     */
    public void updateMSFAppDefault(String appName) {
        // upload test MSF app jar to MSF app S3 bucket
        ApplicationDetail appDetail = this.describeApplication(appName).getApplicationDetail();
        this.uploadMSFAppJarToS3(appDetail);

        // set the configuration update data (MSF app jar location config updates)
        ApplicationConfigurationUpdate appConfigUpdate = new ApplicationConfigurationUpdate();
        appConfigUpdate.setApplicationCodeConfigurationUpdate(this.getAppCodeConfigUpdate());

        // update the application config
        updateApplication(appName, appConfigUpdate);
    }

    /**
     * Updates the MSF app for a cross-VPC integration test, where the MSK cluster and OpenSearch cluster are in
     * different VPCs. Does the same actions as updateMSFAppDefault() but this method also updates the MSF app runtime
     * configuration parameters to support a cross-VPC setup.
     *
     * @param appName Name of MSF app to update
     */
    public void updateMSFAppCrossVPC(String appName) {
        // upload test MSF app jar to MSF app S3 bucket
        ApplicationDetail appDetail = this.describeApplication(appName).getApplicationDetail();
        this.uploadMSFAppJarToS3(appDetail);

        // set the configuration update data (MSF app jar location and cross-VPC config updates)
        ApplicationConfigurationUpdate appConfigUpdate = new ApplicationConfigurationUpdate();
        appConfigUpdate.setApplicationCodeConfigurationUpdate(this.getAppCodeConfigUpdate());
        appConfigUpdate.setEnvironmentPropertyUpdates(this.getCrossVpcFlinkAppConfigUpdate(appDetail));

        // update the application config
        updateApplication(appName, appConfigUpdate);
    }

    /**
     * Constructs the ApplicationCodeConfigurationUpdate object needed to update the MSF app JAR location.
     *
     * @return ApplicationCodeConfigurationUpdate containing the new MSF app JAR S3 location.
     */
    private ApplicationCodeConfigurationUpdate getAppCodeConfigUpdate() {
        ApplicationCodeConfigurationUpdate appCodeConfigUpdate = new ApplicationCodeConfigurationUpdate();
        S3ContentLocationUpdate s3ContentLocationUpdate = new S3ContentLocationUpdate()
                .withFileKeyUpdate(MSF_APP_JAR_S3_FILE_KEY);
        appCodeConfigUpdate.setCodeContentUpdate(new CodeContentUpdate().withS3ContentLocationUpdate(s3ContentLocationUpdate));
        return appCodeConfigUpdate;
    }

    /**
     * Constructs the EnvironmentPropertyUpdates object needed to update the MSF app runtime properties to support
     * cross-VPC setup between MSK and OpenSearch.
     *
     * @param appDetail ApplicationDetail from describing the MSF application
     * @return EnvironmentPropertyUpdates containing the new runtime property changes
     */
    private EnvironmentPropertyUpdates getCrossVpcFlinkAppConfigUpdate(ApplicationDetail appDetail) {
        EnvironmentPropertyUpdates envPropertyUpdates = new EnvironmentPropertyUpdates();

        List<PropertyGroup> propertyGroups = appDetail
                .getApplicationConfigurationDescription()
                .getEnvironmentPropertyDescriptions()
                .getPropertyGroupDescriptions();

        String msfAppVpcId = appDetail.getApplicationConfigurationDescription()
                .getVpcConfigurationDescriptions()
                .get(0).getVpcId();

        String osClusterName = "";

        for (PropertyGroup group : propertyGroups) {
            if (group.getPropertyGroupId().equals(APPLICATION_PROPERTIES_GROUP_NAME)) {
                Map<String, String> propertyMap = group.getPropertyMap();
                osClusterName = propertyMap.get(PROPERTY_OS_NAME);
                break;
            }
        }

        OpenSearchHelper openSearchHelper = new OpenSearchHelper();
        String crossVpcEndpointURL = openSearchHelper.getCrossVpcEndpoint(osClusterName, msfAppVpcId);

        Map<String, String> runtimePropertiesMap = Map.ofEntries(entry(PROPERTY_OS_ENDPOINT, crossVpcEndpointURL));

        PropertyGroup propertyGroup = new PropertyGroup();
        propertyGroup.setPropertyGroupId(APPLICATION_PROPERTIES_GROUP_NAME);
        propertyGroup.setPropertyMap(runtimePropertiesMap);
        List<PropertyGroup> propertyGroupsUpdate = List.of(new PropertyGroup());
        envPropertyUpdates.setPropertyGroups(propertyGroupsUpdate);

        return envPropertyUpdates;
    }

    /**
     * Upload the test MSF app jar to the MSF app's configured S3 bucket.
     *
     * @param appDetail ApplicationDetail from describing the MSF application
     */
    private void uploadMSFAppJarToS3(ApplicationDetail appDetail) {
        try {
            String s3BucketArn = appDetail.getApplicationConfigurationDescription()
                    .getApplicationCodeConfigurationDescription()
                    .getCodeContentDescription()
                    .getS3ApplicationCodeLocationDescription()
                    .getBucketARN();
            String s3BucketName = getBucketNameFromArn(s3BucketArn);

            AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
            PutObjectRequest putObjectRequest = new PutObjectRequest(s3BucketName, MSF_APP_JAR_S3_FILE_KEY,
                    new File(MSF_APP_JAR_LOCAL_PATH));
            s3Client.putObject(putObjectRequest);
        } catch (Exception e) {
            throw new RuntimeException("Failed to upload test MSF app jar to the MSF app bucket: ", e);
        }
    }

    /**
     * Get S3 bucket name from an S3 bucket ARN
     *
     * @param s3BucketArn ARN of an S3 bucket
     * @return Bucket name
     */
    private String getBucketNameFromArn(String s3BucketArn) {
        Pattern s3BucketArnPattern = Pattern.compile("arn:aws:s3:::(?<s3BucketName>[a-z0-9.\\-]+)");
        Matcher matcher = s3BucketArnPattern.matcher(s3BucketArn);
        if (matcher.find()) {
            return matcher.group("s3BucketName");
        } else {
            throw new RuntimeException("Could not get bucket name from S3 bucket ARN: " + s3BucketArn);
        }
    }

    /**
     * Describe an MSF app
     *
     * @param appName Name of the MSF app
     * @return DescribeApplicationResult with the info about the MSF app
     */
    private DescribeApplicationResult describeApplication(String appName) {
        DescribeApplicationRequest describeApplicationRequest = new DescribeApplicationRequest().withApplicationName(appName);
        return this.msfClient.describeApplication(describeApplicationRequest);
    }

    /**
     * Update the MSF application with the given ApplicationConfigurationUpdate config.
     *
     * @param appName Name of the MSF app to update
     * @param appConfigUpdate The configuration update config containing changes to apply to the MSF app config
     */
    private void updateApplication(String appName, ApplicationConfigurationUpdate appConfigUpdate) {
        // get the conditional token for the update request
        DescribeApplicationRequest describeApplicationRequest = new DescribeApplicationRequest().withApplicationName(appName);
        DescribeApplicationResult describeApplicationResult = this.msfClient.describeApplication(describeApplicationRequest);
        String conditionalToken = describeApplicationResult.getApplicationDetail().getConditionalToken();

        // update the application
        UpdateApplicationRequest updateApplicationRequest = new UpdateApplicationRequest()
                .withApplicationName(appName)
                .withConditionalToken(conditionalToken)
                .withApplicationConfigurationUpdate(appConfigUpdate);
        this.msfClient.updateApplication(updateApplicationRequest);

        // wait for the application to complete updating
        String appStatus = pollMSFAppStatus(appName);
        if (appStatus.isEmpty()) {
            throw new RuntimeException("Failed to update application " + appName);
        }
        if (appStatus.equals("UPDATING")) {
            throw new RuntimeException("MSF app " + appName + " is still in UPDATING state and did not complete " +
                    "updating within test timeout");
        }
        if (!appStatus.equals("READY")) {
            throw new RuntimeException("MSF app " + appName + " did not end in READY state after updating: " +
                    appStatus);
        }
    }

    /**
     * Poll the MSF app status by periodically checking its status.
     *
     * @param appName Name of the MSF app to check
     * @return The MSF app status
     */
    private String pollMSFAppStatus(String appName) {
        int retryCount = 0;
        String appStatus = "";
        try {
            while (retryCount++ <= MAX_POLL_STACK_STATUS_RETRIES) {
                ApplicationDetail appDetail = this.describeApplication(appName).getApplicationDetail();
                appStatus = appDetail.getApplicationStatus();
                if (TERMINAL_MSF_APP_STATUSES.contains(appStatus)) {
                    return appStatus;
                }
                Thread.sleep(POLL_STACK_STATUS_DELAY);
            }
            return appStatus;
        } catch (Exception e) {
            throw new RuntimeException("Error occurred when polling MSF app status for " + appName, e);
        }
    }
}
