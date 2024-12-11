package com.amazonaws.datastreamvectorization.integrationtests;

import com.amazonaws.services.kinesisanalyticsv2.AmazonKinesisAnalyticsV2;
import com.amazonaws.services.kinesisanalyticsv2.AmazonKinesisAnalyticsV2ClientBuilder;
import com.amazonaws.services.kinesisanalyticsv2.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CopyObjectRequest;

public class MSFHelper {

    AmazonKinesisAnalyticsV2 msfClient;

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

    private UpdateApplicationResult updateApplication(String appName, UpdateApplicationRequest updateAppRequest) {
        DescribeApplicationRequest describeApplicationRequest = new DescribeApplicationRequest().withApplicationName(appName);
        DescribeApplicationResult describeApplicationResult = this.msfClient.describeApplication(describeApplicationRequest);
        String conditionalToken = describeApplicationResult.getApplicationDetail().getConditionalToken();

        ApplicationConfigurationUpdate appConfigUpdate = new ApplicationConfigurationUpdate();

        UpdateApplicationRequest updateApplicationRequest = new UpdateApplicationRequest()
                .withApplicationName(appName)
                .withConditionalToken(conditionalToken)
                .withApplicationConfigurationUpdate(appConfigUpdate);
        return this.msfClient.updateApplication(updateApplicationRequest);
    }
}
