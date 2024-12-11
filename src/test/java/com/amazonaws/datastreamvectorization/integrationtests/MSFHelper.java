package com.amazonaws.datastreamvectorization.integrationtests;

import com.amazonaws.services.kinesisanalyticsv2.AmazonKinesisAnalyticsV2;
import com.amazonaws.services.kinesisanalyticsv2.AmazonKinesisAnalyticsV2ClientBuilder;
import com.amazonaws.services.kinesisanalyticsv2.model.*;

public class MSFHelper {

    AmazonKinesisAnalyticsV2 msfClient;

    public MSFHelper() {
        this.msfClient = AmazonKinesisAnalyticsV2ClientBuilder.defaultClient();
    }

    public StartApplicationResult startApplication(String appName) {
        StartApplicationRequest startApplicationRequest = new StartApplicationRequest().withApplicationName(appName);
        return this.msfClient.startApplication(startApplicationRequest);
    }

    public StopApplicationResult stopApplication(String appName, boolean force) {
        StopApplicationRequest stopApplicationRequest = new StopApplicationRequest()
                .withApplicationName(appName)
                .withForce(force);
        return this.msfClient.stopApplication(stopApplicationRequest);
    }
}
