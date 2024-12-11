package com.amazonaws.datastreamvectorization.integrationtests;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
@AllArgsConstructor
public class MSKClusterData {
    String MSKClusterName;
    String MSKClusterArn;
    String MSKClusterSubnetIds;
    String MSKClusterSecurityGroupIds;
    String MSKTopics;
    String MSKVpcId;
}
