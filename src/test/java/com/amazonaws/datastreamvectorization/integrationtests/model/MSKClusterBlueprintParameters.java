package com.amazonaws.datastreamvectorization.integrationtests.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

/**
 * Model containing MSK related blueprint stack parameter values
 */
@Getter
@Setter
@Builder
@AllArgsConstructor
public class MSKClusterBlueprintParameters {
    String MSKClusterName;
    String MSKClusterArn;
    String MSKClusterSubnetIds;
    String MSKClusterSecurityGroupIds;
    String MSKTopics;
    String MSKVpcId;
}
